""" module containing lambda handler to get william hill futures """
import json
import logging
import datetime
import time
import os
import copy
import urllib.request
from urllib.error import HTTPError
import pymongo
from pymongo.errors import PyMongoError
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

name_playerId_map = {}

def process_player_futures(event, context):
    """ lambda handler to process william hill futures """
    s3 = boto3.client("s3")
    status_code = 500
    message = ""
    league = ""
    try:
        book_id = os.environ["BOOKID"]
        league = os.environ["LEAGUE"]
        league_id = int(os.environ["CBS_LEAGUE_ID"])
        logger.info(event)
        message = json.loads(event['Records'][0]['Sns']['Message'])
        bucket_name = message['Records'][0]['s3']['bucket']['name']
        logger.info(bucket_name)
        file_key = message['Records'][0]['s3']['object']['key']
        logger.info(file_key)
    except KeyError as key_error:
        message = "KeyError"
        logger.error("%s", key_error)
    except TypeError as type_error:
        message = "TypeError"
        logger.error("%s", type_error)
    else:
        if league == file_key:
            status_code, message = process_s3(s3, bucket_name, file_key, book_id, league, league_id)
        else:
            status_code = 200
            message = "file doesn't match league"
    response = {
        "statusCode": status_code,
        "body": league + " player futures completed",
        "message": message,
    }

    return response

def process_s3(s3, bucket_name, file_key, book_id, league, league_id):
    """ get s3 file """
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = obj['Body'].read()
        futures = json.loads(data)
    except ClientError as ce:
        status_code = 501
        message = "s3 get object error"
        logger.error(message)
        logger.error(ce.response["Error"]["Code"])
    except ValueError:
        status_code = 500
        message = "failed to decode json"
        logger.error(message)
    else:
        status_code, message, futures = filter_and_fix_futures(futures, book_id, league, league_id)
        if status_code == 200:
            status_code, message = persist_player_futures_by_player(futures, book_id, league, league_id)
        if status_code == 200:
            status_code, message = persist_league_player_futures(futures)


    return status_code, message

def filter_and_fix_futures(futures, book_id, league, league_id):
    """ filter and fix futures """
    status_code = 200
    try:
        filtered_futures = []
        with open(os.path.join(__location__, "market-names-players-"+league+".json")) as json_file:
            markets = json.load(json_file)
        logger.info(markets["market_names"])
        status_code, message, william_hill_vendor_player_map = get_wh_vendor_player_map(league)
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("Failed to load json file")
        logger.error("%s", key_error)
    else:
        for future in futures:

            if any(tmn in future["name"] for tmn in markets["market_names"]):
                future["_id"] = future["id"]
                del future["id"]
                future["league_id"] = league_id
                future["book_id"] = book_id
                filtered_futures.append(future)
        message = "done"
    return status_code, message, filtered_futures

def persist_player_futures_by_player(futures, book_id, league, league_id):
    """ persist league team futures to documentDb """
    try:
        doc_db_connstring = os.environ["DOC_DB_CONNECTION_STRING"]
        mongo_client = get_client(doc_db_connstring)
        player_collection = os.environ["PLAYER_MONGO_COLLECTION"]
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("Failed to connect to DocDB")
        logger.error("%s", key_error)
    else:
        try:
            player_futures = fix_player_futures(futures, league)
            for key in player_futures:
                t_f = {}
                t_f["_id"] = key
                t_f["futures"] = player_futures[key]
                t_f_json = json.loads(json.dumps(t_f, default=str))
                t_s = time.time()
                t_f_json["update_date"] = datetime.datetime.fromtimestamp(t_s, None)
                result = replace_one(mongo_client, player_collection, t_f_json["_id"], t_f_json)
                if not result.raw_result['ok']:
                    logger.error(result.raw_result)
                    logger.error("Error upserting document %s", t_f_json["name"])
                else:
                    expire_resource(t_f_json["_id"])
            status_code = 200
            message = "player futures persisted"
        except PyMongoError as pm_error:
            status_code = 500
            message = "Pymongo Error"
            logger.error(message)
            logger.error("%s", pm_error)
    return status_code, message

def persist_league_player_futures(futures):
    """ persist league player futures to documentDb """
    try:
        doc_db_connstring = os.environ["DOC_DB_CONNECTION_STRING"]
        mongo_client = get_client(doc_db_connstring)
        league_collection = os.environ["LEAGUE_PLAYER_MONGO_COLLECTION"]
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("Failed to connect to DocDB for league player collection")
        logger.error("%s", key_error)
    else:
        try:
            for future in futures:
                if "markets" in future and len(future["markets"]) > 0:
                    t_s = time.time()
                    future["update_date"] = datetime.datetime.fromtimestamp(t_s, None)
                    result = replace_one(mongo_client, league_collection, future["_id"], future)
                    if not result.raw_result['ok']:
                        logger.error(result.raw_result)
                        logger.error("Error upserting document %s", future["name"])
                else:
                    if "name" in future:
                        logger.error("No market for %s", future["name"])
                    else:
                        logger.error("Missing market for future")
            status_code = 200
            message = "player futures persisted"
        except PyMongoError as pm_error:
            status_code = 500
            message = "Pymongo Error"
            logger.error("Failed to write to DocDB for league player collection")
            logger.error("%s", pm_error)

    return status_code, message


def fix_player_futures(futures, league):

    status_code, message, william_hill_vendor_player_map = get_wh_vendor_player_map(league)
    if status_code == 500:
        logger.warning("Failed to get vendor mappings")
    else:
        player_futures = {}
        for future in futures:
            if len(future["markets"]) > 0:
                if str(future["name"]).endswith("- Your Odds"):
                    future_copy = copy.deepcopy(future)
                    name = future["name"].split(" -")[0]
                    player_id = william_hill_vendor_player_map.get(name,0)
                    if player_id == 0:
                        logger.warning("No vendor mapping found for: "+name)
                    else:
                        for selection in future["markets"][0]["selections"]:
                            selection['playerId'] = player_id
                        for market in future_copy["markets"]:
                            for selection in market["selections"]:
                                selection['playerId'] = player_id
                            if player_id in player_futures:
                                player_futures.get(player_id).append(future_copy)
                            else:
                                player_futures[player_id] = []
                                player_futures.get(player_id).append(future_copy)
                else:
                    for selection in future["markets"][0]["selections"]:
                        future_copy = copy.deepcopy(future)
                        future_copy["markets"][0]["selections"] = []
                        if "name" in selection:
                            name = selection["name"]
                            player_id = william_hill_vendor_player_map.get(name,0)
                            if player_id == 0:
                                logger.warning("No vendor mapping found for: "+name)
                            else:
                                selection['playerId'] = player_id
                                future_copy["markets"][0]["selections"].append(selection)
                                if player_id in player_futures:
                                    player_futures.get(player_id).append(future_copy)
                                else:
                                    player_futures[player_id] = []
                                    player_futures.get(player_id).append(future_copy)
            else:
                if "name" in future:
                    logger.error("No market for %s", future["name"])
                else:
                    logger.error("Missing market for future")
    return player_futures

def replace_one(mongo_client, player_collection, _id, data):
    """ pymongo replace_one """
    return mongo_client.atlas[player_collection].replace_one({"_id" : _id}, data, upsert=True)


def expire_resource(key):
    """ expire napi resource for future """
    try:
        stage = os.environ["STAGE"]
        napi_api = "https://sdf"
        if stage != "prod":
            napi_api += "-qa"
        napi_api += "-api.cbssports.cloud/napi/resource/player/futures/"
        napi_api += str(key)
        napi_api += "?access_token=d97eb9bd975a433768af1e4d878ef93be40cbeb1&force_rebuild=1"
        req = urllib.request.Request(napi_api)
        urllib.request.urlopen(req)
    except HTTPError as http_error:
        message = "failed to retrieve " + napi_api
        logger.error(message)
        logger.error("%s %s", http_error.code, http_error.reason)

def get_wh_vendor_player_map(league):
    """ get wh vendor player map """
    try:
        stage = os.environ["STAGE"]
        primpy_api = "https://sdf"
        if stage != "prod":
            primpy_api += "-qa"
        primpy_api += "-api.cbssports.cloud/primpy/livescoring/williamhill/players/mappings/league/"
        primpy_api += league
        primpy_api += "?access_token=15fe23dca68e90c740f708f89ec8c7aa67272051&fields=mappedPlayers"
        logger.info(primpy_api)
        req = urllib.request.Request(primpy_api)
        data = json.load(urllib.request.urlopen(req))
    except HTTPError as http_error:
        message = "failed to retrieve " + primpy_api
        logger.error(message)
        logger.error("%s %s", http_error.code, http_error.reason)
        status_code = http_error.code
    except ValueError:
        status_code = 500
        message = "failed to decode json from Primpy vendor mapping"
        logger.error(message)
    else:
        status_code = 200
        message = "found vendor player mappings"
        vendor_player_map = {}
        for mapped_player in data["mappedPlayers"]:
            vendor_player_map[mapped_player["vendorPlayerId"]] = mapped_player["cbsPlayerId"]
    return status_code, message, vendor_player_map

def get_client(conn):
    """ get mongo client """
    return pymongo.MongoClient(conn)
