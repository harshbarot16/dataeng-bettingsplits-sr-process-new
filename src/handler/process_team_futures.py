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

def process_team_futures(event, context):
    """ lambda handler to process william hill futures """
    s3 = boto3.client("s3")
    status_code = 500
    message = ""
    league = ""
    try:
        book_id = os.environ["BOOKID"]
        league = os.environ["LEAGUE"]
        league_id = int(os.environ["CBS_LEAGUE_ID"])
        message = json.loads(event['Records'][0]['Sns']['Message'])
        bucket_name = message['Records'][0]['s3']['bucket']['name']
        file_key = message['Records'][0]['s3']['object']['key']
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
        "body": league + " team futures completed",
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
        logger.error(ce.response["Error"]["Code"])
    except ValueError:
        status_code = 500
        message = "failed to decode json"
        logger.error(message)
    else:
        status_code, message, futures = fix_futures(futures, book_id, league, league_id)
        if status_code == 200:
            status_code, message = persist_league_team_futures(futures, book_id, league, league_id)
        if status_code == 200:
            status_code, message = persist_team_futures_by_team(futures, book_id, league, league_id)

    return status_code, message

def persist_league_team_futures(futures, book_id, league, league_id):
    """ persist league team futures to documentDb """
    try:
        doc_db_connstring = os.environ["DOC_DB_CONNECTION_STRING"]
        mongo_client = get_client(doc_db_connstring)
        league_collection = os.environ["LEAGUE_MONGO_COLLECTION"]
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
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
            message = "team futures persisted"
        except PyMongoError as pm_error:
            status_code = 500
            message = "Pymongo Error"
            logger.error("%s", pm_error)

    return status_code, message

def fix_futures(futures, book_id, league, league_id):
    """ filter and fix futures """
    try:
        filtered_futures = []
        with open(os.path.join(__location__, "market-names-"+league+".json")) as json_file:
            markets = json.load(json_file)
        logger.info(markets["market_names"])
        status_code, message, stats_vendor_team_map = get_stats_vendor_team_map(league)
        status_code, message, william_hill_vendor_team_map = get_wh_vendor_team_map(league)
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("%s", key_error)
    else:
        for future in futures:
            if any(tmn in future["name"] for tmn in markets["market_names"]):
                future["_id"] = future["id"]
                del future["id"]
                future["league_id"] = league_id
                future["book_id"] = book_id
                future = add_cbs_team_id(future, stats_vendor_team_map, william_hill_vendor_team_map)
                filtered_futures.append(future)
        message = "done"
    return status_code, message, filtered_futures

def persist_team_futures_by_team(futures, book_id, league, league_id):
    """ persist league team futures to documentDb """
    try:
        doc_db_connstring = os.environ["DOC_DB_CONNECTION_STRING"]
        mongo_client = get_client(doc_db_connstring)
        team_collection = os.environ["TEAM_MONGO_COLLECTION"]
        logger.info(doc_db_connstring)
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("%s", key_error)
    else:
        try:
            team_futures = fix_team_futures(futures)
            for key in team_futures:
                t_f = {}
                t_f["_id"] = key
                t_f["futures"] = team_futures[key]
                t_f_json = json.loads(json.dumps(t_f, default=str))
                t_s = time.time()
                t_f_json["update_date"] = datetime.datetime.fromtimestamp(t_s, None)
                result = replace_one(mongo_client, team_collection, t_f_json["_id"], t_f_json)
                if not result.raw_result['ok']:
                    logger.error(result.raw_result)
                    logger.error("Error upserting document %s", t_f_json["name"])
                else:
                    expire_resource(t_f_json["_id"])
            status_code = 200
            message = "team futures persisted"
        except PyMongoError as pm_error:
            status_code = 500
            message = "Pymongo Error"
            logger.error("%s", pm_error)
    return status_code, message

def fix_team_futures(futures):
    """ group all team futures by teams for insertion """
    team_futures = {}
    for future in futures:
        if "_id" in future:
            del future["_id"]
        if "team_id" in future:
            team_id = future["team_id"]
            if team_id in team_futures:
                team_futures.get(team_id).append(future)
            else:
                team_futures[team_id] = []
                team_futures.get(team_id).append(future)
        else:
            if len(future["markets"]) > 0:
                for selection in future["markets"][0]["selections"]:
                    future_copy = copy.deepcopy(future)
                    future_copy["markets"][0]["selections"] = []
                    if "team_id" in selection:
                        team_id = selection["team_id"]
                        future_copy["markets"][0]["selections"].append(selection)
                        if team_id in team_futures:
                            team_futures.get(team_id).append(future_copy)
                        else:
                            team_futures[team_id] = []
                            team_futures.get(team_id).append(future_copy)
            else:
                if "name" in future:
                    logger.error("No market for %s", future["name"])
                else:
                    logger.error("Missing market for future")
    return team_futures

def replace_one(mongo_client, team_collection, _id, data):
    """ pymongo replace_one """
    return mongo_client.atlas[team_collection].replace_one({"_id" : _id}, data, upsert=True)



def get_wh_vendor_team_map(league):
    """ get wh vendor team map """
    try:
        primpy_api = "http://sdf-api.cbssports.cloud/primpy/fantasy/williamhill/teams/mappings/league/"
        primpy_api += league
        primpy_api += "?access_token=d3f02e8cba092ac4accbb02f63281f86880de43f"
        req = urllib.request.Request(primpy_api)
        data = json.load(urllib.request.urlopen(req))
    except HTTPError as http_error:
        message = "failed to retrieve " + primpy_api
        logger.error(message)
        logger.error("%s %s", http_error.code, http_error.reason)
        status_code = http_error.code
    except ValueError:
        status_code = 500
        message = "failed to decode json"
        logger.error(message)
    else:
        status_code = 200
        message = "found vendor team mappings"
        vendor_team_map = {}
        for mapped_team in data["mappedTeams"]:
            vendor_team_map[mapped_team["vendorTeamId"]] = mapped_team["cbsTeamId"]
    return status_code, message, vendor_team_map

def add_cbs_team_id(future, stats_vendor_team_map, william_hill_vendor_team_map):
    """ get vendor team mapping """
    for market in future["markets"]:
        selection_team = False
        for selection in market["selections"]:
            if "statsParticipantId" in selection and selection["statsParticipantId"] != "":
                if int(selection["statsParticipantId"]) in stats_vendor_team_map:
                    selection["team_id"] = stats_vendor_team_map[int(selection["statsParticipantId"])]
                    selection_team = True
            if not selection_team:
                future = add_team_id_from_name(future, william_hill_vendor_team_map)
    return future

def add_team_id_from_name(future, william_hill_vendor_team_map):
    """ get team id from name """
    name_list = (future["name"]).split(" ")
    for index, name in enumerate(name_list):
        found = False
        for team_name in william_hill_vendor_team_map.keys():
            if index < len(name_list) - 3 and name in team_name and name_list[index+1] in team_name and name_list[index+2] in team_name:
                future["team_id"] = william_hill_vendor_team_map.get(team_name)
                found = True
            elif index < len(name_list) - 2 and name in team_name and name_list[index+1] in team_name:
                future["team_id"] = william_hill_vendor_team_map.get(team_name)
                found = True
            if found:
                break
        if found:
            break
    return future

def expire_resource(key):
    """ expire napi resource for future """
    try:
        stage = os.environ["STAGE"]
        napi_api = "https://sdf"
        if stage == "qa":
            napi_api += "-qa"
        napi_api += "-api.cbssports.cloud/napi/resource/team/futures/"
        napi_api += str(key)
        napi_api += "?access_token=d97eb9bd975a433768af1e4d878ef93be40cbeb1&force_rebuild=1"
        req = urllib.request.Request(napi_api)
        urllib.request.urlopen(req)
    except HTTPError as http_error:
        message = "failed to retrieve " + napi_api
        logger.error(message)
        logger.error("%s %s", http_error.code, http_error.reason)

def get_client(conn):
    """ get mongo client """
    return pymongo.MongoClient(conn)
