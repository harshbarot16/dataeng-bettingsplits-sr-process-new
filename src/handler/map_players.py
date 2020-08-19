""" module containing lambda handler to get william hill futures and create vendor player mappings"""
import json
import logging
import os
import urllib.request
from urllib.error import HTTPError
import boto3
from botocore.exceptions import ClientError
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import src.util.mysqlClient as mysqlClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

def map_players(event, context):
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
        logger.error("s3 connect failed")
        logger.error("%s", key_error)
    except TypeError as type_error:
        message = "TypeError"
        logger.error("s3 connect failed")
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
        status_code, message, roster_name_playerid_map = get_roster_player_ids_map(league)
        if status_code == 200:
            status_code, message, vendor_name_playerid_map = get_wh_vendor_player_map(league)
        if status_code == 200:
            status_code, message, filtered_futures = filter_futures(futures, book_id, league, league_id)
        if status_code == 200:
            check_and_add_vendor_mappings(filtered_futures, roster_name_playerid_map, vendor_name_playerid_map)

    return status_code, message

def filter_futures(futures, book_id, league, league_id):
    status_code = 200
    try:
        filtered_futures = []
        with open(os.path.join(__location__, "market-names-players-"+league+".json")) as json_file:
            markets = json.load(json_file)
        logger.info(markets["market_names"])
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("Failed to read json markets file")
        logger.error("%s", key_error)
    else:
        for future in futures:
            if any(tmn in future["name"] for tmn in markets["market_names"]):
                filtered_futures.append(future)
        message = "done"
    return status_code, message, filtered_futures


def check_and_add_vendor_mappings(futures, roster_name_playerid_map, vendor_name_playerid_map):
    for future in futures:
        if len(future["markets"]) > 0:
            for selection in future["markets"][0]["selections"]:
                if "name" in selection:
                    name = selection["name"]
                    if name in vendor_name_playerid_map:
                        logger.info("Name existing in vendor mapping: "+name+". Nothing to do")
                    else:
                        logger.info("No vendor mapping, looking in roster")
                        player_id = get_player_id_from_name(name, roster_name_playerid_map)
                        if player_id == 0:
                            logger.warning("No good match found for: "+name)
                        else:
                            logger.info("Match found, need to create new vendor mapping for player_id: "+str(player_id))
                            mysqlClient.addVendorMapping(56, player_id, name, 59)
        else:
            if "name" in future:
                logger.error("No market for %s", future["name"])
            else:
                logger.error("Missing market for future")
    return ""

def get_roster_player_ids_map(league):
    """  """
    name_player_id_map = {}

    try:
        recordOffset = 0
        recordLimit = 1000
        season_year, season_type = mysqlClient.getCurrentSeason(league)
        status, message, response = get_primpy_roster(league, season_year, season_type, recordOffset, recordLimit)
        rosterResponse = json.loads(response.read())
        meta = rosterResponse['meta']
        totalRecords = meta['totalRecords']
        loopCount = (totalRecords//1000) + 1

        for rec in rosterResponse['data']:
            name = rec['firstName'] + ' ' + rec['lastName']
            player_id = rec['playerId']
            name_player_id_map[name] = player_id
        for i in range(1,loopCount):
            recordOffset = recordOffset + 1000
            status, message, response = get_primpy_roster(league, season_year, season_type, recordOffset, recordLimit)
            rosterResponse = json.loads(response.read())
            for rec in rosterResponse['data']:
                name = rec['firstName'] + ' ' + rec['lastName']
                player_id = rec['playerId']
                name_player_id_map[name] = player_id

    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("Failure while getting player vendor map")
        logger.error("%s", key_error)
    else:
        status_code = 200
        message = "Found league roster"

    return status_code, message, name_player_id_map

def get_primpy_roster(league, season_year, season_type, recordOffset, recordLimit):
    try:
        stage = os.environ["STAGE"]
        primpy_api = "https://sdf"
        if stage != "prod":
            primpy_api += "-qa"
        primpy_api += "-api.cbssports.cloud/primpy/livescoring/roster/league/"
        primpy_api += str(league)
        primpy_api += "?access_token=15fe23dca68e90c740f708f89ec8c7aa67272051&seasonYear="
        primpy_api += str(season_year)
        primpy_api += "&seasonType="
        primpy_api +=  str(season_type)
        primpy_api +=  "&fields=meta,data(playerId,firstName,lastName)&assocType=C,H"
        primpy_api += "&recordOffset="
        primpy_api += str(recordOffset)
        primpy_api += "&recordLimit="
        primpy_api += str(recordLimit)
        logger.info(primpy_api)
        req = urllib.request.Request(primpy_api)
        response = urllib.request.urlopen(req)
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("Primpy league roster query failed")
        logger.error("%s", key_error)
    else:
        status_code = 200
        message = "Found league roster"

    return status_code, message, response

def get_player_id_from_name(name, roster_name_playerid_map):
    player_id = 0
    match_name = ''
    current_similarity_score = 0
    for rec_name in roster_name_playerid_map:
        score = fuzz.ratio(rec_name, name)
        if score > current_similarity_score:
            current_similarity_score = score
            player_id = roster_name_playerid_map[rec_name]
            match_name = rec_name
    if current_similarity_score < 85:
        logger.warning("No close match found for: "+name+" , closes match was: "+match_name+" with a score of: "+str(current_similarity_score))
        player_id = 0
    else:
        logger.info( "name match: "+name+" "+match_name+" playerId: "+str(player_id)+" similarity score: "+str(current_similarity_score))
    return player_id


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
        message = "failed to decode json from player mappings Primpy response"
        logger.error(message)
    else:
        status_code = 200
        message = "found vendor player mappings"
        vendor_player_map = {}
        for mapped_player in data["mappedPlayers"]:
            vendor_player_map[mapped_player["vendorPlayerId"]] = mapped_player["cbsPlayerId"]
    return status_code, message, vendor_player_map

