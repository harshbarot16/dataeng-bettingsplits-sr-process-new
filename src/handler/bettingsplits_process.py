""" module containing lambda handler to get betting split insights """
import json
import logging
import datetime
import time
import os
import pymongo
from pymongo.errors import PyMongoError
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_betting_splits(event, context):
    """ lambda handler to fetch betting splits insights for respective leagues """
    s3 = boto3.client("s3")
    status_code = 500
    endpoint = ""
    message = ""
    try:
        league = os.environ["LEAGUE"]
        bucket = os.environ["BUCKET"]
    except KeyError as key_error:
        message = "KeyError"
        logger.error("%s", key_error)
    else:
        try:
            response = s3.get_object(Bucket=bucket, Key=league)
            content = response['Body']
        except ClientError as ce:
            status_code = 501
            message = "s3 get object error"
            logger.error(ce.response["Error"]["Code"])
        else:
            try:
                json_data = json.loads(content.read())
            except ValueError:
                message = "failed to decode json"
                logger.error(message)
            else:
                for game_ids in json_data:
                    data, game_id_trunc = process_s3_objects(json_data[game_ids], game_ids, league)
                    status_code, message = load_betting_splits_docdb(game_id_trunc, data)

    response = {
        "statusCode": status_code,
        "body": "Betting Splits for " + endpoint + " games processed successfully",
        "message": message,
    }
    return response




def process_s3_objects(body,game_ids,league):
    """ Processes betting splits data to get the latest insights for all market """
    result = []
    split_id = game_ids.split('_')  # String split to get GameId, HomeTeamId, AwayTeamId
    for count in range(3):
        if 'event' in body['results'][count]:
            del body['results'][count]['event']
            if body['results'][count]['betting']['market'] == 'spread' or body['results'][count]['betting']['market'] == 'moneyLine':
                if league == 'nhl' and body['results'][count]['betting']['market'] == 'spread':
                    body['results'][count]['betting']['market'] = 'puckLine'
                if league == 'mlb' and body['results'][count]['betting']['market'] == 'spread':
                    body['results'][count]['betting']['market'] = 'runLine'
                iteration = 0
                for teams in body['results'][count]['betting']['selections']:
                    if teams['home_away'] == 'home':
                        body['results'][count]['betting']['selections'][iteration]['home_team_id'] = split_id[1]
                    else:
                        body['results'][count]['betting']['selections'][iteration]['away_team_id'] = split_id[2]
                    iteration += 1
        result.append(body['results'][count])
    return result, split_id[0]

def load_betting_splits_docdb(game_id_trunc,data):

    try:
        doc_db_connstring = os.environ["DOC_DB_CONNECTION_STRING"]
        mongo_client = get_client(doc_db_connstring)
        betting_splits_collection = os.environ["BETTING_SPLITS_MONGO_COLLECTION"]
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("Failed to connect to DocDB")
        logger.error("%s", key_error)
    else:
        try:
            betting = {'_id': game_id_trunc, 'Markets': data}
            betting_json = json.loads(json.dumps(betting, default=str))
            update_ts = time.time()
            betting_json['update_db_ts'] = datetime.datetime.fromtimestamp(update_ts, None)
            result = replace_one(mongo_client, betting_splits_collection, betting_json["_id"], betting_json)
            if not result.raw_result['ok']:
                logger.error(result.raw_result)
                logger.error("Error upserting document %s", betting_json["_id"])
        except PyMongoError as pm_error:
            status_code = 500
            message = "Pymongo Error"
            logger.error(message)
            logger.error("%s", pm_error)
        else:
            status_code = 200
            message = "Betting Split Insights loaded"

    return status_code, message

def replace_one(mongo_client, betting_splits_collection, _id, data):
    """ pymongo replace_one """
    return mongo_client.atlas[betting_splits_collection].replace_one({"_id": _id}, data, upsert=True)

def get_client(conn):
    """ get mongo client """
    return pymongo.MongoClient(conn)




