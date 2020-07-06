""" module containing lambda handler to get william hill futures """
import json
import logging
import os
from pymongo.errors import ConnectionFailure
from pymongo import MongoClient
import boto3
from botocore.exceptions import ClientError


def process_team_futures(event, context):
    """ lambda handler to process william hill futures """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    s3 = boto3.client("s3")
    status_code = 500
    message = ""
    league = ""
    try:
        book_id = os.environ["BOOKID"]
        league = os.environ["LEAGUE"]
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
    except KeyError as key_error:
        message = "KeyError"
        logger.error("%s", key_error)
    except TypeError as type_error:
        message = "TypeError"
        logger.error("%s", type_error)
    else:
        if league == file_key:
            status_code, message = process_s3(s3, bucket_name, file_key, book_id, league)
        else:
            status_code = 200
            message = "file doesn't match league"
    response = {
        "statusCode": status_code,
        "body": league + " team futures completed",
        "message": message,
    }

    return response

def process_s3(s3, bucket_name, file_key, book_id, league):
    """ get s3 file """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = obj['Body'].read()
        logger.info(data)
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
        status_code, message = persist_team_futures(futures, book_id, league)
        logger.info(json.dumps(futures))

    return status_code, message

def persist_team_futures(futures, book_id, league):
    """ persist to documentDb """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
        doc_db_connstring = os.environ["DOC_DB_CONNECTION_STRING"]
        mongo_client = get_client(doc_db_connstring)
        collection = os.environ["MONGO_COLLECTION"]
        tf_collection = mongo_client.atlas[collection]
    except KeyError as key_error:
        status_code = 500
        message = "KeyError"
        logger.error("%s", key_error)
    else:
        try:
            for team_future in futures:
                team_future["_id"] = team_future["id"]
                tf_collection.replace_one({"_id" : team_future["_id"]}, team_future, {"upsert": "true"})
            status_code = 200
            message = "team futures processed"
        except ConnectionFailure:
            status_code = 500
            message = "Pymongo Connection Failure"

    return status_code, message

def get_client(conn):
    """ get mongo client """
    return MongoClient(conn)
