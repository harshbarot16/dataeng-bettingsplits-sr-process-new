""" module containing lambda handler to get william hill futures """
import json
import logging
import os
import re
import boto3
from botocore.exceptions import ClientError


def process_team_futures(event, context):
    """ lambda handler to process william hill futures """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    s3 = boto3.client("s3")
    # retrieve bucket name and file_key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    logger.info('Reading %s from %s', file_key, bucket_name)
    status_code = 500
    message = ""
    league = ""
    try:
        book_id = os.environ["BOOKID"]
        league = os.environ["LEAGUE"]
    except KeyError as key_error:
        message = "KeyError"
        logger.error("%s", key_error)
    else:
        if re.match(league, file_key):
            try:
                obj = s3.get_object(Bucket=bucket_name, Key=file_key)
                logger.info(obj)
                futures = json.loads(obj['Body'].read())
            except ClientError as ce:
                status_code = 501
                message = "s3 get object error"
                logger.error(ce.response["Error"]["Code"])
            except ValueError:
                message = "failed to decode json"
                logger.error(message)
            else:
                status_code, message = process_team_futures_json(futures, book_id)

    response = {
        "statusCode": status_code,
        "body": league + " team futures completed",
        "message": message,
    }

    return response

def process_team_futures_json(futures, book_id):
    """ persist team futures to documentdb """
    return 200, "team futures processed"
