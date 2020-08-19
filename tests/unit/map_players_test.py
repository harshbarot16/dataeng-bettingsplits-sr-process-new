""" test process_player_futures """
from botocore.exceptions import ClientError
import src.handler.map_players
import json

def test_map_players_keyerror(bad_environ):
    event = sns_nfl_event()
    response = src.handler.map_players.map_players(event, None)
    assert response["statusCode"] == 500
    assert response["body"] == " player futures completed"
    assert response["message"] == "KeyError"

def test_map_players_eventerror(environ):
    response = src.handler.map_players.map_players(None, None)
    assert response["statusCode"] == 500
    assert response["body"] == "nfl player futures completed"
    assert response["message"] == "TypeError"

def test_map_players_filter_futures(environ):
    futures = []
    future = {}
    future['name'] = 'test'
    futures.append(future)
    status_code, message, filtered_futures = src.handler.map_players.filter_futures(futures, "wh:book:whnj", "nfl", 59)
    assert status_code == 200
    assert message == "done"

def test_map_players_get_roster_player_ids_map(environ):
    status_code, message, filtered_futures = src.handler.map_players.get_roster_player_ids_map("nfl")
    assert status_code == 500

# def test_process_player_futures_incomplete_eventerror(environ):
#     event = s3_object_incomplete_event()
#     response = src.handler.process_player_futures.process_player_futures(event, None)
#     assert response["statusCode"] == 500
#     assert response["body"] == "nfl player futures completed"
#     assert response["message"] == "KeyError"
#
# def test_process_player_futures_valueerror(environ):
#     event = s3_object_mismatched_event()
#     response = src.handler.process_player_futures.process_player_futures(event, None)
#     assert response["statusCode"] == 500
#     assert response["body"] == "nfl player futures completed"
#     assert response["message"] == "KeyError"
#
# def test_process_player_futures_clienterror(environ, s3):
#     s3.get_object = s3_get_object_value_error
#     bucket_name = "dataeng-futures-wh-qa"
#     file_key = "nfl"
#     book_id = "wh:book:whnj"
#     league = "nfl"
#     status_code, message = src.handler.process_player_futures.process_s3(
#         s3, bucket_name, file_key, book_id, league, 59)
#     assert status_code == 501
#     assert message == "s3 get object error"
#
# def test_process_player_futures(environ):
#     event = sns_nfl_event()
#     response = src.handler.process_player_futures.process_player_futures(event, None)
#     assert response["statusCode"] == 501
#     assert response["body"] == "nfl player futures completed"

def s3_get_object_value_error(Bucket, Key):
    error_response = {}
    error_response["Error"] = {}
    error_response["Error"]["Code"] = "ConditionalCheckFailedException"
    raise ClientError(error_response, "GetObject")

def json_load_value_error(s3_obj, **kwargs):
    """ raise value error """
    raise ValueError

def s3_object_event():
    """ return fake s3 event """
    return {
        "Records": [
            {
                "s3": {
                    "object": {"key": "nfl"},
                    "bucket": {"name": "dataeng-futures-wh-qa"},
                },
            }
        ]
    }

def s3_object_incomplete_event():
    """ return fake s3 event """
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "dataeng-futures-wh-qa"},
                },
            }
        ]
    }

def s3_object_mismatched_event():
    return {
        "Records": [
            {
                "s3": {
                    "object": {"key": "mlb"},
                    "bucket": {"name": "dataeng-futures-wh-qa"},
                },
            }
        ]
    }

def sns_nfl_event():
    return {
        "Records": [
            {
                "Sns": {
                    "Message": "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2020-08-14T17:23:24.098Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AROARQN6WUJEPSP467VYI:dataeng-futures-wh-qa-nfl\"},\"requestParameters\":{\"sourceIPAddress\":\"3.235.144.72\"},\"responseElements\":{\"x-amz-request-id\":\"6FD5BECB4E1DF63D\",\"x-amz-id-2\":\"VoB824lxeBH97anWbwq2JmGsT2GgyrQ8AXZB4Mtlq11twPlHUAoZITAS9eRRUJUngAPKyZ83I2r57crZI8jOwn7D80vZDOXjZKz5UZ0rbj0=\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"wh-ingest-nfl-fanout\",\"bucket\":{\"name\":\"dataeng-futures-wh-qa\",\"ownerIdentity\":{\"principalId\":\"A2WJT0TKWSQKKW\"},\"arn\":\"arn:aws:s3:::dataeng-futures-wh-qa\"},\"object\":{\"key\":\"nfl\",\"size\":191254,\"eTag\":\"8ab4ecefd4bafc964363fb1a6b8c37b6\",\"sequencer\":\"005F36C890A9B60CE4\"}}}]}"
                }
            }
        ]
    }
