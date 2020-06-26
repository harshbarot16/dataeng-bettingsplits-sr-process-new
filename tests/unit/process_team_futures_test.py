""" test process_team_futures """
from botocore.exceptions import ClientError
import src.handler.process_team_futures

def test_process_team_futures_keyerror():
    """ test keyerror """
    event = s3_object_event()
    response = src.handler.process_team_futures.process_team_futures(event, None)
    assert response["statusCode"] == 500
    assert response["body"] == " team futures completed"
    assert response["message"] == "KeyError"

def test_process_team_futures_valueerror(environ):
    """ test value error """
    event = s3_object_event()
    src.handler.process_team_futures.json.loads = json_load_value_error
    response = src.handler.process_team_futures.process_team_futures(event, None)
    assert response["statusCode"] == 500
    assert response["body"] == "nfl team futures completed"
    assert response["message"] == "failed to decode json"

def test_process_team_futures_clienterror(environ, s3):
    """ test s3 client error """
    event = s3_object_event()
    s3.get_object = s3_get_object_value_error
    response = src.handler.process_team_futures.process_team_futures(event, None)
    assert response["statusCode"] == 501
    assert response["body"] == "nfl team futures completed"
    assert response["message"] == "s3 get object error"

def s3_get_object_value_error(bucket, key):
    """ raise client error """
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
                    "bucket": {"name": "dataeng-futures-wh-sls-qa"},
                },
            }
        ]
    }
