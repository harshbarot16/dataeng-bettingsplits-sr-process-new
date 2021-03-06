""" test process_team_futures """
from botocore.exceptions import ClientError
import src.handler.process_team_futures


def test_process_team_futures_keyerror(bad_environ):
    """ test keyerror """
    event = s3_object_event()
    response = src.handler.process_team_futures.process_team_futures(event, None)
    assert response["statusCode"] == 500
    assert response["body"] == " team futures completed"
    assert response["message"] == "KeyError"

def test_process_team_futures_eventerror(environ):
    """ test typeerror """
    response = src.handler.process_team_futures.process_team_futures(None, None)
    assert response["statusCode"] == 500
    assert response["body"] == "nfl team futures completed"
    assert response["message"] == "TypeError"

def test_process_team_futures_incomplete_eventerror(environ):
    """ test s3 event keyerror """
    event = s3_object_incomplete_event()
    response = src.handler.process_team_futures.process_team_futures(event, None)
    assert response["statusCode"] == 500
    assert response["body"] == "nfl team futures completed"
    assert response["message"] == "KeyError"

def test_process_team_futures_valueerror(environ):
    """ test mismatched league error """
    event = s3_object_mismatched_event()
    response = src.handler.process_team_futures.process_team_futures(event, None)
    assert response["statusCode"] == 501
    assert response["body"] == "nfl team futures completed"
    assert response["message"] == "s3 get object error"

def test_implied_prob(environ, team_future_with_id):
    """ test implied prob """
    status_code, message, futures = src.handler.process_team_futures.add_implied_prob(team_future_with_id)
    assert status_code == 200
    assert message == "Success"
    assert "ip" in futures[0]["markets"][0]["selections"][0]["price"]

def test_process_team_futures_clienterror(environ, s3):
    """ test s3 client error """
    s3.get_object = s3_get_object_value_error
    bucket_name = "dataeng-futures-wh-qa"
    file_key = "nfl"
    book_id = "wh:book:whnj"
    league = "nfl"
    status_code, message = src.handler.process_team_futures.process_s3(
        s3, bucket_name, file_key, book_id, league, 59)
    assert status_code == 501
    assert message == "s3 get object error"

def test_process_team_futures_json_error(environ, s3):
    """ test value error """
    src.handler.process_team_futures.json.loads = json_load_value_error
    bucket_name = "dataeng-futures-wh-qa"
    file_key = "nfl"
    book_id = "wh:book:whnj"
    league = "nfl"
    status_code, message = src.handler.process_team_futures.process_s3(
        s3, bucket_name, file_key, book_id, league, 59)
    assert status_code == 500
    assert message == "failed to decode json"

def s3_get_object_value_error(Bucket, Key):
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
    """ return fake s3 event """
    return {
        "Records": [
            {
                "Sns": {
                    "Message": "{\"Records\":[{\"s3\":{\"bucket\":{\"name\":\"dataeng-futures-wh-qa\"},\"object\":{\"key\":\"nfl\"}}}]}"
                }
            }
        ]
    }
