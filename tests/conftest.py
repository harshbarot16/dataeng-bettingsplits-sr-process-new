""" pytest fixtures """
import os
import json
import pytest
import boto3
import mongomock
from moto import mock_s3

@pytest.fixture(name="aws_credentials", scope="session", autouse=True)
def fix_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture(name="s3")
def fix_s3(team_future_without_id):
    """ s3 """
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="dataeng-futures-wh-qa")
        s3.put_object(
            Bucket="dataeng-futures-wh-qa",
            Body=get_team_future_for_fixture(),
            Key="nfl",
        )
        yield s3

@pytest.fixture(name="environ")
def fix_environ():
    """ environ """
    os.environ["LEAGUE"] = "nfl"
    os.environ["CBS_LEAGUE_ID"] = "59"
    os.environ["BOOKID"] = "wh:book:whnj"
    os.environ["DOC_DB_CONNECTION_STRING"] = "atlas.mongodb.uri=mongodb://atlas_write:weakWriteQA@sdf-mongo-qa.transit.cbsig.net/atlas?authSource=admin"
    os.environ["LEAGUE_MONGO_COLLECTION"] = "legue_team_futures_wh"
    os.environ["TEAM_MONGO_COLLECTION"] = "team_futures_wh"

@pytest.fixture(name="bad_environ")
def fix_bad_environ():
    """ environ """
    if "DOC_DB_CONNECTION_STRING" in os.environ:
        del os.environ["DOC_DB_CONNECTION_STRING"]
    if "LEAGUE" in os.environ:
        del os.environ["LEAGUE"]

@pytest.fixture(autouse=True)
def patch_mongo(monkeypatch):
    """ use mongomock client """
    def get_fake_client(conn):
        return mongomock.MongoClient()
    monkeypatch.setattr('src.handler.process_team_futures.get_client', get_fake_client)

@pytest.fixture(name="team_future_without_id")
def get_team_future():
    """ get team future"""
    data = """
        [{
            "_id": "27f71aad-3369-3143-aa6c-245fc3b70a5b",
            "id": "27f71aad-3369-3143-aa6c-245fc3b70a5b",
            "name": "Atlanta Falcons To Make The Playoffs",
            "sportId": "americanfootball",
            "competitionId": "007d7c61-07a7-4e18-bb40-15104b6eac92",
            "competitionName": "NFL",
            "eventType": "TNMT",
            "tradedInPlay": false,
            "started": false,
            "startTime": "2020-09-10T23:00Z",
            "active": true,
            "display": true,
            "expiryDateInMillis": 9223372036854775807,
            "markets": [{
                "id": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                "name": "To Make The Playoffs",
                "line": 0.0,
                "active": true,
                "display": true,
                "tradedInPlay": false,
                "selections": [{
                        "id": "1bec92b3-f0a2-3ac6-91a4-70e04e7e88f4",
                        "marketId": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                        "active": true,
                        "display": true,
                        "name": "No",
                        "price": {
                            "a": -300,
                            "d": 1.33,
                            "f": "1/3"
                        }
                    },
                    {
                        "id": "7a9ce639-b610-3bcc-b60d-d9a6fb369665",
                        "marketId": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                        "active": true,
                        "display": true,
                        "name": "Yes",
                        "price": {
                            "a": 240,
                            "d": 3.4,
                            "f": "12/5"
                        }
                    }
                ]
            }]
        }]
        """
    return json.loads(data)

@pytest.fixture(name="team_future_with_id")
def get_team_future_with_id():
    """ get team future with id """
    data = """
        [{
            "team_id" : 405,
            "_id": "27f71aad-3369-3143-aa6c-245fc3b70a5b",
            "id": "27f71aad-3369-3143-aa6c-245fc3b70a5b",
            "name": "Atlanta Falcons To Make The Playoffs",
            "sportId": "americanfootball",
            "competitionId": "007d7c61-07a7-4e18-bb40-15104b6eac92",
            "competitionName": "NFL",
            "eventType": "TNMT",
            "tradedInPlay": false,
            "started": false,
            "startTime": "2020-09-10T23:00Z",
            "active": true,
            "display": true,
            "expiryDateInMillis": 9223372036854775807,
            "markets": [{
                "id": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                "name": "To Make The Playoffs",
                "line": 0.0,
                "active": true,
                "display": true,
                "tradedInPlay": false,
                "selections": [{
                        "id": "1bec92b3-f0a2-3ac6-91a4-70e04e7e88f4",
                        "marketId": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                        "active": true,
                        "display": true,
                        "name": "No",
                        "price": {
                            "a": -300,
                            "d": 1.33,
                            "f": "1/3"
                        }
                    },
                    {
                        "id": "7a9ce639-b610-3bcc-b60d-d9a6fb369665",
                        "marketId": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                        "active": true,
                        "display": true,
                        "name": "Yes",
                        "price": {
                            "a": 240,
                            "d": 3.4,
                            "f": "12/5"
                        }
                    }
                ]
            }]
        }]
        """
    return json.loads(data)

@pytest.fixture(name="team_future_with_no_market")
def get_team_future_with_no_market():
    """ get team future with no market """
    data = """[
        {
        "_id": "0c8ec734-4d98-32ee-8852-a9c4fde64f32",
        "id": "0c8ec734-4d98-32ee-8852-a9c4fde64f32",
        "name": "2020 NFC South Winner",
        "sportId": "americanfootball",
        "competitionId": "007d7c61-07a7-4e18-bb40-15104b6eac92",
        "competitionName": "NFL",
        "eventType": "TNMT",
        "tradedInPlay": false,
        "started": false,
        "startTime": "2020-09-09T23:00:00.000Z",
        "active": true,
        "display": true,
        "expiryDateInMillis": null,
        "markets": [],
        "statsEventId": ""
        }
        ]
    """
    return json.loads(data)

@pytest.fixture(name="team_future_winner")
def get_team_future_with_winner():
    """ get team future with winner """
    data = """[
        {
        "_id": "0c8ec734-4d98-32ee-8852-a9c4fde64f32",
        "id": "0c8ec734-4d98-32ee-8852-a9c4fde64f32",
        "name": "2020 NFC South Winner",
        "sportId": "americanfootball",
        "competitionId": "007d7c61-07a7-4e18-bb40-15104b6eac92",
        "competitionName": "NFL",
        "eventType": "TNMT",
        "tradedInPlay": false,
        "started": false,
        "startTime": "2020-09-09T23:00:00.000Z",
        "active": true,
        "display": true,
        "expiryDateInMillis": null,
        "markets": [
            {
                "id": "d1dc0e87-6e5f-3446-930a-def3716c3352",
                "name": "Division Winner",
                "line": null,
                "active": true,
                "display": true,
                "tradedInPlay": false,
                "selections": [
                    {
                        "id": "615ff889-8e49-319d-b60a-1e83d34a9b9f",
                        "marketId": "d1dc0e87-6e5f-3446-930a-def3716c3352",
                        "active": true,
                        "display": true,
                        "name": "Atlanta Falcons",
                        "price": {
                            "a": 600,
                            "d": 7.0,
                            "f": "6/1"
                        },
                        "statsParticipantId": "323",
                        "team_id": 405
                    }
                ]
            }
        ],
        "statsEventId": ""
        }
        ]
    """
    return json.loads(data)

def get_team_future_for_fixture():
    """ get team future"""
    data = """
        [{
            "_id": "27f71aad-3369-3143-aa6c-245fc3b70a5b",
            "id": "27f71aad-3369-3143-aa6c-245fc3b70a5b",
            "name": "Atlanta Falcons To Make The Playoffs",
            "sportId": "americanfootball",
            "competitionId": "007d7c61-07a7-4e18-bb40-15104b6eac92",
            "competitionName": "NFL",
            "eventType": "TNMT",
            "tradedInPlay": false,
            "started": false,
            "startTime": "2020-09-10T23:00Z",
            "active": true,
            "display": true,
            "expiryDateInMillis": 9223372036854775807,
            "markets": [{
                "id": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                "name": "To Make The Playoffs",
                "line": 0.0,
                "active": true,
                "display": true,
                "tradedInPlay": false,
                "selections": [{
                        "id": "1bec92b3-f0a2-3ac6-91a4-70e04e7e88f4",
                        "marketId": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                        "active": true,
                        "display": true,
                        "name": "No",
                        "price": {
                            "a": -300,
                            "d": 1.33,
                            "f": "1/3"
                        }
                    },
                    {
                        "id": "7a9ce639-b610-3bcc-b60d-d9a6fb369665",
                        "marketId": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                        "active": true,
                        "display": true,
                        "name": "Yes",
                        "price": {
                            "a": 240,
                            "d": 3.4,
                            "f": "12/5"
                        }
                    }
                ]
            }]
        }]
        """
    return data
