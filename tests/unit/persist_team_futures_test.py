""" test process_team_futures """
import json
import os
import pytest
from pymongo import MongoClient
import src.handler.process_team_futures

def test_persist_team_futures_keyerror(bad_environ):
    """ test keyerror """
    book_id = "wh:book:whnj"
    league = "nfl"
    status_code, message = src.handler.process_team_futures.persist_team_futures(get_json_futures(), book_id, league)
    assert status_code == 500
    assert message == "KeyError"

def test_persist_team_futures_connerror(environ):
    """ test connerror """
    book_id = "wh:book:whnj"
    league = "nfl"
    status_code, message = src.handler.process_team_futures.persist_team_futures(get_json_futures(), book_id, league)
    assert status_code == 500
    assert message == "Pymongo Connection Failure"

@pytest.fixture(name="environ")
def fix_environ():
    """ environ """
    os.environ["LEAGUE"] = "nfl"
    os.environ["BOOKID"] = "wh:book:whnj"
    # os.environ["DOC_DB_CONNECTION_STRING"] = "mongodb://atlas_write:weakWriteQA@datatech-sdf-docdb-qa.cluster-c5q8zvl01dua.us-east-1.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    os.environ["DOC_DB_CONNECTION_STRING"] = "atlas.mongodb.uri=mongodb://atlas_write:weakWriteQA@sdf-mongo-qa.transit.cbsig.net/atlas?authSource=admin"
    os.environ["MONGO_COLLECTION"] = "team_futures_wh"

@pytest.fixture(autouse=True)
def patch_mongo(monkeypatch):
    """ use bad client """
    def get_bad_client(conn):
        return MongoClient(connect=False)
    monkeypatch.setattr('src.handler.process_team_futures.get_client', get_bad_client)

def get_json_futures():
    """ get team future"""
    data = """
        [{
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
