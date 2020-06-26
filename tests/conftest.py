""" pytest fixtures """
import os
import pytest
import boto3
from moto import mock_s3


@pytest.fixture(name="aws_credentials", scope="session", autouse=True)
def fix_aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(name="s3", scope="session", autouse=True)
def fix_s3():
    """ s3 """
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="dataeng-futures-wh-sls-qa")
        s3.put_object(
            Bucket="dataeng-futures-wh-sls-qa",
            Body=get_team_future(),
            Key="nfl",
        )
        yield s3

@pytest.fixture(name="environ", scope="session")
def fix_environ():
    """ environ """
    os.environ["LEAGUE"] = "nfl"
    os.environ["BOOKID"] = "wh:book:whnj"

def get_team_future():
    """ get team future"""
    data = """
        [
        {
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
            "markets": [
                {
                    "id": "743cbdf5-149e-3a07-8a96-bc8a8d8b1988",
                    "name": "To Make The Playoffs",
                    "line": 0.0,
                    "active": true,
                    "display": true,
                    "tradedInPlay": false,
                    "selections": [
                        {
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
                }
            ]
        }
        ]
        """
    return data
