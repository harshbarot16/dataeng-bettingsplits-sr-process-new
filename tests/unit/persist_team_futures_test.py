""" test process_team_futures """
from pymongo.errors import PyMongoError
import src.handler.process_team_futures

def test_persist_team_futures_keyerror(bad_environ, team_future_without_id):
    """ test keyerror """
    status_code, message = src.handler.process_team_futures.persist_league_team_futures(team_future_without_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 500
    assert message == "KeyError"

def test_persist_team_futures_by_team_keyerror(bad_environ, team_future_with_id):
    """ test keyerror """
    status_code, message = src.handler.process_team_futures.persist_team_futures_by_team(team_future_with_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 500
    assert message == "KeyError"

def test_persist_team_futures_success(environ, team_future_with_id):
    """ test connerror """
    src.handler.process_team_futures.expire_resource = expire_resource_call
    status_code, message = src.handler.process_team_futures.persist_league_team_futures(team_future_with_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 200
    assert message == "team futures persisted"

def test_persist_team_futures_no_markets_success(environ, team_future_with_no_market):
    """ test connerror """
    src.handler.process_team_futures.expire_resource = expire_resource_call
    status_code, message = src.handler.process_team_futures.persist_league_team_futures(team_future_with_no_market, "wh:book:whnj", "nfl", 59)
    assert status_code == 200
    assert message == "team futures persisted"

def test_persist_team_futures_by_team_success(environ, team_future_with_id):
    """ test connerror """
    src.handler.process_team_futures.expire_resource = expire_resource_call
    status_code, message = src.handler.process_team_futures.persist_team_futures_by_team(team_future_with_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 200
    assert message == "team futures persisted"

def test_persist_team_futures_connerror(environ, team_future_with_id):
    """ test connerror """
    src.handler.process_team_futures.replace_one = error_replace_one
    status_code, message = src.handler.process_team_futures.persist_league_team_futures(team_future_with_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 500
    assert message == "Pymongo Error"

def test_persist_team_futures_by_team_connerror(environ, team_future_with_id):
    """ test connerror """
    src.handler.process_team_futures.replace_one = error_replace_one
    status_code, message = src.handler.process_team_futures.persist_team_futures_by_team(team_future_with_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 500
    assert message == "Pymongo Error"

def error_replace_one(mongo_client, team_collection, _id, data):
    """ raise PyMongo error """
    print("raising pymongo error")
    raise PyMongoError

def expire_resource_call(key):
    """ do nothing """
