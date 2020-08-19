""" test process_team_futures """
from pymongo.errors import PyMongoError
import src.handler.process_player_futures

def test_persist_player_futures_keyerror(bad_environ, player_future_with_id):
    """ test keyerror """
    status_code, message = src.handler.process_player_futures.persist_league_player_futures(player_future_with_id)
    assert status_code == 500
    assert message == "KeyError"

def test_persist_player_futures_by_player_keyerror(bad_environ, player_future_with_id):
    """ test keyerror """
    status_code, message = src.handler.process_player_futures.persist_player_futures_by_player(player_future_with_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 500
    assert message == "KeyError"

def test_persist_player_futures_success(environ, player_future_with_id):
    """ test connerror """
    src.handler.process_player_futures.expire_resource = expire_resource_call
    status_code, message = src.handler.process_player_futures.persist_player_futures_by_player(player_future_with_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 200
    assert message == "player futures persisted"

def test_persist_player_futures_no_markets_success(environ, player_future_with_no_market):
    """ test connerror """
    src.handler.process_player_futures.expire_resource = expire_resource_call
    status_code, message = src.handler.process_player_futures.persist_league_player_futures(player_future_with_no_market)
    assert status_code == 200
    assert message == "player futures persisted"

def test_persist_player_futures_by_player_success(environ, player_future_with_id):
    """ test connerror """
    src.handler.process_team_futures.expire_resource = expire_resource_call
    status_code, message = src.handler.process_player_futures.persist_player_futures_by_player(player_future_with_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 200
    assert message == "player futures persisted"

def test_persist_player_futures_connerror(environ, player_future_with_id):
    """ test connerror """
    src.handler.process_player_futures.replace_one = error_replace_one
    status_code, message = src.handler.process_player_futures.persist_league_player_futures(player_future_with_id)
    assert status_code == 500
    assert message == "Pymongo Error"

def test_persist_player_futures_by_player_connerror(environ, player_future_with_id):
    """ test connerror """
    src.handler.process_player_futures.replace_one = error_replace_one
    status_code, message = src.handler.process_player_futures.persist_player_futures_by_player(player_future_with_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 200
    assert message == "player futures persisted"

def error_replace_one(mongo_client, team_collection, _id, data):
    """ raise PyMongo error """
    print("raising pymongo error")
    raise PyMongoError

def expire_resource_call(key):
     """ do nothing """
