""" test fix futures """
import src.handler.process_team_futures

def test_fix_futures(environ, team_future_without_id):
    """ test fix fixtures """
    src.handler.process_team_futures.get_stats_vendor_team_map = get_stats_vendor_team_map
    src.handler.process_team_futures.get_wh_vendor_team_map = get_wh_vendor_team_map
    status_code, message, filtered_futures = src.handler.process_team_futures.fix_futures(team_future_without_id, "wh:book:whnj", "nfl", 59)
    assert status_code == 200
    assert message == "done"
    assert filtered_futures[0]["team_id"] == 405

def test_fix_team_futures(environ, team_future_with_id):
    """ test fix fixtures """
    team_futures = src.handler.process_team_futures.fix_team_futures(team_future_with_id)
    assert 405 in team_futures.keys()

def test_fix_team_futures_winner(environ, team_future_winner):
    """ test fix fixtures """
    team_futures = src.handler.process_team_futures.fix_team_futures(team_future_winner)
    assert 405 in team_futures.keys()

def test_fix_team_futures_winner_no_market(environ, team_future_with_no_market):
    """ test fix fixtures """
    team_futures = src.handler.process_team_futures.fix_team_futures(team_future_with_no_market)
    assert not team_futures

def get_stats_vendor_team_map(league):
    """ return map """
    vendor_team_map = {323 : 405}
    return 200, "good", vendor_team_map

def get_wh_vendor_team_map(league):
    """ return map """
    vendor_team_map = {"Atlanta Falcons" : 405}
    return 200, "good", vendor_team_map
