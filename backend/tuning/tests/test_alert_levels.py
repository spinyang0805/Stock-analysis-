import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tuning import alert_levels  # noqa: E402


def test_build_levels_priorities_and_dedup():
    state = [
        {"stock_id": "2330", "name": "台積電", "status": "open", "user_holds": True, "stop_price": 1000.0,
         "tp_price": 1200.0},
        {"stock_id": "2317", "name": "鴻海", "status": "open", "user_holds": False, "stop_price": 200.0,
         "tp_price": None},
        {"stock_id": "1101", "name": "台泥", "status": "closed", "stop_price": 30.0},
    ]
    out = {"sell_alerts": [{"stock_id": "2330", "name": "台積電"}],
           "intraday_levels": {"2330": {"trailing_stop": 1010.0, "ma20_break": 1050.0}}}
    rows = {(r[0], r[1]): r for r in alert_levels.build(out, state)}
    assert rows[("2330", "stop")][5] == 1 and rows[("2317", "stop")][5] == 2
    assert ("2317", "take_profit") not in rows and ("1101", "stop") not in rows
    assert rows[("2330", "trailing_stop")][2] == "below" and rows[("2330", "take_profit")][2] == "above"
    assert rows[("2330", "ma20_break")][5] == 2


def test_duplicate_level_keeps_higher_priority():
    state = [{"stock_id": "2330", "name": "A", "status": "open", "user_holds": False, "stop_price": 100.0},
             {"stock_id": "2330", "name": "A", "status": "exit_signalled", "user_holds": True, "stop_price": 101.0}]
    rows = {(r[0], r[1]): r for r in alert_levels.build({}, state)}
    assert rows[("2330", "stop")][5] == 1 and rows[("2330", "stop")][3] == 101.0
