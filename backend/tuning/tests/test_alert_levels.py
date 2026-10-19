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


def test_context_roles_priority_and_missing_ctx():
    ctx = {"prev_close": 100.0, "ma20": 98.0, "ma60": 95.0, "ma20_up": True, "atr": 2.0, "avg_vol20": 1500.0,
           "hh20": 105.0, "ll20": 90.0}
    out = {"sell_alerts": [{"stock_id": "2330", "name": "台積電", "trailing_stop": 95.0}],
           "buy": [{"stock_id": "2330", "name": "台積電"}, {"stock_id": "2317", "name": "鴻海",
                    "stop_loss_if_fill_at_ref": 92.0}, {"stock_id": "9999", "name": "無資料"}],
           "intraday_levels": {"2330": {"ctx": ctx}, "2317": {"ctx": ctx}, "1101": {"ctx": ctx}}}
    state = [{"stock_id": "1101", "name": "台泥", "status": "open", "stop_price": 88.0, "tp_price": 0}]
    rows = {r["code"]: r for r in alert_levels.build_context(out, state)}
    assert set(rows) == {"2330", "2317", "1101"}            # 9999 has no context → skipped
    assert rows["2330"]["role"] == "holding" and rows["2330"]["stop"] == 95.0
    assert rows["1101"]["role"] == "tracked" and rows["1101"]["target"] is None
    assert rows["2317"]["role"] == "pick" and rows["2317"]["stop"] == 92.0 and rows["2317"]["etf"] is False
    assert all(set(alert_levels.CTX_COLS) <= set(r) for r in rows.values())


def test_long_term_holdings_get_no_trailing_stop():
    ctx = {"prev_close": 30.0}
    out = {"sell_alerts": [{"stock_id": "00929", "name": "ETF", "trailing_stop": 29.31},
                           {"stock_id": "2330", "name": "A", "trailing_stop": 95.0},
                           {"stock_id": "2317", "name": "B", "trailing_stop": 90.0}],
           "intraday_levels": {k: {"ctx": ctx, "trailing_stop": 1.0} for k in ("00929", "2330", "2317")}}
    style = {"2317": "存股"}
    rows = {r["code"]: r for r in alert_levels.build_context(out, [], style=style)}
    assert rows["00929"]["stop"] is None and rows["2317"]["stop"] is None and rows["2330"]["stop"] == 95.0
    assert rows["00929"]["long_term"] and rows["2317"]["long_term"] and not rows["2330"]["long_term"]
    kinds = {(r[0], r[1]) for r in alert_levels.build(out, [], style)}
    assert ("00929", "trailing_stop") not in kinds and ("2330", "trailing_stop") in kinds


def test_context_follows_evening_advice():
    ctx = {"prev_close": 100.0}
    out = {"sell_alerts": [{"stock_id": "6182", "name": "合晶", "trailing_stop": 114.5},
                           {"stock_id": "8131", "name": "福懋科", "trailing_stop": 61.6}],
           "intraday_levels": {"6182": {"ctx": ctx}, "8131": {"ctx": ctx}}}
    advice = {"6182": {"action": "分批攤平（長線）", "stop": None, "allow_add": True},
              "8131": {"action": "建議出場", "stop": 60.0, "allow_add": False}}
    rows = {r["code"]: r for r in alert_levels.build_context(out, [], advice=advice)}
    assert rows["6182"]["long_term"] and rows["6182"]["stop"] is None and rows["6182"]["allow_add"]
    assert not rows["8131"]["long_term"] and rows["8131"]["stop"] == 60.0 and not rows["8131"]["allow_add"]
