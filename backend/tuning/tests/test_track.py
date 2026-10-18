import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tuning import indicators as ind  # noqa: E402
from tuning import simulate, track  # noqa: E402


def bars(closes, opens=None, noise=0.0):
    n = len(closes)
    c = np.asarray(closes, float)
    o = np.asarray(opens if opens is not None else c, float)
    dates = pd.bdate_range("2026-01-01", periods=n).strftime("%Y%m%d")
    w = 1 + noise
    g = pd.DataFrame({"date": dates, "open": o, "high": np.maximum(o, c) * w, "low": np.minimum(o, c) / w,
                      "close": c, "volume": 1000.0, "fill_ok": True})
    for k in ("open", "high", "low", "close"):
        g["a" + k] = g[k]
    return g


def pick(g, day, **kw):
    b = {"stock_id": "1234", "name": "測試", "signals": [{"desc": "x", "h": kw.get("h", 10)}],
         "stop_pct": kw.get("stop", -0.05), "take_profit_pct": kw.get("tp"), "reliable": False,
         "sell_rule": None}
    state = []
    track.open_new(state, [b], g["date"].iloc[day], top=10)
    return state


def ev(st, g, t, holdings=frozenset()):
    return track.evaluate(st, {"1234": g.iloc[:t + 1]}, None if holdings is None else set(holdings),
                          g["date"].iloc[t])


def test_waiting_then_entry_at_next_open():
    g = bars([100] * 80)
    st = pick(g, 60)
    ev(st, g, 60)
    assert st[0]["status"] == "waiting" and st[0]["action"] == "待進場"
    ev(st, g, 61)
    assert st[0]["status"] == "open" and st[0]["entry_price"] == 100 and st[0]["entry_date"] == g["date"].iloc[61]


def test_stop_decided_at_close_executes_next_open():
    closes = [100] * 61 + [100, 100, 94, 93, 92]
    opens = [100] * 61 + [100, 100, 99, 91, 92]
    g = bars(closes, opens, noise=0.03)
    st = pick(g, 60, stop=-0.05, h=20)
    ev(st, g, 63)                                   # close 94 ≤ stop 95 → decided, not yet executed
    assert st[0]["status"] == "open" and st[0]["action"] == "出場" and "明日開盤" in st[0]["reason"]
    ev(st, g, 64)                                   # executed at the open of day 64 (91)
    assert st[0]["status"] == "closed" and st[0]["exit_date"] == g["date"].iloc[64]
    assert st[0]["exit_price"] == 91 and st[0]["ret_net"] < -0.08


def test_missed_runs_are_replayed():
    closes = [100] * 61 + [100, 100, 94, 93, 92, 92, 92]
    g = bars(closes, noise=0.03)
    st = pick(g, 60, stop=-0.05, h=20)
    ev(st, g, 61)
    ev(st, g, 67)                                   # several days skipped: the stop on day 63 is still caught
    assert st[0]["status"] == "closed" and st[0]["exit_date"] == g["date"].iloc[64]


def test_exit_signalled_kept_while_user_holds_then_closed_when_sold():
    closes = [100] * 61 + [100, 94, 93, 92, 91, 90]
    g = bars(closes, noise=0.03)
    st = pick(g, 60, stop=-0.05, h=20)
    ev(st, g, 62, {"1234"})
    ev(st, g, 64, {"1234"})
    assert st[0]["status"] == "exit_signalled" and "仍持有" in st[0]["reason"]
    ev(st, g, 65, set())
    assert st[0]["status"] == "closed" and st[0]["exit_reason"] == "user_sold"


def test_failed_holdings_query_never_closes_a_held_position():
    closes = [100] * 61 + [100, 101, 102, 103]
    g = bars(closes, noise=0.03)
    st = pick(g, 60, stop=-0.2, h=30)
    ev(st, g, 62, {"1234"})
    ev(st, g, 63, None)                             # query failed → unknown, not "sold"
    assert st[0]["status"] == "open" and st[0]["user_holds"] is True


def test_gap_up_open_is_skipped():
    opens = [100] * 61 + [110] + [110] * 3
    g = bars([100] * 61 + [110] * 4, opens)
    st = pick(g, 60)
    ev(st, g, 61)
    assert st[0]["status"] == "skipped"


def test_no_valid_open_is_skipped():
    g = bars([100] * 70)
    g.loc[61, "fill_ok"] = False
    st = pick(g, 60)
    ev(st, g, 62)
    assert st[0]["status"] == "skipped" and "有效開盤" in st[0]["reason"]


def test_planned_holding_exits_at_open_after_h_closes():
    g = bars([100] * 61 + [100.5] * 10, noise=0.03)
    st = pick(g, 60, h=3, stop=-0.2)
    ev(st, g, 64)
    assert st[0]["status"] == "closed" and "已達計畫 3 日" in st[0]["exit_reason"]
    assert st[0]["exit_date"] == g["date"].iloc[64]      # entry 61, closes 61..63, exit open 64 (= t+h+1)


def test_ex_dividend_drop_is_not_a_stop():
    raw = [100.0] * 63 + [92.0] * 3
    g = bars(raw, noise=0.03)
    f = 92.0 / 100.0
    for k in ("open", "high", "low", "close"):
        g.loc[:62, "a" + k] = g.loc[:62, k] * f       # backward adjustment: past bars scaled down
    st = pick(g, 60, stop=-0.05, h=20)
    ev(st, g, len(g) - 1)
    assert st[0]["status"] == "open", st[0].get("reason")
    assert st[0]["entry_price"] == 100.0                # shown raw entry price unchanged


def test_matches_backtest_label_for_time_exit():
    from tuning import evaluate
    g = bars(list(np.linspace(100, 110, 80)), noise=0.01)
    t, h = 50, 5
    fr = evaluate.forward_returns(g, h)
    f = ind.features(g)
    sim = simulate.simulate_position(g, f, ind.sell_signals(f), t + 1, h, stop_pct=-0.5)
    assert sim["status"] == "closed" and sim["exit_i"] == t + h + 1
    assert abs(sim["ret"] - fr["gross"].iloc[t]) < 1e-12


def test_no_duplicate_open_for_same_stock():
    g = bars([100] * 80)
    st = pick(g, 60)
    b = {"stock_id": "1234", "name": "測試", "signals": [{"desc": "x", "h": 5}], "reliable": False}
    assert track.open_new(st, [b], g["date"].iloc[61], top=10) == []
