"""track.py — follow every short-term pick, every trading day, until it is closed.

State is a JSON list of positions (history/out/daily/tracked_picks.json; in the cloud an asset of the
`daily-picks` release). Each run RE-SIMULATES every active position from its entry with simulate.py, so a
missed run never skips a stop / exit, and the outcome is identical to the backtest convention:

  waiting         picked, next session not traded yet (entry = next session OPEN)
  open            in the position; action 續抱 / 注意 / 出場（明日開盤）
  exit_signalled  the simulated exit happened but the user still holds the stock → alert daily until sold
  closed          virtual exit executed at the next open after the decision, or the user sold
  skipped         entry not possible (gap-up ≥ 9.5% or no valid open)

Holdings: a set of codes, or None when the holdings query failed — then no user_holds / user_sold
transitions are made that day (a failed query must never look like "the user sold everything").
"""
import json
import os

import numpy as np

from tuning import indicators as ind
from tuning.evaluate import Costs
from tuning.simulate import simulate_position

WARN_SIGNALS = ("s_ma20_break", "s_tl_up_break", "s_pat_bear_struct", "s_mkt_turn_bear", "s_whale_disp")
DEFAULT_STOP = -0.08
ACTIVE = ("waiting", "open", "exit_signalled")


def load_state(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_state(path, state):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def _r(x):
    return None if x is None or not np.isfinite(x) else round(float(x), 2)


def open_new(state, buys, as_of, top):
    """Open a 'waiting' position for each of today's top BUY candidates not already tracked
    (and not closed today — a same-day exit is not immediately re-entered)."""
    blocked = {p["stock_id"] for p in state if p["status"] in ACTIVE or p.get("exit_date") == as_of}
    added = []
    for b in buys[:top]:
        if b["stock_id"] in blocked:
            continue
        sig = b["signals"][0]
        p = {"id": f"{b['stock_id']}-{as_of}", "stock_id": b["stock_id"], "name": b["name"], "pick_date": as_of,
             "signals": [s["desc"] for s in b["signals"]], "plan_hold": int(sig["h"]),
             "sell_rule": b.get("sell_rule"), "stop_pct": b.get("stop_pct") or DEFAULT_STOP,
             "tp_pct": b.get("take_profit_pct"), "trial": not b.get("reliable", False),
             "status": "waiting", "entry_date": None, "entry_price": None, "user_holds": False}
        state.append(p)
        blocked.add(b["stock_id"])
        added.append(p)
    return added


def evaluate(state, adjusted, holdings, as_of, costs=Costs()):
    """Re-simulate every active position on the data up to as_of. Returns the positions closed today."""
    closed_today = []
    known = holdings is not None
    for p in state:
        if p["status"] not in ACTIVE:
            continue
        g = adjusted.get(p["stock_id"])
        holds = (p["stock_id"] in holdings) if known else p.get("user_holds", False)
        was_holding = p.get("user_holds", False)
        if known:
            p["user_holds"] = holds
        p["last_eval"] = as_of
        if g is None or not len(g):
            p.update(action="資料缺漏", reason="今日無此股資料，維持原狀態")
            continue
        g = g.reset_index(drop=True)
        dates = g["date"].astype(str).to_numpy()
        stale = dates[-1] != as_of
        after = np.flatnonzero(dates > p["pick_date"])
        if not len(after):
            p.update(action="待進場", reason="明日開盤進場" + ("" if not stale else f"（資料停在 {dates[-1]}）"))
            continue
        e = int(after[0])
        f = ind.features(g)
        sells = ind.sell_signals(f)
        rule = (p.get("sell_rule") or {}).get("signal")
        sim = simulate_position(g, f, sells, e, p["plan_hold"], p["stop_pct"], p.get("tp_pct"), rule,
                                ind.ATR_MULT.get(rule, 2.5))
        etf = str(p["stock_id"]).startswith("00")
        last = len(g) - 1
        if sim["status"] == "skipped":
            if holds:                                  # the user bought anyway → follow it from that open
                sim = simulate_position(g, f, sells, e, p["plan_hold"], p["stop_pct"], p.get("tp_pct"), rule,
                                        ind.ATR_MULT.get(rule, 2.5), gap_skip=np.inf)
            if sim["status"] == "skipped":             # not held, or no valid entry fill at all
                p.update(status="skipped", action="未進場", reason=sim["reason"], exit_date=dates[sim["exit_i"]],
                         closed_on=as_of)
                closed_today.append(p)
                continue
        p.update(entry_date=dates[e], entry_price=_r(float(g["open"].iloc[e])),
                 days_held=sim["held"], stop_price=_r(sim["stop"]), tp_price=_r(sim["tp"]),
                 last_close=_r(float(g["close"].iloc[last])),
                 ret_net=round(float(costs.net(sim["ret"], etf)), 4) if sim["ret"] is not None else None)
        stale_note = f"（資料停在 {dates[-1]}）" if stale else ""
        if "adj_factor" in g and float(g["adj_factor"].iloc[e]) < 0.999:   # an ex-date fell after the entry
            stale_note += "（持有期間除權息，停損／停利已依還原價調整）"
        if known and was_holding and not holds:
            # the user's own sale: mark at today's close (the earlier virtual exit, if any, is kept separately)
            mark = float(g["aclose"].iloc[last]) / float(g["aopen"].iloc[e]) - 1
            if sim["status"] == "closed":
                p["virtual_exit"] = {"date": dates[sim["exit_i"]], "reason": _reason(sim["reason"], rule),
                                     "ret_net": round(float(costs.net(sim["ret"], etf)), 4)}
            p.update(status="closed", action="已賣出", reason="交易紀錄顯示已無持股", exit_date=as_of,
                     exit_price=p["last_close"], exit_reason="user_sold",
                     ret_net=round(float(costs.net(mark, etf)), 4))
            p["closed_on"] = as_of
            closed_today.append(p)
            continue
        if sim["status"] == "closed":
            reason = _reason(sim["reason"], rule)
            if holds:
                p.update(status="exit_signalled", action="出場",
                         reason=f"{reason}（{dates[sim['decided_i']]} 收盤觸發；你仍持有，賣出前每天提醒）{stale_note}")
            else:
                p.update(status="closed", action="出場", reason=reason, exit_date=dates[sim["exit_i"]],
                         exit_price=_r(float(g["open"].iloc[sim["exit_i"]])), exit_reason=reason, closed_on=as_of)
                closed_today.append(p)
            continue
        if sim["pending"]:
            p.update(status="exit_signalled" if holds else "open", action="出場",
                     reason=f"{_reason(sim['pending'], rule)}，明日開盤出場{stale_note}")
            continue
        warns = [ind.SELL_DESC[w] for w in WARN_SIGNALS if w in sells and bool(sells[w].iloc[last])]
        p.update(status="open", action="注意" if warns else "續抱",
                 reason=("；".join(warns) if warns else "條件未觸發") + stale_note)
    return closed_today


def _reason(r, rule):
    return f"出場訊號：{ind.SELL_DESC.get(rule, rule)}" if r == "出場訊號" else r


def summary(state):
    done = [p for p in state if p["status"] == "closed" and p.get("ret_net") is not None]
    if not done:
        return None
    r = np.array([p["ret_net"] for p in done])
    return {"n": len(r), "win": float((r > 0).mean()), "mean": float(r.mean())}
