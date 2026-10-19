"""combo_daily.py — evening job: "combination punch" (組合拳) candidates, paper-tracked until the strict sell.

User decision (2026-10-19): the morning buy candidates switch from single signals to the combo rule
(research_combo 'main' buy + 'strict3' sell = 嚴格 B), shown on its own website page. Each candidate is
followed on paper: waiting (buy at the next open) → holding → sell_signal (sell at the next open) → closed
(dropped from the active list, kept in history). Every run re-simulates each position from its entry, so a
missed day never skips a sell. Also writes a traffic light for every stock of the monthly ranking list.

Rules (fixed in advance, same for every stock — see research_combo for the theory):
  BUY   stage 2 (close > rising MA60) and market not bearish + real breakout (3 days) + money in (5 days)
        + chips / relative strength / revenue (first day all four hold)
  SELL  ≥ 2 different structure breaks and ≥ 2 confirmation kinds in 10 days, or stage 4 (close < falling
        MA60), or close ≤ entry × 0.90
Universe: today's top 500 by 60-day turnover. Public output, no private data.

  python backend/tuning/combo_daily.py --source parquet --adjust-source exchange [--state public/data/combo_signals.json]
"""
import argparse
import json
import os
import sys
from datetime import datetime

import numpy as np

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "tuning"

from tuning import indicators as ind  # noqa: E402
from tuning import research_combo as rc  # noqa: E402
from tuning.data import log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.run_tuning import add_common_args, prepare  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATE = os.path.join(REPO, "public", "data", "combo_signals.json")
RANK = os.path.join(REPO, "public", "data", "rank_paper.json")
UNIVERSE = 500
KEEP_CLOSED = 120
SELL_RULE = "strict3"
CAT = {"trig": ("突破", rc.TRIGGER), "money": ("資金", rc.MONEY), "chips": ("籌碼", rc.CHIPS),
       "rs": ("相對強度", rc.RS), "rev": ("營收", rc.REV)}
WIN = {"trig": 3, "money": 5, "chips": 10, "rs": 10, "rev": 40}


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--state", default=STATE)
    ap.add_argument("--years", type=float, default=1.6)
    ap.add_argument("--seed-days", type=int, default=40,
                    help="first run only: replay the last N sessions so the page starts with history")
    add_common_args(ap)
    return ap.parse_args(argv)


def fired(sigs, keys, i, w, desc):
    """Descriptions of the signals among keys that fired in the w sessions ending at i."""
    out = []
    for k in keys:
        if k in sigs and sigs[k].to_numpy(bool)[max(0, i - w + 1):i + 1].any():
            out.append(desc.get(k, k))
    return out


def buy_reasons(det, i):
    b, lay = det["buys"], det["layers"]
    r = {}
    for key, (name, keys) in CAT.items():
        got = fired(b, keys, i, WIN[key], ind.BUY_DESC)
        if key == "money" and lay["obv_on"][max(0, i - 4):i + 1].any():
            got.append("OBV 20/60 黃金交叉")
        if got:
            r[name] = got
    return r


def sell_reasons(det, i):
    s, lay = det["sells"], det["layers"]
    r = {}
    st = fired(s, rc.STRUCT, i, 10, ind.SELL_DESC)
    if st:
        r["結構破壞"] = st
    money = fired(s, rc.CONF_CATS["money"], i, 10, ind.SELL_DESC)
    if lay["obv_off"][max(0, i - 9):i + 1].any():
        money.append("OBV 20/60 死亡交叉")
    for name, got in (("資金流出", money), ("籌碼賣出", fired(s, rc.CONF_CATS["chips"], i, 10, ind.SELL_DESC)),
                      ("環境轉壞", fired(s, rc.CONF_CATS["env"], i, 10, ind.SELL_DESC))):
        if got:
            r[name] = got
    if lay["stage4"][i]:
        r["第四階段"] = ["收盤跌破下彎的季線"]
    return r


def evaluate(pos, g, det, sell_mask, as_of):
    """Re-simulate one paper position from its signal day with this run's prices."""
    d = g["date"].astype(str).to_numpy()
    ao, ac = g["aopen"].to_numpy(float), g["aclose"].to_numpy(float)
    raw_o, raw_c = g["open"].to_numpy(float), g["close"].to_numpy(float)
    ok = (g["fill_ok"] if "fill_ok" in g else np.ones(len(g), bool))
    ok = np.asarray(ok, bool)
    n = len(g)
    t = int(np.searchsorted(d, pos["signal_date"]))
    if t >= n or d[t] != pos["signal_date"]:
        return pos                                         # signal day not in this run's data: leave as is
    e = t + 1
    if e >= n:
        pos["status"] = "waiting"
        return pos
    if not ok[e] or ao[e] >= ac[t] * 1.095:
        pos.update(status="skipped", note="隔天開盤跳空大漲或無法成交，放棄")
        return pos
    entry = ao[e]
    pos.update(entry_date=d[e], entry_price=round(float(raw_o[e]), 2))
    hit = np.flatnonzero(sell_mask[e:] | (ac[e:] <= entry * (1 - rc.STOP)))
    dec = e + int(hit[0]) if len(hit) else None
    if dec is not None and dec - e >= rc.CAP:
        dec = e + rc.CAP - 1
    last = n - 1
    pos["last_close"] = round(float(raw_c[last]), 2)
    pos["days"] = int(last - e + 1)
    if dec is None or dec - e >= rc.CAP:
        pos.update(status="holding", ret=round(float(ac[last] / entry - 1), 4))
        pos.pop("sell_reasons", None)
        return pos
    why = sell_reasons(det, dec) or {}
    if ac[dec] <= entry * (1 - rc.STOP):
        why = {"停損": ["收盤比買價跌 10%"], **why}
    x = dec + 1
    while x < n and not ok[x]:
        x += 1
    pos["sell_reasons"] = why
    pos["sell_signal_date"] = d[dec]
    if x >= n:                                             # decided at the last close → sell tomorrow
        pos.update(status="sell_signal", ret=round(float(ac[last] / entry - 1), 4))
        return pos
    net = Costs().net(ao[x] / entry - 1, False)
    pos.update(status="closed", exit_date=d[x], exit_price=round(float(raw_o[x]), 2), ret=round(float(net), 4),
               days=int(x - e))
    return pos


def replay(det_by, sell_by, current, names, days, as_of):
    """First run: buys of the last `days` sessions, one position at a time per stock (today's universe)."""
    out = []
    for sid in sorted(current):
        if sid not in det_by:
            continue
        g, det, buy = det_by[sid]
        d = g["date"].astype(str).to_numpy()
        n = len(g)
        i = max(0, n - days)
        while i < n - 1:                                   # today's own signals are added as 'new' later
            if not buy["main"][i]:
                i += 1
                continue
            p = {"stock_id": sid, "name": names.get(sid, ""), "signal_date": d[i], "status": "waiting",
                 "signal_close": round(float(g["close"].iloc[i]), 2), "buy_reasons": buy_reasons(det, i)}
            p = evaluate(p, g, det, sell_by[sid], as_of)
            out.append(p)
            if p["status"] not in ("closed", "skipped"):
                break
            nxt = np.searchsorted(d, p.get("exit_date", d[i]))
            i = max(i + 1, int(nxt))
    return out


def light(det, sell_mask, i):
    if sell_mask[i]:
        return "red"
    return "green" if det["layers"]["state"][i] else "yellow"


def main(argv=None):
    a = parse_args(argv)
    state = {"positions": [], "closed": []}
    seed = not os.path.exists(a.state)
    if not seed:
        with open(a.state, encoding="utf-8") as fh:
            state = json.load(fh)
    active = [p for p in state.get("positions", []) if p["status"] in ("waiting", "holding", "sell_signal")]
    rank_ids = []
    if os.path.exists(RANK):
        with open(RANK, encoding="utf-8") as fh:
            per = (json.load(fh).get("periods") or [])
        rank_ids = [q["id"] for q in per[-1]["picks"]] if per else []
    a.no_holdings = False
    a.holdings = ",".join(dict.fromkeys([p["stock_id"] for p in active] + rank_ids)) or None
    if a.holdings is None:
        a.no_holdings = True
    prep = prepare(a, UNIVERSE, a.years)
    as_of = prep["calendar"][-1]
    adj = prep["adjusted"]
    current = set(prep["current"])
    want = current | {p["stock_id"] for p in active} | set(rank_ids)
    names = {sid: prep["meta"].get(sid, {}).get("name", "") for sid in want}
    det_by, sell_by = {}, {}
    for sid in sorted(want):
        g = adj.get(sid)
        if g is None or len(g) < 120:
            continue
        g = g.reset_index(drop=True)
        buy, sell, _, _, _, det = rc.masks(g, detail=True)
        det_by[sid], sell_by[sid] = (g, det, buy), sell[SELL_RULE]
    log(f"[combo_daily] {len(det_by)} stocks evaluated, as of {as_of}")
    if seed:
        active = replay(det_by, sell_by, current, names, a.seed_days, as_of)
        log(f"[combo_daily] first run: replayed {len(active)} positions over the last {a.seed_days} sessions")
    # new candidates: the buy fired at today's close, in today's universe, not already followed
    have = {p["stock_id"] for p in active if p["status"] in ("waiting", "holding", "sell_signal")}
    new = []
    for sid in sorted(current):
        if sid not in det_by or sid in have:
            continue
        g, det, buy = det_by[sid]
        if str(g["date"].iloc[-1]) != as_of or not buy["main"][-1]:
            continue
        new.append({"stock_id": sid, "name": names.get(sid, ""), "signal_date": as_of, "status": "waiting",
                    "signal_close": round(float(g["close"].iloc[-1]), 2), "buy_reasons": buy_reasons(det, len(g) - 1)})
    positions = []
    closed = list(state.get("closed", []))
    for p in active + new:
        sid = p["stock_id"]
        if sid in det_by:
            g, det, _ = det_by[sid]
            p = evaluate(p, g, det, sell_by[sid], as_of)
        p["in_rank"] = sid in rank_ids
        if p["status"] in ("closed", "skipped"):
            closed.insert(0, {**p, "closed_on": as_of})
        else:
            positions.append(p)
    lights = {}
    for sid in rank_ids:
        if sid in det_by:
            g, det, _ = det_by[sid]
            i = len(g) - 1
            lights[sid] = light(det, sell_by[sid], i)
    done = [c for c in closed if c["status"] == "closed"]
    summary = {"n": len(done), "win": round(float(np.mean([c["ret"] > 0 for c in done])), 3) if done else None,
               "mean": round(float(np.mean([c["ret"] for c in done])), 4) if done else None}
    state = {"updated": as_of, "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"), "rule": "combo_strictB",
             "new_today": [p["stock_id"] for p in new], "positions": positions, "closed": closed[:KEEP_CLOSED],
             "rank_lights": lights, "summary": summary}
    os.makedirs(os.path.dirname(a.state), exist_ok=True)
    with open(a.state, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=1)
    log(f"[combo_daily] new {len(new)}, active {len(positions)}, "
        f"sell signals {sum(p['status'] == 'sell_signal' for p in positions)} → {a.state}")


if __name__ == "__main__":
    main()
