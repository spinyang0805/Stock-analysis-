"""rank_paper.py — monthly ranking list, paper-traded (no real orders; public, no private data).

User decision (2026-10-19): research_rank found "momentum + revenue growth, top 20" ≈ 0050 over 2020–2026
(+30.4% vs +29.5% a year, t 1.6) — not proven. Instead of trading it, publish the list every month and
follow it on paper for half a year to see whether live results look like the backtest.

Rule (same as research_rank 'combo', fixed in advance):
  universe   current top-500 4-digit non-ETF stocks by 60-day turnover
  score      mean percentile of 12-1 momentum (close[t-21] / close[t-252] - 1) and the latest PUBLISHED
             monthly revenue YoY; both must exist
  rebalance  the first evening run of each calendar month ranks at that close; buy the top 20 equal-weight
             at the next open, hold until the next period's entry open
  costs      fees (60% discount) + tax on the replaced names only
Benchmarks: 0050 and 006208 over the same open-to-open window (adjusted, dividends included).

  python backend/tuning/rank_paper.py --source parquet --adjust-source exchange [--state public/data/rank_paper.json]

The state file is the whole public record; every run re-prices the unfinished periods from scratch, so a
missed day never loses anything.
"""
import argparse
import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "tuning"

from tuning.data import log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.research_rank import LOOK, pct_rank, wide  # noqa: E402
from tuning.run_tuning import add_common_args, prepare  # noqa: E402

TOP = 20
UNIVERSE = 500
ETFS = ("0050", "006208")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATE = os.path.join(REPO, "public", "data", "rank_paper.json")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--state", default=STATE)
    ap.add_argument("--force-rank", action="store_true", help="rank today even if this month already has a list")
    add_common_args(ap)
    return ap.parse_args(argv)


def load_state(path):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"strategy": "mom_rev", "top": TOP, "periods": []}


def rank_today(prep):
    """Top-TOP picks at the last calendar date: [{id, score, mom, rev}] (empty if the data is not ready)."""
    cal = list(prep["calendar"])
    t = len(cal) - 1
    if t < LOOK:
        log(f"[rank_paper] only {len(cal)} sessions loaded (< {LOOK + 1}); cannot rank")
        return []
    adj = prep["adjusted"]
    raw = wide(adj, "aclose", cal)
    close = raw.ffill()
    rev = wide(adj, "rev_yoy", cal)
    names = [c for c in prep["current"] if c in raw.columns and c not in ETFS and pd.notna(raw.iloc[t][c])]
    mom = (close.iloc[t - 21][names] / close.iloc[t - LOOK][names] - 1)
    ry = rev.iloc[t][names]
    score = pd.concat([pct_rank(mom), pct_rank(ry)], axis=1).mean(axis=1, skipna=False).dropna()
    if len(score) < 2 * TOP:
        log(f"[rank_paper] only {len(score)} names have both momentum and revenue; not ranking")
        return []
    top = score.sort_values(ascending=False).index[:TOP]
    return [{"id": c, "score": round(float(score[c]), 4), "mom": round(float(mom[c]), 4),
             "rev": round(float(ry[c]), 4)} for c in top]


def reprice(state, prep):
    """Entry / exit dates and returns for every unfinished period (entry = first session after as_of,
    exit = next period's entry open; open periods are marked at the last close)."""
    cal = list(prep["calendar"])
    adj = prep["adjusted"]
    last = cal[-1]
    costs = Costs()
    periods = state["periods"]
    for i, p in enumerate(periods):
        if p.get("status") == "closed":
            continue
        after = [d for d in cal if d > p["as_of"]]
        if not after:
            p["status"] = "waiting"
            continue
        p["entry"] = after[0]
        nxt = periods[i + 1] if i + 1 < len(periods) else None
        nxt_after = [d for d in cal if d > nxt["as_of"]] if nxt else []
        exit_d = nxt_after[0] if nxt_after else None

        def ret(sid):
            g = adj.get(sid)
            if g is None:
                return None
            g = g.set_index("date")
            if p["entry"] not in g.index or not np.isfinite(g.at[p["entry"], "aopen"]) or g.at[p["entry"], "aopen"] <= 0:
                return None
            e = g.at[p["entry"], "aopen"]
            if exit_d is not None:
                x = g["aopen"].get(exit_d, np.nan)
                if not np.isfinite(x):                         # no trade that day → last close before it
                    x = g.loc[g.index < exit_d, "aclose"].dropna().iloc[-1]
            else:
                x = g["aclose"].dropna().iloc[-1]
            return float(x / e - 1)

        rets = [ret(q["id"]) for q in p["picks"]]
        for q, r in zip(p["picks"], rets):
            q["ret"] = None if r is None else round(r, 4)
        ok = [r for r in rets if r is not None]
        gross = float(np.mean(ok)) if ok else 0.0
        prev = {q["id"] for q in periods[i - 1]["picks"]} if i > 0 else set()
        cur = {q["id"] for q in p["picks"]}
        denom = max(len(cur), len(prev), 1)
        turn = (len(prev - cur) * costs.sell(False) + len(cur - prev) * costs.buy()) / denom
        p["gross"] = round(gross, 4)
        p["net"] = round((1 + gross) * (1 - turn) - 1, 4)
        for etf in ETFS:
            r = ret(etf)
            p[f"b{etf}"] = None if r is None else round(r, 4)
        p["exit"] = exit_d
        p["marked"] = exit_d or last
        p["status"] = "closed" if exit_d else "open"


def main(argv=None):
    a = parse_args(argv)
    state = load_state(a.state)
    periods = state["periods"]
    unfinished = [q["id"] for p in periods if p.get("status") != "closed" for q in p["picks"]]
    a.no_holdings = False
    a.holdings = ",".join(dict.fromkeys(list(ETFS) + unfinished))
    # cheap pass first: latest date and the unfinished periods' prices
    prep = prepare(a, 0, 0.6, codes=unfinished)
    as_of = prep["calendar"][-1]
    new_month = not periods or periods[-1]["as_of"][:6] != as_of[:6]
    if (new_month or a.force_rank) and (not periods or periods[-1]["as_of"] != as_of):
        big = prepare(a, UNIVERSE, 1.6)            # point-in-time universe + revenue for the ranking
        picks = rank_today(big)
        if picks:
            periods.append({"as_of": as_of, "status": "waiting", "picks": picks})
            log(f"[rank_paper] new list for {as_of}: {', '.join(q['id'] for q in picks)}")
            prep = big
    reprice(state, prep)
    state.update({"strategy": "mom_rev", "top": TOP, "updated": as_of,
                  "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M")})
    os.makedirs(os.path.dirname(a.state), exist_ok=True)
    with open(a.state, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=1)
    log(f"[rank_paper] {len(periods)} periods, as of {as_of} → {a.state}")


if __name__ == "__main__":
    main()
