"""research_portfolio.py — capital-constrained portfolio backtest of the 組合拳 rules (research only).

Per-trade averages ignore that money is limited and trades cluster. This simulates ONE account:
  * start 1.0, at most N positions, each new position = equity / N at the entry open (no leverage)
  * signals at the close of day t fill at the open of t+1 (skip gap-up ≥ 9.5% or no valid open)
  * when more signals than free slots: highest priority first (default: 20-day relative strength vs TAIEX)
  * exits decided at a close (sell rule / stop / profit rule), filled at the next valid open
  * idle cash: 'cash' (0%) or '0050' (parked in 0050, sold first when a slot is needed)
  * costs: fee 0.1425% × 0.6 per side + 0.3% sell tax (0.1% for 0050)
Benchmark: 0050 buy-and-hold (total return, adjusted). Metrics: CAGR, max drawdown, worst year, % losing months,
exposure, trades, win rate.

  python backend/tuning/research_portfolio.py --source parquet --adjust-source exchange --years 8 [--variants a,b]
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
if __package__ in (None, ""):
    __package__ = "tuning"

from tuning import indicators as ind  # noqa: E402
from tuning import research_combo as rc  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.run_tuning import add_common_args, prepare  # noqa: E402

COSTS = Costs()
BUY_C, SELL_C, SELL_ETF = COSTS.buy(), COSTS.sell(False), COSTS.sell(True)

# variant = dict of knobs; every variant is listed in the report (pre-registered list, no search on results)
VARIANTS = {
    "base_N10_cash": dict(n=10, idle="cash", bear="none"),
    "base_N20_cash": dict(n=20, idle="cash", bear="none"),
    "N10_idle0050": dict(n=10, idle="0050", bear="none"),
    "N20_idle0050": dict(n=20, idle="0050", bear="none"),
    "N10_idle0050_bear240": dict(n=10, idle="0050", bear="ma240"),
    "N20_idle0050_bear240": dict(n=20, idle="0050", bear="ma240"),
}


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--universe", type=int, default=500)
    ap.add_argument("--years", type=float, default=8.0)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--variants", default=",".join(VARIANTS))
    ap.add_argument("--extra", default=None, help="JSON {name: knobs} of more variants")
    ap.add_argument("--start", default="20200102", help="first trading day of the account")
    ap.add_argument("--report", default=os.path.join(OUT_DIR, "research_portfolio.md"))
    add_common_args(ap)
    ap.set_defaults(no_holdings=False, holdings="0050,00631L")
    return ap.parse_args(argv)


OVERHEAT = {"s_rsi_p80", "s_rsi_p90", "s_kd_x_80", "s_bb_upper_fail", "s_shoot_high", "s_engulf_high",
            "s_val_pe_exp", "s_val_pb_exp"}         # "too hot" is not "turning down": never part of a fast exit


def stock_job(a):
    """Per stock (calendar-aligned): buy-signal day, strict-B sell, fast sell (>=2 different NON-overheat sell
    signals in 5 days), 20-day RS vs the market, ATR14."""
    sid, g, cp, n_cal = a
    g = g.reset_index(drop=True)
    buy, sell, atr, S, _, det = rc.masks(g, detail=True)
    sl = det["sells"]
    sig = np.array([v.to_numpy(bool) for k, v in sl.items() if k not in OVERHEAT])
    two = sum(rc.recent(m, 5).astype(int) for m in sig) >= 2
    f = ind.features(g)
    rs20 = (f["rs"] / f["rs"].shift(20) - 1).to_numpy() if "rs" in f else np.full(len(g), np.nan)
    out = {}
    for k, v in (("buy", buy["main"]), ("sell", sell["strict3"]), ("fast", two)):
        m = np.zeros(n_cal, bool)
        m[cp] = v
        out[k] = m
    for k, v in (("rs20", rs20), ("atr", atr)):
        r = np.full(n_cal, np.nan)
        r[cp] = v
        out[k] = r
    return sid, out


def simulate(knobs, P, sig, mkt, cal, start_i):
    """Day loop over one account. knobs:
      n        max positions;  idle  'cash' | '0050'
      bear     'none' | 'ma240' (TAIEX below a falling 240-day MA → ALSO exit on the fast rule, stop 7%)
      expo     'none' | 'regime' (new-entry slots: TAIEX regime +1 → n, 0 → n/2, −1 → n/4)
               | 'ma240' (below 240-day MA → n/2, below a falling one → no new entries)
      size     'equal' (equity / n) | 'risk' (min(1/n, 0.5% / stop distance) of equity)
      stopmode 'fixed' (stop, default 10%) | 'atr' (clip(3·ATR14/close, 6%, 12%) fixed at entry)
      trail    0 | 1 (after the best close ≥ +20%: exit if close < best close − 4·ATR14)
      prio     'rs' (20-day RS) | 'rank' (monthly momentum+revenue score)
      slip     per-side slippage fraction (default 0)
      settle   0 (sale cash reusable the same day — Taiwan nets buys/sells at T+2 settlement) | 2 (conservative:
               sale proceeds usable only 2 sessions later)
      capital  starting money in NT$ (default 10,000,000) — a new position may not exceed 5% of the stock's
               20-day average traded value
      mode     'combo' | 'etf_timing' (only 0050: hold it while TAIEX closed above its 240-day MA, else cash)
      cap500   1 (default) | 0 — exit after 500 sessions held"""
    O, C, OK = P["open"], P["close"], P["ok"]
    H, L = P["high"], P["low"]
    CF = P["close_ff"]                     # last known close (suspended / missing rows keep their value)
    BUY, SELL, FAST, RS, ATR = sig["buy"], sig["sell"], sig["fast"], sig["rs20"], sig["atr"]
    RANK = sig.get("rank")
    e50 = P["e50"]
    T, K = O.shape
    n, idle = knobs["n"], knobs["idle"]
    bear_mode, expo = knobs.get("bear", "none"), knobs.get("expo", "none")
    size, stopmode, trail = knobs.get("size", "equal"), knobs.get("stopmode", "fixed"), knobs.get("trail", 0)
    prio, slip, stop = knobs.get("prio", "rs"), knobs.get("slip", 0.0), knobs.get("stop", 0.10)
    settle, capital = knobs.get("settle", 0), knobs.get("capital", 1e7)
    mode, cap500 = knobs.get("mode", "combo"), knobs.get("cap500", 1)
    TV = P["tv20"]
    cash, park = 1.0, 0.0
    unsettled = []                         # [(usable_from_day, amount)] when settle > 0
    skipped = []                           # (day, k, reason) signals that were not bought
    pos = {}                               # k → [shares, entry_px, entry_day, stop_frac, best_close]
    pend_exit, pend_buy = set(), []
    trades, equity, expos = [], np.full(T, np.nan), np.zeros(T)
    for t in range(start_i, T):
        for k in list(pend_exit):
            locked = (np.isfinite(H[t, k]) and H[t, k] == L[t, k] and t > 0
                      and O[t, k] <= CF[t - 1, k] * 0.905)          # limit-down all day: the sell cannot fill
            if k in pos and OK[t, k] and np.isfinite(O[t, k]) and not locked:
                sh, ep, ed, _, _ = pos.pop(k)
                px = O[t, k] * (1 - slip)
                if settle:
                    unsettled.append((t + settle, sh * px * (1 - SELL_C)))
                else:
                    cash += sh * px * (1 - SELL_C)
                trades.append((ed, t, px / ep * (1 - SELL_C) / (1 + BUY_C) - 1, k))
                pend_exit.discard(k)
        if unsettled:
            cash += sum(a for d_, a in unsettled if d_ <= t)
            unsettled = [(d_, a) for d_, a in unsettled if d_ > t]
        if mode in ("mix_timing", "mix_hold"):
            ws = knobs["mix"]                                   # {code: weight}
            want_in = mode == "mix_hold" or (t > 0 and mkt["above"][knobs.get("ma", 240)][t - 1])
            if not hasattr(simulate, "_mix") or simulate._mix[0] is not knobs:
                simulate._mix = (knobs, {})
            held = simulate._mix[1]
            if want_in and not held and cash > 1e-9 and all(OK[t, P["col"][c]] for c in ws):
                for c, w in ws.items():
                    k_ = P["col"][c]
                    held[k_] = cash * w / (O[t, k_] * (1 + slip) * (1 + BUY_C))
                cash = 0.0
            elif not want_in and held and all(OK[t, k_] for k_ in held):
                for k_, sh in held.items():
                    cash += sh * O[t, k_] * (1 - slip) * (1 - SELL_ETF)
                trades.append((t, t, 0.0, e50))
                held.clear()
            val = sum(sh * CF[t, k_] for k_, sh in held.items())
            equity[t] = cash + val
            expos[t] = 1.0 if held else 0.0
            if t + 1 >= T:
                equity[t] -= val * (SELL_ETF + slip)
            continue
        if mode in ("etf_timing", "etf_hold"):
            # one ETF (knob etf, default 0050): etf_hold = always; etf_timing = only while TAIEX closed above its
            # 240-day MA the previous day, else cash
            ex_ = P["col"][knobs.get("etf", "0050")]
            want_in = mode == "etf_hold" or (t > 0 and mkt["above"][knobs.get("ma", 240)][t - 1])
            if want_in and cash > 1e-9 and OK[t, ex_] and np.isfinite(O[t, ex_]):
                park += cash / (O[t, ex_] * (1 + slip) * (1 + BUY_C))
                cash = 0.0
            elif not want_in and park > 0 and OK[t, ex_] and np.isfinite(O[t, ex_]):
                cash += park * O[t, ex_] * (1 - slip) * (1 - SELL_ETF)
                trades.append((t, t, 0.0, ex_))
                park = 0.0
            equity[t] = cash + park * CF[t, ex_]
            expos[t] = 1.0 if park > 0 else 0.0
            if t + 1 >= T:
                equity[t] -= park * CF[t, ex_] * (SELL_ETF + slip)
            continue
        if pend_buy:
            eq_open = cash + sum(a for _, a in unsettled) + park * (O[t, e50] if np.isfinite(O[t, e50]) else CF[t - 1, e50]) + sum(
                p[0] * (O[t, k] if np.isfinite(O[t, k]) else CF[t - 1, k]) for k, p in pos.items())
            for k, cap_n in pend_buy:
                if len(pos) >= cap_n:
                    skipped.append((t - 1, k, "slots"))
                    continue
                if k in pos or not OK[t, k] or not np.isfinite(O[t, k]) or O[t, k] >= CF[t - 1, k] * 1.095:
                    skipped.append((t - 1, k, "gap"))
                    continue
                c0 = CF[t - 1, k]
                sf = stop if stopmode == "fixed" else float(np.clip(3 * ATR[t - 1, k] / c0, 0.06, 0.12)) \
                    if np.isfinite(ATR[t - 1, k]) else stop
                w = 1 / n if size == "equal" else min(1 / n, 0.005 / sf)
                want = eq_open * w
                if cash < want and park > 0 and np.isfinite(O[t, e50]):
                    p50 = O[t, e50] * (1 - slip)
                    sh50 = min(park, (want - cash) / (p50 * (1 - SELL_ETF)))
                    park -= sh50
                    cash += sh50 * p50 * (1 - SELL_ETF)
                liq = TV[t - 1, k] * 0.05 / (capital * max(eq_open, 1e-9)) if np.isfinite(TV[t - 1, k]) else 0.0
                amt = min(want, cash, liq * eq_open)          # ≤ 5% of the stock's 20-day traded value
                if amt <= want * 0.5:
                    skipped.append((t - 1, k, "cash" if min(want, cash) <= want * 0.5 else "liquidity"))
                    continue
                px = O[t, k] * (1 + slip)
                pos[k] = [amt / (px * (1 + BUY_C)), px, t, sf, px]
                cash -= amt
            pend_buy = []
        if idle == "0050" and cash > 1e-6 and np.isfinite(O[t, e50]) and OK[t, e50]:
            park += cash / (O[t, e50] * (1 + slip) * (1 + BUY_C))
            cash = 0.0
        stock_val = sum(p[0] * (CF[t, k] if np.isfinite(CF[t, k]) else p[1]) for k, p in pos.items())
        equity[t] = cash + sum(a for _, a in unsettled) + park * CF[t, e50] + stock_val
        expos[t] = stock_val / equity[t] if equity[t] > 0 else 0
        if t + 1 >= T:
            equity[t] -= stock_val * (SELL_C + slip) + park * CF[t, e50] * (SELL_ETF + slip)
            break
        fast_now = bear_mode == "ma240" and mkt["fall240"][t]
        for k, p in pos.items():
            sh, ep, ed, sf, best = p
            if k in pend_exit or not np.isfinite(C[t, k]):
                continue
            p[4] = best = max(best, C[t, k])
            out = SELL[t, k] or C[t, k] <= ep * (1 - sf)
            if fast_now and t > ed:
                out = out or FAST[t, k] or C[t, k] <= ep * 0.93
            if cap500 and t - ed + 1 >= rc.CAP:            # 500-session cap
                out = True
            if trail and best >= ep * 1.20 and np.isfinite(ATR[t, k]) and C[t, k] < best - 4 * ATR[t, k]:
                out = True
            if out:
                pend_exit.add(k)
        cap_n = n
        if expo == "regime":
            cap_n = {1: n, 0: max(1, n // 2), -1: max(1, n // 4)}[int(mkt["reg60"][t])]
        elif expo == "ma240":
            cap_n = 0 if mkt["fall240"][t] else (max(1, n // 2) if mkt["below240"][t] else n)
        free = cap_n - (len(pos) - len(pend_exit))
        if free > 0:
            cand = [k for k in np.flatnonzero(BUY[t]) if k not in pos and k != e50]
            if prio == "random":                          # robustness: random order among same-day signals
                rng = np.random.default_rng(knobs.get("seed", 0) * 100003 + t)
                rng.shuffle(cand)
            elif prio == "rank" and RANK is not None:
                cand.sort(key=lambda k: -(RANK[t, k] if np.isfinite(RANK[t, k]) else -9))
            else:
                cand.sort(key=lambda k: -(RS[t, k] if np.isfinite(RS[t, k]) else -9))
            pend_buy = [(k, cap_n) for k in cand[:free]]
            skipped += [(t, k, "slots") for k in cand[free:]]
        else:
            skipped += [(t, k, "slots") for k in np.flatnonzero(BUY[t]) if k not in pos and k != e50]
    simulate.last_skipped = skipped
    simulate.last_positions_log = None
    return equity, trades, expos


def diagnose(eq, tr, skipped, P, cal, start_i, tv_rank):
    """Where the worst drawdown happened, the size profile of what was held, and how skipped signals did."""
    s = pd.Series(eq[start_i:], index=range(start_i, len(eq)))
    dd = s / s.cummax() - 1
    trough = int(dd.idxmin())
    peak = int(s.loc[:trough].idxmax())
    out = {"mdd_from": cal[peak], "mdd_to": cal[trough]}
    O, CF = P["open"], P["close_ff"]
    T = len(cal)
    rk = [tv_rank[ed, k] for ed, xd, r, k in tr if k != P["e50"] and np.isfinite(tv_rank[ed, k])]
    out["entry_size_pct"] = float(np.median(rk)) if rk else float("nan")      # 1.0 = biggest traded value
    out["held_2330"] = sum(1 for _, _, _, k in tr if k == P.get("c2330"))
    f60 = lambda t, k: CF[min(t + 61, T - 1), k] / O[t + 1, k] - 1 if t + 1 < T and np.isfinite(O[t + 1, k]) else np.nan  # noqa: E731
    sk = {}
    for t, k, why in skipped:
        if t >= start_i:
            sk.setdefault(why, []).append(f60(t, k))
    out["skipped"] = {w: (len(v), float(np.nanmean(v))) for w, v in sk.items()}
    bought = [f60(ed - 1, k) for ed, xd, r, k in tr if k != P["e50"]]
    out["bought_f60"] = float(np.nanmean(bought)) if bought else float("nan")
    return out


def metrics(eq, cal, start_i):
    s = pd.Series(eq[start_i:], index=pd.to_datetime(cal[start_i:]))
    assert s.notna().all(), "equity has gaps"
    # day before the start = the 1.0 invested (so first-day costs count)
    s = pd.concat([pd.Series([1.0], index=[s.index[0] - pd.Timedelta(days=1)]), s])
    yrs = (s.index[-1] - s.index[0]).days / 365.25
    cagr = (s.iloc[-1] / s.iloc[0]) ** (1 / yrs) - 1
    mdd = (s / s.cummax() - 1).min()
    yearly = s.groupby(s.index.year).apply(lambda x: x.iloc[-1] / x.iloc[0] - 1)
    # year returns chained from the previous year's last close
    ye = s.groupby(s.index.year).last()
    yr = ye.pct_change()
    yr.iloc[0] = ye.iloc[0] / s.iloc[0] - 1
    mo = s.resample("ME").last().pct_change().dropna()
    return {"total": s.iloc[-1] / s.iloc[0] - 1, "cagr": cagr, "mdd": mdd, "worst_year": yr.min(),
            "yearly": yr.to_dict(), "lose_months": (mo < 0).mean()}


def main(argv=None):
    a = parse_args(argv)
    t0 = time.time()
    prep = prepare(a, a.universe, a.years)
    cal = [str(x) for x in prep["calendar"]]
    pos_of = {d: i for i, d in enumerate(cal)}
    adj, inu = prep["adjusted"], prep["inu"]
    inu_set = set(zip(inu["stock_id"], inu["date"].astype(str)))
    ETF_ONLY = {"0050", "00631L"}
    sids = [s for s in adj if s not in ETF_ONLY] + [s for s in ("0050", "00631L") if s in adj]
    K, T = len(sids), len(cal)
    O, C, OK = np.full((T, K), np.nan), np.full((T, K), np.nan), np.zeros((T, K), bool)
    H, Lw = np.full((T, K), np.nan), np.full((T, K), np.nan)
    TV20 = np.full((T, K), np.nan)
    jobs = []
    for k, sid in enumerate(sids):
        g = adj[sid].reset_index(drop=True)
        cp = np.array([pos_of[str(d)] for d in g["date"]])
        O[cp, k], C[cp, k] = g["aopen"].to_numpy(float), g["aclose"].to_numpy(float)
        H[cp, k], Lw[cp, k] = g["ahigh"].to_numpy(float), g["alow"].to_numpy(float)
        tvr = (g["close"] * g["volume"]).astype(float)              # NT$ traded value (raw prices)
        TV20[cp, k] = tvr.rolling(20, min_periods=5).mean().shift(0).to_numpy()
        OK[cp, k] = (g["fill_ok"] if "fill_ok" in g else pd.Series(True, index=g.index)).to_numpy(bool)
        if sid not in ETF_ONLY:
            jobs.append((sid, g, cp, T))
    sig = {k: np.zeros((T, K), bool) for k in ("buy", "sell", "fast")}
    sig["rs20"] = np.full((T, K), np.nan)
    sig["atr"] = np.full((T, K), np.nan)
    col = {s: i for i, s in enumerate(sids)}
    with ProcessPoolExecutor(a.workers) as ex:
        for sid, out in ex.map(stock_job, jobs, chunksize=4):
            k = col[sid]
            for name, arr in out.items():
                sig[name][:, k] = arr
    # buy only while the stock is in the point-in-time universe on the signal day
    member = np.zeros((T, K), bool)
    for sid, d in inu_set:
        if sid in col and d in pos_of:
            member[pos_of[d], col[sid]] = True
    sig["buy"] &= member
    # TAIEX below a falling 240-day MA (for the bear switch)
    from tuning import data as dm
    tx = dm.load_market_index()["TAIEX"].sort_index()
    tx.index = tx.index.astype(str)
    tx = tx.reindex(cal).ffill()
    m240 = tx.rolling(240, min_periods=200).mean()
    m60 = tx.rolling(60, min_periods=60).mean()
    reg60 = np.where((tx > m60) & (m60 > m60.shift(5)), 1, np.where((tx < m60) & (m60 < m60.shift(5)), -1, 0))
    mkt = {"reg60": reg60, "below240": (tx < m240).to_numpy(), "above240": (tx > m240).to_numpy(),
           "above": {L_: (tx > tx.rolling(L_, min_periods=int(L_ * 0.8)).mean()).to_numpy() for L_ in (120, 200, 240, 280)},
           "fall240": ((tx < m240) & (m240 < m240.shift(20))).to_numpy()}
    # monthly momentum (12-1) + revenue score, re-ranked every 21 sessions, for the 'rank' priority
    from tuning.research_rank import EVERY, LOOK, scores_at, wide
    Pw = {"open": wide(adj, "aopen", cal), "close": wide(adj, "aclose", cal), "rev_yoy": wide(adj, "rev_yoy", cal),
          "pb": wide(adj, "pb", cal)}
    Pw["close_ff"] = Pw["close"].ffill()
    Mw = pd.DataFrame(member, index=range(T), columns=sids).reindex(columns=Pw["close"].columns).fillna(False)
    Mw.index = Pw["close"].index
    rank = np.full((T, K), np.nan)
    for t0_ in range(LOOK, T, EVERY):
        sc = scores_at(t0_, Pw, {t0_: Mw.iloc[t0_]})["combo"]
        for sid_, v in sc.dropna().items():
            if sid_ in col:
                rank[t0_:t0_ + EVERY, col[sid_]] = v
    sig["rank"] = rank
    tv_rank = pd.DataFrame(np.where(member, TV20, np.nan)).rank(axis=1, pct=True).to_numpy()
    P = {"open": O, "close": C, "high": H, "low": Lw, "tv20": TV20, "c2330": col.get("2330"), "col": col, "close_ff": pd.DataFrame(C).ffill().to_numpy(), "ok": OK,
         "e50": col["0050"]}
    start_i = next(i for i, d in enumerate(cal) if d >= a.start)
    log(f"[portfolio] {K} stocks, {T} days, signals ready in {time.time() - t0:.0f}s")
    variants = {k: VARIANTS[k] for k in a.variants.split(",") if k in VARIANTS}
    if a.extra:
        variants.update(json.loads(a.extra))
    e50 = col["0050"]
    b = P["close_ff"][:, e50].copy()
    bench = np.full(T, np.nan)
    sh0 = 1.0 / (O[start_i, e50] * 1.001 * (1 + BUY_C))           # 10 bp slippage + fee on entry
    bench[start_i:] = sh0 * b[start_i:]
    bench[-1] *= 1 - SELL_ETF - 0.001                               # liquidation cost at the end
    _bm = metrics(bench, cal, start_i)
    bm_cagr, bm_mdd = _bm["cagr"], _bm["mdd"]
    res = {}
    mc = {}
    for name, kn in variants.items():
        if kn.get("seeds"):                                         # Monte Carlo over the same-day order
            rows = []
            for sd in range(kn["seeds"]):
                k2 = dict(kn, prio="random", seed=sd)
                eq, tr, ex = simulate(k2, P, sig, mkt, cal, start_i)
                if k2.get("blend"):
                    eq = (1 - k2["blend"]) * bench + k2["blend"] * eq
                mm = metrics(eq, cal, start_i)
                rows.append((mm["cagr"], mm["mdd"]))
            r = np.array(rows)
            mc[name] = {"n": len(r), "cagr_p05": float(np.percentile(r[:, 0], 5)), "cagr_med": float(np.median(r[:, 0])),
                        "cagr_p95": float(np.percentile(r[:, 0], 95)), "mdd_med": float(np.median(r[:, 1])),
                        "mdd_p05": float(np.percentile(r[:, 1], 5)),
                        "beat_cagr": float((r[:, 0] > bm_cagr).mean()), "beat_both": float(((r[:, 0] > bm_cagr) & (r[:, 1] >= bm_mdd)).mean())}
            log(f"[portfolio] MC {name}: CAGR median {mc[name]['cagr_med']:+.1%} MDD median {mc[name]['mdd_med']:.1%}")
            continue
        blend = kn.get("blend")
        eq, tr, ex = simulate(kn, P, sig, mkt, cal, start_i)
        if blend:                                                   # (1 − blend) in 0050 B&H + blend in combo, no rebalancing
            eq = (1 - blend) * bench + blend * eq
            ex = ex * blend
        m = metrics(eq, cal, start_i)
        m["exposure"] = float(ex[start_i:].mean())
        m.update(diagnose(eq, tr, getattr(simulate, "last_skipped", []), P, cal, start_i, tv_rank))
        r = np.array([x[2] for x in tr]) if tr else np.array([0.0])
        m.update(trades=len(tr), win=float((r > 0).mean()), avg=float(r.mean()))
        res[name] = m
        log(f"[portfolio] {name}: CAGR {m['cagr']:+.1%} MDD {m['mdd']:.1%} trades {len(tr)}")
    bm = metrics(bench, cal, start_i)
    years = sorted(bm["yearly"])
    L = [f"# 組合拳：實際資金配置回測（{cal[start_i]}–{cal[-1]}）", "",
         "一個帳戶、最多 N 檔、每檔投入當時淨值的 1/N；訊號同時太多時先買 20 日相對強度最強的。"
         "收盤決定、隔天開盤成交，含手續費（6 折）與證交稅。閒置資金：cash = 現金不計息；0050 = 停在 0050。", "",
         "| 做法 | 總報酬 | 年化 | 最大跌幅 | 最差年度 | 虧損月份比例 | 平均持股比例 | 交易數 | 勝率 | 每筆平均 |",
         "|---|---|---|---|---|---|---|---|---|---|",
         f"| **0050 買進持有** | {bm['total']:+.0%} | {bm['cagr']:+.1%} | {bm['mdd']:.0%} | {bm['worst_year']:+.0%} | "
         f"{bm['lose_months']:.0%} | 100% | – | – | – |"]
    for name, m in res.items():
        L.append(f"| {name} | {m['total']:+.0%} | {m['cagr']:+.1%} | {m['mdd']:.0%} | {m['worst_year']:+.0%} | "
                 f"{m['lose_months']:.0%} | {m['exposure']:.0%} | {m['trades']:,} | {m['win']:.0%} | {m['avg']:+.1%} |")
    if mc:
        L += ["", "## 隨機重複模擬（同一天訊號太多時，隨機決定買哪幾檔；每個做法跑多次）", "",
              f"0050：年化 {bm_cagr:+.1%}、最大跌幅 {bm_mdd:.0%}。", "",
              "| 做法 | 次數 | 年化（5%／中位數／95%） | 最大跌幅（中位數／最差 5%） | 年化贏 0050 的機率 | 年化贏且跌幅不更深的機率 |",
              "|---|---|---|---|---|---|"]
        for name, m in mc.items():
            L.append(f"| {name} | {m['n']} | {m['cagr_p05']:+.1%}／{m['cagr_med']:+.1%}／{m['cagr_p95']:+.1%} | "
                     f"{m['mdd_med']:.0%}／{m['mdd_p05']:.0%} | {m['beat_cagr']:.0%} | {m['beat_both']:.0%} |")
    L += ["", "## 診斷", "", "| 做法 | 最大跌幅期間 | 買進股票的成交值排名中位數(1=最大) | 買進者 60 日後報酬 | 被跳過的訊號（原因：筆數／60 日後報酬） |",
          "|---|---|---|---|---|"]
    for name, m in res.items():
        sk = "；".join(f"{w}: {n:,}／{v:+.1%}" for w, (n, v) in m.get("skipped", {}).items()) or "–"
        L.append(f"| {name} | {m['mdd_from']}～{m['mdd_to']} | {m['entry_size_pct']:.2f} | {m['bought_f60']:+.1%} | {sk} |")
    L += ["", "## 每年報酬", "", "| 做法 | " + " | ".join(str(y) for y in years) + " |",
          "|---|" + "---|" * len(years),
          "| 0050 | " + " | ".join(f"{bm['yearly'][y]:+.0%}" for y in years) + " |"]
    for name, m in res.items():
        L.append(f"| {name} | " + " | ".join(f"{m['yearly'].get(y, float('nan')):+.0%}" for y in years) + " |")
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    with open(a.report.replace(".md", ".json"), "w", encoding="utf-8") as fh:
        json.dump({"benchmark": bm, "variants": res}, fh, ensure_ascii=False, default=float, indent=1)
    log(f"[portfolio] report → {a.report} in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
