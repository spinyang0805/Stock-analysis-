"""research_etf_long.py — long-history test of simple 0050 / leveraged-0050 timing (research only).

Round-table question (2026-10-20): the only rule that beat 0050 on BOTH return and drawdown in 2020–2026 was
"50% 0050 + 50% 00631L while TAIEX is above its 240-day MA, else cash". 2020–2026 is mostly a bull market, so this
re-tests the same frozen rules on the longest history available:
  * real 0050 prices from 2003-06-30 (FinMind TaiwanStockPrice, raw) adjusted for cash dividends and splits
  * real 00631L from 2014-10-31; before that a SYNTHETIC 2× daily 0050 (2·r − 1.0%/yr fee/financing drag), so
    2008 is covered (marked as synthetic)
  * TAIEX from history/market_index.parquet (TWSE)
Signal: TAIEX close vs its N-day MA at day t → trade at the open of t+1. Costs: fee 0.1425% × 0.6 per side,
0.1% ETF tax on sells, 10 bp slippage per side. Idle money earns 0%.
Reports CAGR, max drawdown, worst year, yearly returns, and a 12-month block bootstrap of the yearly difference vs
0050 buy-and-hold (uncertainty from the market path, not from parameter choice).

  python backend/tuning/research_etf_long.py [--start 2004-01-02]
"""
import argparse
import json
import os
import sys

import numpy as np
import pandas as pd
import requests

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
if __package__ in (None, ""):
    __package__ = "tuning"

from tuning.data import HISTORY_DIR, OUT_DIR  # noqa: E402

FEE = 0.001425 * 0.6
CASH_RATE = 0.0            # yearly interest on idle cash (set by --cash-rate)
TAX_ETF = 0.001
SLIP = 0.001
L2_DRAG = 0.010            # synthetic 2× before 00631L: yearly fee + financing drag


def finmind(dataset, code, start="2003-01-01"):
    cache = os.path.join(HISTORY_DIR, "cache", f"finmind_{dataset}_{code}.json")
    if os.path.exists(cache):
        return json.load(open(cache, encoding="utf-8"))
    r = requests.get(f"https://api.finmindtrade.com/api/v4/data?dataset={dataset}&data_id={code}&start_date={start}",
                     timeout=120).json()
    data = r.get("data") or []
    os.makedirs(os.path.dirname(cache), exist_ok=True)
    json.dump(data, open(cache, "w", encoding="utf-8"))
    return data


def adjusted(code):
    """Daily open/close adjusted (back-adjusted to today's scale) for cash dividends and splits."""
    px = pd.DataFrame(finmind("TaiwanStockPrice", code))
    px = px[px["close"] > 0].copy()
    px["date"] = pd.to_datetime(px["date"])
    px = px.set_index("date").sort_index()[["open", "close"]].astype(float)
    factor = pd.Series(1.0, index=px.index)
    div = pd.DataFrame(finmind("TaiwanStockDividend", code))
    events = {}
    if len(div):
        for _, r in div.iterrows():
            d, cash = r.get("CashExDividendTradingDate"), float(r.get("CashEarningsDistribution") or 0)
            if d and cash > 0:
                events[pd.Timestamp(d)] = events.get(pd.Timestamp(d), 0.0) + cash
    prev = px["close"].shift(1)
    ratio = prev / px["close"]
    for d in px.index[1:]:
        f = 1.0
        if d in events and prev[d] > events[d]:
            f *= (prev[d] - events[d]) / prev[d]
        rr = ratio[d]
        if rr >= 1.8 and abs(rr - round(rr)) < 0.08 * round(rr) and d not in events:     # split (e.g. 1:4)
            f *= 1 / round(rr)
        if f != 1.0:
            factor[factor.index < d] *= f
    return px.mul(factor, axis=0)


def run(sig, legs, costs=True):
    """sig: bool Series (hold while True; decided at close t, traded at open t+1). legs: {name: (open, close)} with
    weights; returns the daily equity (start 1.0)."""
    dates = sig.index
    eq = np.empty(len(dates))
    cash, held, was_in = 1.0, {}, False
    for i, d in enumerate(dates):
        want = bool(sig.iloc[i - 1]) if i > 0 else False
        if want and not was_in:
            for n, (w, o, c) in legs.items():
                held[n] = cash * w / (o.iloc[i] * (1 + SLIP) * (1 + FEE))
            cash = 0.0
        elif not want and was_in:
            for n, (w, o, c) in legs.items():
                cash += held[n] * o.iloc[i] * (1 - SLIP) * (1 - FEE - TAX_ETF)
            held = {}
        was_in = want
        eq[i] = cash + sum(held[n] * legs[n][2].iloc[i] for n in held)
    if held:
        eq[-1] = cash + sum(held[n] * legs[n][2].iloc[-1] * (1 - SLIP) * (1 - FEE - TAX_ETF) for n in held)
    return pd.Series(eq, index=dates)


def run_weights(w50, wl2, o50, c50, ol2, cl2, band=0.0):
    """Target weights (Series, decided at close t, traded at open t+1) in 0050 and the 2× leg; the rest is cash at 0%.
    Rebalances whenever the target changes (or a leg drifts more than `band` from target when band > 0).
    Costs: fee each side, ETF tax on sells, slippage each side, charged on the traded amount."""
    n = len(w50)
    eq = np.empty(n)
    cash, s50, sl2 = 1.0, 0.0, 0.0
    prev_t = (0.0, 0.0)
    for i in range(n):
        if i > 0:
            t50, tl2 = float(w50.iloc[i - 1]), float(wl2.iloc[i - 1])
            v = cash + s50 * o50.iloc[i] + sl2 * ol2.iloc[i]
            drift = band > 0 and v > 0 and (abs(s50 * o50.iloc[i] / v - t50) > band or abs(sl2 * ol2.iloc[i] / v - tl2) > band)
            if (t50, tl2) != prev_t or drift:
                legs = {"50": [s50, o50.iloc[i], t50], "l2": [sl2, ol2.iloc[i], tl2]}
                for leg in legs.values():                          # sells first …
                    sh, px, tgt = leg
                    if tgt * v < sh * px:
                        q = (sh * px - tgt * v) / px
                        cash += q * px * (1 - SLIP) * (1 - FEE - TAX_ETF)
                        leg[0] = sh - q
                for leg in legs.values():                          # … then buys with the freed cash
                    sh, px, tgt = leg
                    if tgt * v > sh * px:
                        amt = min(tgt * v - sh * px, cash)
                        leg[0] = sh + amt / (px * (1 + SLIP) * (1 + FEE))
                        cash -= amt
                s50, sl2 = legs["50"][0], legs["l2"][0]
                prev_t = (t50, tl2)
        cash *= 1 + CASH_RATE / 252
        eq[i] = cash + s50 * c50.iloc[i] + sl2 * cl2.iloc[i]
    eq[-1] -= (s50 * c50.iloc[-1] + sl2 * cl2.iloc[-1]) * (SLIP + FEE + TAX_ETF)
    return pd.Series(eq, index=w50.index)


def stats(eq):
    s = pd.concat([pd.Series([1.0], index=[eq.index[0] - pd.Timedelta(days=1)]), eq])
    yrs = (s.index[-1] - s.index[0]).days / 365.25
    ye = s.groupby(s.index.year).last()
    yr = ye.pct_change()
    yr.iloc[0] = ye.iloc[0] - 1
    under = s < s.cummax()
    runs, cur = [], 0
    for u in under:
        cur = cur + 1 if u else 0
        runs.append(cur)
    out = {"cagr": s.iloc[-1] ** (1 / yrs) - 1, "mdd": (s / s.cummax() - 1).min(), "worst": yr.min(), "yearly": yr,
           "underwater_days": int(max(runs)) if runs else 0}
    for lab, lo, hi in (("p1", "2004-01-01", "2014-12-31"), ("p2", "2015-01-01", "2099-12-31")):
        sub = eq[(eq.index >= lo) & (eq.index <= hi)]
        if len(sub) > 250:
            base = eq[eq.index < lo].iloc[-1] if (eq.index < lo).any() else 1.0
            ss = pd.concat([pd.Series([base], index=[sub.index[0] - pd.Timedelta(days=1)]), sub])
            y_ = (ss.index[-1] - ss.index[0]).days / 365.25
            out[lab] = (ss.iloc[-1] / ss.iloc[0]) ** (1 / y_) - 1, (ss / ss.cummax() - 1).min()
    return out


def weight_rule(spec, e50, txd, tx, dates):
    """Target weights from a spec:
      {"type": "fixed", "w50": x, "wl2": y, "freq": "M"|"Q"|"Y"}   — rebalanced at each period's first day
      {"type": "voltarget", "target": 0.18, "lookback": 60, "max": 1.0, "leg": "50"|"l2", "freq": "M"}
         exposure = min(max, target / realised vol of 0050 over lookback days), set at each period start"""
    idx = pd.Series(range(len(dates)), index=dates)
    per = {"M": dates.to_period("M"), "Q": dates.to_period("Q"), "Y": dates.to_period("Y"), "W": dates.to_period("W")}.get(spec.get("freq", "M"), dates.to_period("M"))
    first = pd.Series(per).ne(pd.Series(per).shift(-1)).to_numpy()   # last trading day of each period (decide at its close)
    if spec["type"] == "fixed":
        w50 = pd.Series(spec.get("w50", 0.0), index=dates)
        wl2 = pd.Series(spec.get("wl2", 0.0), index=dates)
        if spec.get("freq") == "never":                                # buy once, never rebalance
            return w50, wl2
        # constant targets: force a rebalance at each period start by alternating a tiny epsilon
        eps = pd.Series(np.where(np.cumsum(first) % 2 == 0, 0.0, 1e-9), index=dates)
        return w50 + eps, wl2
    if spec["type"] == "voltarget":
        r = e50["close"].pct_change()
        vol = r.rolling(spec.get("lookback", 60)).std() * np.sqrt(252)
        expo = (spec["target"] / vol).clip(upper=spec.get("max", 1.0)).fillna(0)
        expo = expo.where(pd.Series(first, index=dates)).ffill().fillna(0).round(2)
        if spec.get("leg", "50") == "50":
            return expo, pd.Series(0.0, index=dates)
        if spec.get("leg") == "mix":                                   # E ≤ 1: 0050 only; 1 < E ≤ 1.5: (2−E) 0050 + (E−1) 2×
            return expo.where(expo <= 1, 2 - expo), (expo - 1).clip(lower=0)
        return pd.Series(0.0, index=dates), expo / 2.0                 # 2× leg: half the weight for the same exposure
    if spec["type"] == "ddtrigger":
        # Luna: 100% 0050; close ≤ cut·H → low weight; back to 100% when close ≥ back·H (H = max close since start)
        c = e50["close"]
        H = c.cummax()
        state, cur = [], 1.0
        for ci, hi in zip(c, H):
            if cur == 1.0 and ci <= spec.get("cut", 0.90) * hi:
                cur = spec.get("low", 0.40)
            elif cur < 1.0 and ci >= spec.get("back", 0.95) * hi:
                cur = 1.0
            state.append(cur)
        return pd.Series(state, index=dates), pd.Series(0.0, index=dates)
    if spec["type"] == "maband":
        # {"type": "maband", "ma": 200, "band": 0.02}: in when TAIEX > (1+band)·MA, out when < (1−band)·MA, else keep
        ma = tx.rolling(spec.get("ma", 200)).mean().reindex(dates)
        up, dn = txd > ma * (1 + spec.get("band", 0.02)), txd < ma * (1 - spec.get("band", 0.02))
        state, cur = [], 0.0
        for u, d_ in zip(up, dn):
            cur = 1.0 if u else (0.0 if d_ else cur)
            state.append(cur)
        return pd.Series(state, index=dates), pd.Series(0.0, index=dates)
    raise ValueError(spec)


def run_cppi(o50, c50, ol2, cl2, floor=0.70, mult=3.0, cap=1.5, e0=0.9):
    """Sol's rule B: daily E = min(cap, max(0, mult·(V − floor·H)/V)); V account, H high-water incl. 1.0;
    decided at the close, traded next open via run-style exposure mapping; once E hits 0 it stays in cash."""
    n = len(o50)
    eq = np.empty(n)
    cash, s50, sl2, H, E, dead = 1.0, 0.0, 0.0, 1.0, e0, False
    for i in range(n):
        if i > 0:
            v = cash + s50 * o50.iloc[i] + sl2 * ol2.iloc[i]
            t50, tl2 = (E, 0.0) if E <= 1 else (2 - E, E - 1)
            legs = [[s50, o50.iloc[i], t50], [sl2, ol2.iloc[i], tl2]]
            for leg in legs:
                if leg[2] * v < leg[0] * leg[1]:
                    q = (leg[0] * leg[1] - leg[2] * v) / leg[1]
                    cash += q * leg[1] * (1 - SLIP) * (1 - FEE - TAX_ETF)
                    leg[0] -= q
            for leg in legs:
                if leg[2] * v > leg[0] * leg[1] * 1.02 or (leg[0] == 0 and leg[2] > 0):   # 2% band avoids daily churn
                    amt = min(leg[2] * v - leg[0] * leg[1], cash)
                    if amt > 0:
                        leg[0] += amt / (leg[1] * (1 + SLIP) * (1 + FEE))
                        cash -= amt
            s50, sl2 = legs[0][0], legs[1][0]
        cash *= 1 + CASH_RATE / 252
        V = cash + s50 * c50.iloc[i] + sl2 * cl2.iloc[i]
        eq[i] = V
        H = max(H, V)
        if not dead:
            E = min(cap, max(0.0, mult * (V - floor * H) / V))
            dead = E <= 0
    eq[-1] -= (s50 * c50.iloc[-1] + sl2 * cl2.iloc[-1]) * (SLIP + FEE + TAX_ETF)
    return pd.Series(eq, index=o50.index)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2004-01-02")
    ap.add_argument("--report", default=os.path.join(OUT_DIR, "research_etf_long.md"))
    ap.add_argument("--weights", default=None, help="JSON {name: weight-rule spec} (see weight_rule)")
    ap.add_argument("--cash-rate", type=float, default=0.0, help="yearly interest on idle cash")
    ap.add_argument("--only-weights", action="store_true", help="skip the built-in MA timing table")
    a = ap.parse_args(argv)
    global CASH_RATE
    CASH_RATE = a.cash_rate
    e50 = adjusted("0050")
    l2r = adjusted("00631L")
    idx = pd.read_parquet(os.path.join(HISTORY_DIR, "market_index.parquet"))
    tx = idx[idx["index"] == "TAIEX"].assign(date=lambda d: pd.to_datetime(d["date"])).set_index("date")["close"]
    dates = e50.index[(e50.index >= a.start) & e50.index.isin(tx.index)]
    e50 = e50.reindex(dates)
    # 2× leg: real 00631L once listed, synthetic 2× daily 0050 before (chained so the series is continuous)
    r50 = e50["close"].pct_change().fillna(0)
    syn = (1 + 2 * r50 - L2_DRAG / 252).cumprod()
    first_real = l2r.index.min()
    l2c = syn.copy()
    real = l2r["close"].reindex(dates).ffill()
    m = dates >= first_real
    # scale the real 00631L so its first day continues the synthetic series (or starts at 1 when all real)
    anchor = syn[~m].iloc[-1] * (1 + 2 * r50[m].iloc[0]) if (~m).any() else 1.0
    scale = anchor / real[m].iloc[0]
    l2c[m] = real[m] * scale
    gap_open = (e50["open"] / e50["close"].shift(1)).fillna(1)        # synthetic open: 2× the overnight move
    l2o = l2c.shift(1) * (1 + 2 * (gap_open - 1))
    l2o[m] = l2r["open"].reindex(dates)[m] * scale
    l2o = l2o.fillna(l2c)
    txd = tx.reindex(dates).ffill()
    out, eqs = {}, {}
    always = pd.Series(True, index=dates)
    defs = {"0050 買進持有": (always, {"0050": 1.0})}
    for L in (200, 240, 280):
        above = txd > tx.rolling(L).mean().reindex(dates)
        defs[f"0050 站上 {L} 日線"] = (above, {"0050": 1.0})
        defs[f"50% 0050＋50% 2倍 站上 {L} 日線"] = (above, {"0050": 0.5, "L2": 0.5})
        defs[f"2倍 站上 {L} 日線"] = (above, {"L2": 1.0})
    defs["50% 0050＋50% 2倍 買進持有"] = (always, {"0050": 0.5, "L2": 0.5})
    defs["2倍 買進持有"] = (always, {"L2": 1.0})
    if a.only_weights:
        defs = {"0050 買進持有": defs["0050 買進持有"]}
    for name, (sig, w) in defs.items():
        legs = {}
        for k, wt in w.items():
            legs[k] = (wt, e50["open"], e50["close"]) if k == "0050" else (wt, l2o, l2c)
        eq = run(sig, legs)
        eqs[name] = eq
        out[name] = stats(eq)
    for name, spec in json.loads(a.weights or "{}").items():
        if spec["type"] == "cppi":
            eq = run_cppi(e50["open"], e50["close"], l2o, l2c, spec.get("floor", 0.7), spec.get("mult", 3.0),
                          spec.get("cap", 1.5), spec.get("e0", 0.9))
            eqs[name] = eq
            out[name] = stats(eq)
            continue
        w50, wl2 = weight_rule(spec, e50, txd, tx, dates)
        eq = run_weights(w50, wl2, e50["open"], e50["close"], l2o, l2c, spec.get("band", 0.0))
        eqs[name] = eq
        out[name] = stats(eq)
    # 12-month block bootstrap of yearly log-return differences vs 0050 B&H
    base = eqs["0050 買進持有"]
    rng = np.random.default_rng(7)
    boot = {}
    lb = np.log(base).diff().dropna()
    for name, eq in eqs.items():
        d = (np.log(eq).diff().dropna() - lb).to_numpy()
        n = len(d)
        blk = 252
        sims = []
        for _ in range(2000):
            starts = rng.integers(0, n - blk, size=n // blk + 1)
            sample = np.concatenate([d[s:s + blk] for s in starts])[:n]
            sims.append(sample.sum() / (n / 252))
        sims = np.array(sims)
        boot[name] = (float(np.percentile(sims, 5)), float(np.median(sims)), float((sims > 0).mean()))
    years = sorted(out["0050 買進持有"]["yearly"].index)
    L = [f"# 0050／2 倍槓桿擇時：長期回測（{dates[0].date()}～{dates[-1].date()}）", "",
         f"大盤收盤站上 N 日線就持有、跌破就賣出（隔天開盤成交），含手續費、0.1% 證交稅、每邊 0.1% 滑價。"
         f"2 倍槓桿：{first_real.date()} 起用真實 00631L，之前用 0050 每日漲跌 ×2、每年扣 1% 成本模擬（含 2008 年）。", "",
         "| 做法 | 年化 | 最大跌幅 | 最差年度 | 最長套牢（交易日） | 2004–14 年化／跌幅 | 2015– 年化／跌幅 | 每年比 0050 多賺（5%／中位數） | 贏 0050 的機率 |",
         "|---|---|---|---|---|---|---|---|---|"]
    fmt = lambda st, k: f"{st[k][0]:+.1%}／{st[k][1]:.0%}" if k in st else "–"      # noqa: E731
    for name, st in out.items():
        b = boot[name]
        L.append(f"| {name} | {st['cagr']:+.1%} | {st['mdd']:.0%} | {st['worst']:+.0%} | {st['underwater_days']} | "
                 f"{fmt(st, 'p1')} | {fmt(st, 'p2')} | {b[0]:+.1%}／{b[1]:+.1%} | {b[2]:.0%} |")
    L += ["", "## 每年報酬", "", "| 做法 | " + " | ".join(str(y) for y in years) + " |", "|---|" + "---|" * len(years)]
    for name, st in out.items():
        L.append(f"| {name} | " + " | ".join(f"{st['yearly'].get(y, np.nan):+.0%}" for y in years) + " |")
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    open(a.report, "w", encoding="utf-8").write("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
