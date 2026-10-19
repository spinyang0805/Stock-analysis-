"""research_style.py — per-stock trading STYLE instead of per-stock signal pairs (read-only research).

User request (2026-10-19): keep looking for per-stock buy / sell rules that beat the market, but in a different
way. research_s2s showed that choosing the best (buy signal, sell signal) pair per stock out of hundreds
overfits (walk-forward -0.43% per trade vs the market). Here each stock may only choose among FIVE classic
styles with fixed textbook parameters (few choices → little room to overfit):

  trend      close crosses above a rising MA60 → sell when close < MA60
  breakout   close > 55-day high → sell when close < 20-day low (turtle / Donchian)
  meanrev    RSI(2) < 10 while close > MA200 → sell when close > MA5 (max 10 days, 2.5 ATR stop)
  rs         relative strength vs the market crosses above its 60-day average → sell when it crosses below
  pullback   uptrend (close > MA200, MA20 > MA60) and the low touches MA20 but the close holds → sell < MA60

Variants: each style for every stock (reference); per-stock choice (best style on the stock's own last 2
years, else no trade); one market-wide style chosen point-in-time; and the same with a ranking filter (entries
only while the stock is in the monthly "momentum + revenue growth" top 100, see research_rank).
Walk-forward like research_s2s: train 504 sessions, test 63; signal at close → next open; one position per
stock; fees + tax; excess = net − equal-weight in-universe open-to-open return over the same days.

  python backend/tuning/research_style.py --source parquet --universe 500 --years 8 --adjust-source exchange
"""
import argparse
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

from tuning import evaluate as ev_mod  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.research_rank import EVERY, LOOK, scores_at, wide  # noqa: E402
from tuning.run_tuning import add_common_args, prepare  # noqa: E402

GAP_SKIP = 0.095
TRAIN, TEST = 504, 63
MIN_TRAIN_TRADES = 3
RANK_TOP = 100
STYLES = {  # name: (cap sessions, stop ATR multiple or None)
    "trend": (250, None), "breakout": (250, None), "meanrev": (10, 2.5), "rs": (250, None), "pullback": (120, None)}
NAMES = {"trend": "趨勢（站上上升的季線，跌破季線賣）", "breakout": "突破（創 55 日新高買，跌破 20 日低點賣）",
         "meanrev": "超跌反彈（長線多頭中 RSI2 < 10 買，站回 5 日線賣）",
         "rs": "相對強度（強過大盤的趨勢轉強買，轉弱賣）", "pullback": "多頭回檔（多頭排列中回測月線守住買，跌破季線賣）"}


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--universe", type=int, default=500)
    ap.add_argument("--years", type=float, default=8.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--report", default=os.path.join(OUT_DIR, "research_style.md"))
    add_common_args(ap)
    ap.set_defaults(no_holdings=True)
    return ap.parse_args(argv)


def rsi(c, n):
    d = np.diff(c, prepend=np.nan)
    up = pd.Series(np.where(d > 0, d, 0.0)).ewm(alpha=1 / n, adjust=False).mean()
    dn = pd.Series(np.where(d < 0, -d, 0.0)).ewm(alpha=1 / n, adjust=False).mean()
    return (100 - 100 / (1 + up / dn.replace(0, np.nan))).fillna(100).to_numpy()


def style_masks(g, bidx):
    """(buy mask, sell mask) per style on the stock's rows; bidx = market index aligned to the rows."""
    c, h, lo = (g[k].to_numpy(float) for k in ("aclose", "ahigh", "alow"))
    s = pd.Series(c)
    ma = {n: s.rolling(n, min_periods=n).mean().to_numpy() for n in (5, 20, 60, 200)}
    prev = lambda a, k=1: np.r_[[np.nan] * k, a[:-k]]                                  # noqa: E731
    hi55 = pd.Series(h).rolling(55, min_periods=55).max().shift(1).to_numpy()
    lo20 = pd.Series(lo).rolling(20, min_periods=20).min().shift(1).to_numpy()
    rs_ = c / bidx
    rs_ma = pd.Series(rs_).rolling(60, min_periods=60).mean().to_numpy()
    r2 = rsi(c, 2)
    with np.errstate(invalid="ignore"):
        up200 = c > ma[200]
        out = {
            "trend": ((c > ma[60]) & (prev(c) <= prev(ma[60])) & (ma[60] > prev(ma[60], 5)), c < ma[60]),
            "breakout": (c > hi55, c < lo20),
            "meanrev": ((r2 < 10) & up200, c > ma[5]),
            "rs": ((rs_ > rs_ma) & (prev(rs_) <= prev(rs_ma)) & up200, rs_ < rs_ma),
            "pullback": (up200 & (ma[20] > ma[60]) & (lo <= ma[20]) & (c > ma[20]) & (prev(c) > prev(ma[20])),
                         c < ma[60]),
        }
    return {k: (np.nan_to_num(b, nan=0).astype(bool), np.nan_to_num(x, nan=0).astype(bool)) for k, (b, x) in out.items()}


def trades(buy, sell, ao, ac, ok, atr, cap, stop, entry_ok):
    """One position at a time. Rows (signal_i, entry_i, exit_i, gross, held, open)."""
    n = len(ao)
    b_idx, s_idx = np.flatnonzero(buy & entry_ok), np.flatnonzero(sell)
    rows, k = [], 0
    while k < len(b_idx):
        t = b_idx[k]
        e = t + 1
        if e >= n:
            break
        if not ok[e] or (np.isfinite(ac[t]) and ao[e] >= ac[t] * (1 + GAP_SKIP)):
            k += 1
            continue
        entry = ao[e]
        j = np.searchsorted(s_idx, e)
        dec = min(s_idx[j] if j < len(s_idx) else n, e + cap - 1)
        if stop and np.isfinite(atr[t]):
            hi = min(dec, n - 1)
            seg = ac[e:hi + 1]
            trail = np.maximum.accumulate(np.where(np.isfinite(seg), seg, -np.inf)) - stop * atr[e:hi + 1]
            lvl = np.maximum(entry - stop * atr[t], np.where(np.isfinite(trail), trail, -np.inf))
            hit = np.flatnonzero(seg <= lvl)
            if len(hit):
                dec = min(dec, e + hit[0])
        x = dec + 1
        while x < n and not ok[x]:
            x += 1
        if x >= n:
            rows.append((t, e, n - 1, ac[n - 1] / entry - 1, n - e, True))
            break
        rows.append((t, e, x, ao[x] / entry - 1, dec - e + 1, False))
        k = np.searchsorted(b_idx, x)
    return rows


def stock_job(a):
    sid, g, inu_mask, rank_mask, bench_idx, etf, cp = a
    ao, ac = g["aopen"].to_numpy(float), g["aclose"].to_numpy(float)
    ok = (g["fill_ok"] if "fill_ok" in g else pd.Series(True, index=g.index)).to_numpy(bool)
    tr = pd.concat([g["ahigh"] - g["alow"], (g["ahigh"] - g["aclose"].shift()).abs(),
                    (g["alow"] - g["aclose"].shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / 14, adjust=False).mean().to_numpy()
    costs, nn, out = Costs(), len(cp), {}
    for name, (buy, sell) in style_masks(g, bench_idx).items():
        cap, stop = STYLES[name]
        for filt, mask in (("all", inu_mask), ("rank", inu_mask & rank_mask)):
            rows = trades(buy, sell, ao, ac, ok, atr, cap, stop, mask)
            if not rows:
                continue
            r = np.array(rows, dtype=float)
            e, x = r[:, 1].astype(int), r[:, 2].astype(int)
            net = costs.net(r[:, 3], etf)
            out[(name, filt)] = {"e": cp[e], "x": cp[np.minimum(x, nn - 1)], "net": net,
                                 "xs": net - (bench_idx[x] / bench_idx[e] - 1), "held": r[:, 4],
                                 "open": r[:, 5].astype(bool)}
    return sid, out


def in_window(c, lo, hi, closed=True):
    m = (c["e"] >= lo) & (c["e"] < hi)
    return m & (c["x"] < hi) & ~c["open"] if closed else m


def main(argv=None):
    a = parse_args(argv)
    t0 = time.time()
    prep = prepare(a, a.universe, a.years)
    cal = list(prep["calendar"])
    pos = {d: i for i, d in enumerate(cal)}
    adj, inu = prep["adjusted"], prep["inu"]
    inu_set = set(zip(inu["stock_id"], inu["date"]))
    # equal-weight in-universe open-to-open index (as research_s2s)
    rets = []
    for sid, g in adj.items():
        o, d = g["aopen"].to_numpy(float), g["date"].to_numpy()
        keep = np.array([(sid, x) in inu_set for x in d])
        rets.append(pd.DataFrame({"date": d[keep], "r": np.r_[o[1:] / o[:-1] - 1, np.nan][keep]}))
    br = pd.concat(rets).groupby("date")["r"].mean().reindex(cal).fillna(0.0).clip(-0.2, 0.2)
    bidx_cal = np.r_[1.0, np.cumprod(1 + br.to_numpy())[:-1]]
    # monthly "momentum + revenue" top-RANK_TOP membership (score at close t, valid until the next rebalance)
    P = {"open": wide(adj, "aopen", cal), "close": wide(adj, "aclose", cal),
         "rev_yoy": wide(adj, "rev_yoy", cal), "pb": wide(adj, "pb", cal)}
    P["close_ff"] = P["close"].ffill()
    cols = P["close"].columns
    M = (inu.assign(v=True).pivot_table(index="date", columns="stock_id", values="v", aggfunc="any")
         .reindex(index=cal, columns=cols).fillna(False).astype(bool))
    in_rank = pd.DataFrame(False, index=range(len(cal)), columns=cols)
    for t in range(LOOK, len(cal), EVERY):
        sc = scores_at(t, P, {t: M.iloc[t]})["combo"].dropna()
        if len(sc) >= 2 * RANK_TOP:
            in_rank.loc[t:t + EVERY - 1, sc.sort_values(ascending=False).index[:RANK_TOP]] = True
    first_rank = next((t for t in range(len(cal)) if in_rank.iloc[t].any()), len(cal))
    log(f"[style] benchmark + ranking ready (ranking from {cal[min(first_rank, len(cal) - 1)]}) in {time.time() - t0:.0f}s")
    jobs = []
    for sid, g in adj.items():
        g = g.reset_index(drop=True)
        d = g["date"].to_numpy()
        m = np.array([(sid, x) in inu_set for x in d])
        if not m.any():
            continue
        cp = np.array([pos[x] for x in d])
        rk = in_rank[sid].to_numpy()[cp] if sid in in_rank else np.zeros(len(d), bool)
        jobs.append((sid, g, m, rk, bidx_cal[cp], prep["meta"][sid]["etf"], cp))
    with ProcessPoolExecutor(a.workers) as ex:
        cands = dict(ex.map(stock_job, jobs, chunksize=4))
    log(f"[style] {len(cands)} stocks simulated in {time.time() - t0:.0f}s")

    n_cal = len(cal)
    folds, s = [], max(TRAIN, first_rank + 63)          # same start for every variant (ranking must exist)
    while s < n_cal:
        folds.append((s - TRAIN, s, min(s + TEST, n_cal)))
        s += TEST
    if not folds:
        sys.exit("[style] not enough history after the ranking starts (needs ≥ 2 × RANK_TOP ranked names)")
    res = {}

    def take(key, sid, c, lo, hi, te):
        m = (c["e"] >= hi) & (c["e"] < te)
        res.setdefault(key, []).extend({"p": int(c["e"][i]), "net": float(c["net"][i]), "xs": float(c["xs"][i]),
                                        "held": float(c["held"][i])} for i in np.flatnonzero(m))

    picked = {}
    for lo, hi, te in folds:
        for filt in ("all", "rank"):
            # market-wide style chosen on the pooled train window
            tot = {}
            for sid, cs in cands.items():
                for (name, f_), c in cs.items():
                    if f_ == filt:
                        m = in_window(c, lo, hi)
                        tot[name] = tot.get(name, 0.0) + c["xs"][m].sum()
            best_pool = max(tot, key=tot.get) if tot and max(tot.values()) > 0 else None
            for sid, cs in cands.items():
                for name in STYLES:
                    c = cs.get((name, filt))
                    if c is not None:
                        take(("fixed", name, filt), sid, c, lo, hi, te)
                if best_pool and (best_pool, filt) in cs:
                    take(("pooled", None, filt), sid, cs[(best_pool, filt)], lo, hi, te)
                best, best_s = None, 0.0
                for name in STYLES:
                    c = cs.get((name, filt))
                    if c is None:
                        continue
                    m = in_window(c, lo, hi)
                    x = c["xs"][m]
                    if len(x) < MIN_TRAIN_TRADES or x.mean() <= 0:
                        continue
                    sc = x.mean() / (x.std(ddof=1) + 1e-9) * np.sqrt(len(x))   # most consistent, not most trades
                    if sc > best_s:
                        best, best_s = name, sc
                if best:
                    take(("per", None, filt), sid, cs[(best, filt)], lo, hi, te)
                    picked[(best, filt)] = picked.get((best, filt), 0) + 1

    def row(tr):
        if len(tr) < 5:
            return f"{len(tr)} | – | – | – | – | – | – |"
        df = pd.DataFrame(tr)
        t, _ = ev_mod.t_cluster(df["xs"].to_numpy(), (df["p"] // ev_mod.CLUSTER_DAYS).to_numpy())
        per100 = df["xs"].sum() / df["held"].sum() * 100
        return (f"{len(df)} | {100 * df['xs'].mean():+.2f}% | {100 * (df['xs'] > 0).mean():.0f}% | {t:+.1f} | "
                f"{df['held'].mean():.0f} | {100 * per100:+.2f}% | {100 * df['net'].min():.0f}% |")

    first, last = cal[folds[0][1]], cal[-1]
    L = [f"# 每檔股票選「交易風格」回測（交易期間 {first}–{last}）", "",
         "不再從幾百組買賣訊號裡替每檔挑最好的（之前證實會過度配適），改成每檔只能從 5 種經典風格挑一種，參數都是教科書固定值。"
         "每檔用「過去 2 年」自己的成績挑最穩定贏大盤（t 值最高）的風格，再交易接下來 3 個月（沒看過的資料），每 3 個月重挑。"
         "收盤出訊號 → 隔天開盤買賣；扣手續費（6 折）與證交稅；超額 = 報酬 − 同期間股票池平均。", "",
         "「排名過濾」= 只在該股屬於當月「強勢股＋營收成長」前 100 名時才進場（排名法在前一份研究表現最好）。", ""]
    hdr = ["| 方式 | 進場範圍 | 筆數 | 平均超額 | 勝率(贏大盤) | t | 平均持有日 | 每 100 持有日超額 | 最差一筆 |",
           "|---|---|---|---|---|---|---|---|---|"]
    fl = {"all": "全部股票", "rank": "排名過濾"}
    L += ["## 每檔自己挑風格 vs 全市場同一風格", ""] + hdr
    for filt in ("all", "rank"):
        L.append(f"| 每檔自己挑（過去 2 年最好的風格） | {fl[filt]} | {row(res.get(('per', None, filt), []))}")
        L.append(f"| 全市場用同一風格（過去 2 年整體最好的） | {fl[filt]} | {row(res.get(('pooled', None, filt), []))}")
    L += ["", "## 每種風格套用到所有股票（參考，沒有挑選）", ""] + hdr
    for name in STYLES:
        for filt in ("all", "rank"):
            L.append(f"| {NAMES[name]} | {fl[filt]} | {row(res.get(('fixed', name, filt), []))}")
    L += ["", "## 每檔最常選到的風格（全部股票）", "", "| 風格 | 次數（股票 × 季） |", "|---|---|"]
    for name in STYLES:
        L.append(f"| {NAMES[name]} | {picked.get((name, 'all'), 0)} |")
    L += ["", "t 值以 20 日區塊分群，|t| ≥ 2 才算明顯（共比了 14 種組合，剛好 2 左右要保守看）。"
          "「每 100 持有日超額」× 2.5 約等於全年都在場內時一年多賺多少。", ""]
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log(f"[style] report → {a.report} in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
