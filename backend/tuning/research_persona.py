"""research_persona.py — does every stock have its own "personality" (股性)? (read-only research)

User hypothesis (2026-10-19): each stock reacts to different signals (W bottom, institutional buying, RSI, KD,
touching MA20 / MA60 …), both for buying and for selling, so each stock should use its own signals.
Earlier studies picked each stock's best signals and lost out of sample. Two direct tests on ALL library
signals (indicators.buy_signals / sell_signals), fixed horizon events (non-overlapping 'stat' events,
in-universe, excess = return − equal-weight universe over the same days):

 1. Persistence. Split the history in two halves. For every (stock, signal) with enough events in both halves,
    take the stock-specific part of the result: cell mean − signal mean − stock mean + grand mean (so a stock
    that simply rose, or a signal that works everywhere, does not count as personality). If personality is
    real, a stock that responded unusually well to a signal in half A should also do so in half B:
    slope of B on A > 0. 95% interval by resampling stocks.
 2. Walk-forward choice. Every year, using only earlier data, choose 3 signals per stock with:
      pooled    the signal's average over all stocks (no personality)
      group     the average over stocks with similar traits (volatility tercile × trending / reverting)
      shrunk    pooled + n/(n+k) × (stock's own − pooled), k = 30
      own       the stock's own average only (pure personality)
    then measure the next year's events of exactly those signals on that stock.

  python backend/tuning/research_persona.py --source parquet --universe 500 --years 8 --adjust-source exchange
"""
import argparse
import os
import sys
import time

import numpy as np
import pandas as pd

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
if __package__ in (None, ""):
    __package__ = "tuning"

from tuning import evaluate as ev_mod  # noqa: E402
from tuning import indicators as ind  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.run_tuning import Cfg, add_common_args, build_events, prepare  # noqa: E402

HORIZONS = (5, 10, 20)
MIN_HALF = 5          # events per (stock, signal) in each half for the persistence test
MIN_OWN = 5           # events per (stock, signal) in training for 'own' / 'shrunk'
SHRINK_K = 30
PICK = 3
N_BOOT = 300


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--universe", type=int, default=500)
    ap.add_argument("--years", type=float, default=8.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--report", default=os.path.join(OUT_DIR, "research_persona.md"))
    add_common_args(ap)
    ap.set_defaults(no_holdings=True)
    return ap.parse_args(argv)


def interaction(df, col):
    """Per (stock, signal): n and the stock×signal part of the mean (double-centred)."""
    cell = df.groupby(["stock_id", "signal"])[col].agg(["mean", "size"])
    sig = df.groupby("signal")[col].mean()
    stk = df.groupby("stock_id")[col].mean()
    grand = df[col].mean()
    idx = cell.index
    cell["dev"] = (cell["mean"] - sig.reindex(idx.get_level_values(1)).to_numpy()
                   - stk.reindex(idx.get_level_values(0)).to_numpy() + grand)
    cell["stock_dev"] = stk.reindex(idx.get_level_values(0)).to_numpy() - grand
    return cell


def slope(a, b, w):
    a = a - np.average(a, weights=w)
    return float(np.sum(w * a * (b - np.average(b, weights=w))) / np.sum(w * a * a))


def persistence(ev, col, rng):
    mid = np.sort(ev["date"].unique())[len(ev["date"].unique()) // 2]
    A, B = interaction(ev[ev["date"] < mid], col), interaction(ev[ev["date"] >= mid], col)
    j = A.join(B, lsuffix="_a", rsuffix="_b", how="inner")
    j = j[(j["size_a"] >= MIN_HALF) & (j["size_b"] >= MIN_HALF)]
    if len(j) < 50:
        return None
    w = 1 / (1 / j["size_a"] + 1 / j["size_b"]).to_numpy()
    stocks = j.index.get_level_values(0).unique().to_numpy()
    by = {s: np.flatnonzero(j.index.get_level_values(0) == s) for s in stocks}
    da, db = j["dev_a"].to_numpy(), j["dev_b"].to_numpy()
    est = slope(da, db, w)
    boots = []
    for _ in range(N_BOOT):
        ii = np.concatenate([by[s] for s in rng.choice(stocks, len(stocks))])
        boots.append(slope(da[ii], db[ii], w[ii]))
    # stock main effect (does a stock that did well after ANY buy signal keep doing well?)
    sa = j.groupby(level=0)["stock_dev_a"].first().to_numpy()
    sb = j.groupby(level=0)["stock_dev_b"].first().to_numpy()
    return {"mid": mid, "pairs": len(j), "stocks": len(stocks), "slope": est,
            "lo": float(np.percentile(boots, 2.5)), "hi": float(np.percentile(boots, 97.5)),
            "corr": float(np.corrcoef(da, db)[0, 1]), "stock_corr": float(np.corrcoef(sa, sb)[0, 1])}


def traits(prep, lo_date, hi_date):
    """Volatility tercile × trending (weekly autocorrelation > 0) per stock, measured on [lo, hi)."""
    out = {}
    for sid, g in prep["adjusted"].items():
        c = g.loc[(g["date"] >= lo_date) & (g["date"] < hi_date), "aclose"].to_numpy(float)
        if len(c) < 120:
            continue
        r = np.diff(np.log(c))
        wk = np.diff(np.log(c[::5]))
        out[sid] = (np.nanstd(r), np.corrcoef(wk[:-1], wk[1:])[0, 1] if len(wk) > 10 else 0.0)
    if not out:
        return {}
    v = pd.Series({k: x[0] for k, x in out.items()})
    cut = v.quantile([1 / 3, 2 / 3]).to_numpy()
    return {k: f"{int(x[0] > cut[0]) + int(x[0] > cut[1])}{'T' if x[1] > 0 else 'R'}" for k, x in out.items()}


def walk_forward(ev, prep, col, sign):
    """sign=+1 buy (higher excess better), -1 sell (lower = better avoided). Returns {method: test events}."""
    dates = np.sort(ev["date"].unique())
    years = sorted({d[:4] for d in dates})
    res = {m: [] for m in ("pooled", "group", "shrunk", "own", "all")}
    for y in years[2:]:
        tr, te = ev[ev["date"] < y + "0101"], ev[(ev["date"] >= y + "0101") & (ev["date"] < str(int(y) + 1) + "0101")]
        # training events must have finished before the test year starts
        tr = tr[tr["exit_date"].astype(str) < y + "0101"]
        if tr.empty or te.empty:
            continue
        tg = traits(prep, dates[0], y + "0101")
        pooled = tr.groupby("signal")[col].mean() * sign
        tr = tr.assign(grp=tr["stock_id"].map(tg))
        grp = tr.groupby(["grp", "signal"])[col].agg(["mean", "size"])
        own = tr.groupby(["stock_id", "signal"])[col].agg(["mean", "size"])
        res["all"].append(te)
        for sid, tes in te.groupby("stock_id"):
            cand = {}
            cand["pooled"] = pooled
            if sid in tg and tg[sid] in grp.index.get_level_values(0):
                gg = grp.loc[tg[sid]]
                cand["group"] = (gg["mean"] * sign)[gg["size"] >= 30]
            if sid in own.index.get_level_values(0):
                oo = own.loc[sid]
                oo = oo[oo["size"] >= MIN_OWN]
                if len(oo):
                    p = pooled.reindex(oo.index).fillna(0)
                    cand["own"] = oo["mean"] * sign
                    cand["shrunk"] = p + oo["size"] / (oo["size"] + SHRINK_K) * (oo["mean"] * sign - p)
            for m, s in cand.items():
                s = s[s > 0].sort_values(ascending=False)
                if len(s):
                    res[m].append(tes[tes["signal"].isin(s.index[:PICK])])
    return {m: (pd.concat(v) if v else pd.DataFrame()) for m, v in res.items()}


def main(argv=None):
    a = parse_args(argv)
    t0 = time.time()
    prep = prepare(a, a.universe, a.years)
    cfg = Cfg()
    cfg.horizons = HORIZONS
    ev, _ = build_events(prep, cfg, Costs(), a.workers)
    ev = ev[ev["in_u"] & ev["stat"]].copy()
    ev["date"] = ev["date"].astype(str)
    ev["xs"] = ev["gross"] - ev["bench"]                       # market-relative, before costs
    log(f"[persona] {len(ev):,} events in {time.time() - t0:.0f}s")
    rng = np.random.default_rng(0)
    L = [f"# 每檔股票有沒有自己的「股性」？（{ev['date'].min()}–{ev['date'].max()}）", "",
         f"用系統全部訊號：向上 {len(ind.BUY_DESC)} 個、向下 {len(ind.SELL_DESC)} 個（W 底、法人買超、RSI、KD、碰月線、碰季線…）。"
         "每次訊號後固定看 5／10／20 個交易日，跟同期間股票池平均比（不重疊的訊號才算）。", ""]
    L += ["## 測試一：過去對某訊號特別有反應的股票，之後還是嗎？", "",
          "把資料切成前後兩半。只看「這檔 × 這個訊號」特有的部分（扣掉訊號本身好壞、扣掉這檔本身漲跌）。"
          "如果股性存在，前半特別有效的組合，後半也該比較有效：**延續比例 > 0**。"
          "延續比例 0.3 = 前半多賺 1%，後半平均還多賺 0.3%；0 = 完全不延續（只是運氣）。", "",
          "| 方向 | 看幾天 | 組合數 | 股票數 | 延續比例 | 95% 區間 | 相關係數 | 「這檔本身」前後相關 |",
          "|---|---|---|---|---|---|---|---|"]
    for side, nm in (("buy", "向上訊號"), ("sell", "向下訊號")):
        for h in HORIZONS:
            r = persistence(ev[(ev["side"] == side) & (ev["h"] == h)], "xs", rng)
            if r is None:
                L.append(f"| {nm} | {h} 日 | 不足 | | | | | |")
                continue
            L.append(f"| {nm} | {h} 日 | {r['pairs']:,} | {r['stocks']} | {r['slope']:+.2f} | "
                     f"{r['lo']:+.2f} ～ {r['hi']:+.2f} | {r['corr']:+.2f} | {r['stock_corr']:+.2f} |")
    L += ["", f"前後兩半的分界：{fmt(persistence_mid(ev))}。", ""]
    L += ["## 測試二：每年用過去資料替每檔挑 3 個訊號，看下一年", "",
          "| 挑法 | 說明 |", "|---|---|",
          "| 全市場平均 | 每檔都用全部股票平均最好的訊號（不管股性） |",
          "| 同類股票 | 用波動大小 × 「會延續／會反轉」相近的股票一起算 |",
          f"| 收縮 | 全市場平均 + 自己的差異 ×（筆數 ÷（筆數 + {SHRINK_K}）），樣本少就多信全市場 |",
          "| 只看自己 | 完全依這檔自己的歷史（純股性） |", "",
          "| 方向 | 看幾天 | 挑法 | 筆數 | 平均超額 | 勝率 | t |", "|---|---|---|---|---|---|---|"]
    mnames = {"all": "全部訊號（不挑）", "pooled": "全市場平均", "group": "同類股票", "shrunk": "收縮", "own": "只看自己"}
    for side, nm, sign in (("buy", "向上（買）", 1), ("sell", "向下（賣）", -1)):
        for h in (10, 20):
            sub = ev[(ev["side"] == side) & (ev["h"] == h)]
            wf = walk_forward(sub, prep, "xs", sign)
            for m in ("all", "pooled", "group", "shrunk", "own"):
                d = wf.get(m)
                if d is None or len(d) < 20:
                    L.append(f"| {nm} | {h} 日 | {mnames[m]} | {0 if d is None else len(d)} | – | – | – |")
                    continue
                p = pd.to_datetime(d["date"]).rank(method="dense").to_numpy() // ev_mod.CLUSTER_DAYS
                t, _ = ev_mod.t_cluster(d["xs"].to_numpy(), p)
                L.append(f"| {nm} | {h} 日 | {mnames[m]} | {len(d):,} | {100 * d['xs'].mean():+.2f}% | "
                         f"{100 * (d['xs'] > 0).mean():.0f}% | {t:+.1f} |")
    L += ["", "向上訊號：平均超額越高越好。向下訊號：平均超額越低（越負）代表賣掉後避開越多跌幅。",
          "測試二的超額未扣手續費（約 0.4% 來回），各挑法同樣未扣，可以互相比較。", ""]
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log(f"[persona] report → {a.report} in {time.time() - t0:.0f}s")


def persistence_mid(ev):
    u = np.sort(ev["date"].unique())
    return u[len(u) // 2]


def fmt(d):
    return f"{d[:4]}/{d[4:6]}/{d[6:]}"


if __name__ == "__main__":
    main()
