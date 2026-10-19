"""research_entry.py — two research questions for the daily picks (read-only; writes a Markdown report).

A. Chasing: are market-wide (uniform) BUY signals worse when the price is already far above MA20?
   Extension = (close - MA20) / ATR on the signal day. Compared:
     now      enter next open (the live rule)
     pull_k   limit order at close - k*ATR, valid for the next W sessions (fill = min(open, limit)); skipped if
              never filled. Exit at the open h+1 sessions after the fill day (same holding length).
     hybrid   'now' when extension <= X, else the pull-back limit order.
B. Value dips: stocks that fell a lot but look cheap on their own history — forward excess return at 20 / 60
   sessions, for several filters (cheap PE/PB, drawdown, revenue YoY, dividend yield, trend).

Excess = net return (fees + tax) minus the equal-weight mean of the in-universe stocks over the same window;
t-stats clustered by 20-session signal-date blocks. The last 126 sessions are reported separately (holdout).

  python backend/tuning/research_entry.py --source parquet --universe 500 --years 8 --adjust-source exchange
"""
import argparse
import json
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
from tuning.evaluate import Costs, LIMIT_SKIP  # noqa: E402
from tuning.run_tuning import add_common_args, prepare  # noqa: E402

HOLDOUT = 126
EXT_BINS = [-np.inf, 1, 2, 3, 4, np.inf]
EXT_LABELS = ["<1", "1-2", "2-3", "3-4", ">4"]


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--universe", type=int, default=500)
    ap.add_argument("--years", type=float, default=8.0)
    ap.add_argument("--params", default=os.path.join(OUT_DIR, "stock_params.json"))
    ap.add_argument("--window", type=int, default=5, help="pull-back limit order validity (sessions)")
    ap.add_argument("--report", default=os.path.join(OUT_DIR, "research_entry.md"))
    add_common_args(ap)
    ap.set_defaults(no_holdings=True)
    return ap.parse_args(argv)


def uniform_signals(path):
    with open(path, encoding="utf-8") as f:
        pj = json.load(f)
    sig = (pj.get("uniform") or {}).get("signals") or []
    return [(s["signal"], int(s["h"])) for s in sig] or [("rs_lead", 20), ("rs_high", 20)]


def fwd_open_to_open(ao, ok, i, h):
    """Gross return entering at the open of bar i, exiting at the open of bar i+h (next fillable)."""
    n = len(ao)
    j = i + h
    while j < n and not ok[j]:
        j += 1
    if i >= n or j >= n or not ok[i]:
        return np.nan, None
    return ao[j] / ao[i] - 1, j


def stock_rows(sid, g, inu_dates, sigs, horizons, costs, window, etf):
    f = ind.features(g)
    buys = ind.buy_signals(f)
    n = len(g)
    ao, al, ac = (g[c].to_numpy(float) for c in ("aopen", "alow", "aclose"))
    ok = (g["fill_ok"] if "fill_ok" in g else pd.Series(True, index=g.index)).to_numpy(bool)
    dates = g["date"].to_numpy()
    inu = np.isin(dates, inu_dates) if inu_dates is not None else np.ones(n, bool)
    atr, ma20 = f["atr"].to_numpy(float), f["ma20"].to_numpy(float)
    ext = (ac - ma20) / atr
    # benchmark input: open(t+1) → open(t+1+h) gross for every in-universe row
    bench = []
    for h in horizons:
        gr = np.full(n, np.nan)
        for t in range(n - 1):
            if inu[t]:
                gr[t] = fwd_open_to_open(ao, ok, t + 1, h)[0]
        bench.append(pd.DataFrame({"date": dates, "h": h, "gross": gr}))
    # A. uniform-signal events
    a_rows = []
    for name, h in sigs:
        s = buys.get(name)
        if s is None:
            continue
        for t in np.flatnonzero(s.to_numpy()):
            if not inu[t] or t + 1 >= n or not np.isfinite(ext[t]) or not np.isfinite(atr[t]):
                continue
            if ok[t + 1] and ao[t + 1] >= ac[t] * (1 + LIMIT_SKIP):
                continue
            g_now, _ = fwd_open_to_open(ao, ok, t + 1, h)
            row = {"stock_id": sid, "signal": name, "h": h, "date": dates[t], "p": t, "ext": ext[t],
                   "now": g_now, "now_date": dates[t]}
            for k in (0.5, 1.0, 1.5):
                lim = ac[t] - k * atr[t]
                gr, bd = np.nan, None
                for j in range(t + 1, min(t + 1 + window, n)):
                    if ok[j] and al[j] <= lim:
                        px = min(ao[j], lim)
                        x, xi = None, j + h
                        while xi < n and not ok[xi]:
                            xi += 1
                        if xi < n:
                            gr = ao[xi] / px - 1
                            bd = dates[j - 1]          # bench window ≈ open(j) → open(j+h)
                        break
                row[f"pull{k}"], row[f"pull{k}_date"] = gr, bd
            a_rows.append(row)
    # B. value-dip events (condition edges)
    b_rows = []
    c = f["close"]
    dd = c / c.rolling(250, min_periods=120).max() - 1
    cheap = None
    if ind._has(f, "pe"):
        pe = f["pe"].where(f["pe"] > 0)
        cheap = pe < ind.val_q(pe, 0.2)
    if ind._has(f, "pb"):
        pb = f["pb"].where(f["pb"] > 0)
        cpb = pb < ind.val_q(pb, 0.2)
        cheap = cpb if cheap is None else (cheap | cpb)
    if cheap is not None:
        cheap = cheap.fillna(False)
        yoy = f["rev_yoy"].ffill() if "rev_yoy" in f else pd.Series(np.nan, index=f.index)
        dy = f["dy"] if "dy" in f else pd.Series(np.nan, index=f.index)
        not_bear = ~((c < f["ma60"]) & (f["ma60"] < f["ma60"].shift(5)))
        turn = (c > f["ma20"]) & (f["ma20"] > f["ma20"].shift(5))
        base = cheap & (dd <= -0.25)
        variants = {
            "B1 便宜+跌深25%": base,
            "B2 +營收年增≥0": base & (yoy >= 0),
            "B3 +殖利率≥4%": base & (dy >= 4),
            "B4 +營收≥0+非空頭": base & (yoy >= 0) & not_bear,
            "B5 +營收≥0+站上上揚月線": base & (yoy >= 0) & turn,
            "B0 只看便宜(不論跌幅)": cheap,
        }
        for vname, cond in variants.items():
            evt = ind.edge(cond).to_numpy()
            for h in (20, 60):
                for t in ev_mod.thin(np.flatnonzero(evt & inu), h):
                    if t + 1 >= n:
                        continue
                    gr, _ = fwd_open_to_open(ao, ok, t + 1, h)
                    if np.isfinite(gr):
                        b_rows.append({"stock_id": sid, "variant": vname, "h": h, "date": dates[t], "p": t,
                                       "gross": gr})
    return a_rows, b_rows, pd.concat(bench, ignore_index=True)


def stats(x, p):
    x = np.asarray(x, float)
    m = np.isfinite(x)
    x, p = x[m], np.asarray(p)[m]
    if len(x) < 5:
        return {"n": len(x), "mean": np.nan, "win": np.nan, "t": 0.0}
    t, _ = ev_mod.t_cluster(x, p // ev_mod.CLUSTER_DAYS)
    return {"n": len(x), "mean": x.mean(), "win": (x > 0).mean(), "t": t}


def fmt(s):
    if not s["n"] or not np.isfinite(s["mean"]):
        return f"{s['n']} | – | – | –"
    return f"{s['n']} | {100 * s['mean']:+.2f}% | {100 * s['win']:.0f}% | {s['t']:+.1f}"


def main(argv=None):
    a = parse_args(argv)
    t0 = time.time()
    sigs = uniform_signals(a.params)
    horizons = sorted({h for _, h in sigs} | {20, 60})
    prep = prepare(a, a.universe, a.years)
    cal = prep["calendar"]
    pos = {d: i for i, d in enumerate(cal)}
    inu = prep["inu"]
    inu_by = dict(tuple(inu.groupby("stock_id")["date"])) if inu is not None else {}
    costs = Costs(discount=a.fee_discount if hasattr(a, "fee_discount") else 0.6)
    A, B, BEN = [], [], []
    for sid, g in prep["adjusted"].items():
        d = inu_by.get(sid)
        if inu is not None and d is None:
            continue
        r = stock_rows(sid, g.reset_index(drop=True), None if d is None else d.to_numpy(), sigs, horizons, costs,
                       a.window, prep["meta"][sid]["etf"])
        A += r[0]
        B += r[1]
        BEN.append(r[2])
    bench = pd.concat(BEN).groupby(["date", "h"])["gross"].mean()
    log(f"[research] {len(A):,} uniform events, {len(B):,} value-dip events in {time.time() - t0:.0f}s")
    hold_from = cal[-HOLDOUT]
    L = [f"# 進場研究報告（{cal[0]}–{cal[-1]}，{len(prep['adjusted'])} 檔，近 {HOLDOUT} 日另列）", "",
         "超額報酬 = 扣手續費與證交稅後的報酬 − 同期間全市場（股票池）平均報酬。t 值以 20 日區塊分群；"
         "|t| ≥ 2 才算明顯。", ""]

    def xs(df, col, dcol, h):
        b = bench.reindex(pd.MultiIndex.from_arrays([df[dcol], h]))
        return costs.net(df[col].astype(float), False).to_numpy() - b.to_numpy()

    # ── A ──
    ad = pd.DataFrame(A)
    if len(ad):
        ad["bin"] = pd.cut(ad["ext"], EXT_BINS, labels=EXT_LABELS)
        ad["x_now"] = xs(ad, "now", "now_date", ad["h"])
        for k in (0.5, 1.0, 1.5):
            ad[f"x_pull{k}"] = xs(ad, f"pull{k}", f"pull{k}_date", ad["h"])
        ad["hold"] = ad["date"] >= hold_from
        L += ["## A. 追高問題：訊號當天離月線越遠，之後表現是否越差？", "",
              f"訊號：{', '.join(f'{s}（持有 {h} 日）' for s, h in sigs)}。離月線距離 = (收盤 − 月線) ÷ ATR"
              "（ATR ≈ 每日平均波動）。", "",
              "| 離月線（ATR 倍數） | 筆數 | 立即買：平均超額 | 勝率 | t | 近半年 筆數 | 平均超額 | 勝率 | t |",
              "|---|---|---|---|---|---|---|---|---|"]
        for b in EXT_LABELS + ["全部"]:
            sub = ad if b == "全部" else ad[ad["bin"] == b]
            pre, ho = sub[~sub["hold"]], sub[sub["hold"]]
            L.append(f"| {b} | {fmt(stats(pre['x_now'], pre['p']))} | {fmt(stats(ho['x_now'], ho['p']))} |")
        L += ["", f"### 等拉回再買（掛限價 = 訊號日收盤 − k×ATR，{a.window} 個交易日內有效，沒成交就放棄）", "",
              "| 離月線 | 方式 | 成交率 | 成交筆數 | 平均超額 | 勝率 | t | 同批立即買的平均超額 |",
              "|---|---|---|---|---|---|---|---|"]
        pre = ad[~ad["hold"]]
        for b in ["<1", "1-2", "2-3", ">3", "全部"]:
            sub = pre if b == "全部" else (pre[pre["bin"].isin(["3-4", ">4"])] if b == ">3" else pre[pre["bin"] == b])
            for k in (0.5, 1.0, 1.5):
                filled = sub[np.isfinite(sub[f"x_pull{k}"])]
                s = stats(filled[f"x_pull{k}"], filled["p"])
                same = np.nanmean(filled["x_now"]) if len(filled) else np.nan
                fr = len(filled) / len(sub) if len(sub) else np.nan
                L.append(f"| {b} | 拉回 {k} ATR | {100 * fr:.0f}% | {fmt(s)} | {100 * same:+.2f}% |")
        # hybrid: immediate if ext <= X else pull-back k=1.0; unfilled → no trade (counted as 0 excess per signal)
        L += ["", "### 混合規則（離月線 ≤ X 立即買，> X 改掛拉回 1 ATR 限價；沒成交 = 不買）", "",
              "每個訊號的平均超額（沒成交的算 0），與「全部立即買」比較：", "",
              "| X | 期間 | 每訊號平均超額 | 全部立即買 | 差 |", "|---|---|---|---|---|"]
        for X in (2, 3, 4):
            for lab, sub in (("回測期", ad[~ad["hold"]]), ("近半年", ad[ad["hold"]])):
                ok_now = np.isfinite(sub["x_now"])
                sub = sub[ok_now]
                hyb = np.where(sub["ext"] <= X, sub["x_now"], np.nan_to_num(sub["x_pull1.0"], nan=0.0))
                base = sub["x_now"].mean()
                L.append(f"| {X} | {lab} | {100 * np.mean(hyb):+.3f}% | {100 * base:+.3f}% | "
                         f"{100 * (np.mean(hyb) - base):+.3f}% |")
    # ── B ──
    bd = pd.DataFrame(B)
    if len(bd):
        bd["x"] = xs(bd, "gross", "date", bd["h"])
        bd["hold"] = bd["date"] >= hold_from
        L += ["", "## B. 跌深但便宜的股票（價值低接）", "",
              "便宜 = 本益比或淨值比落在自身近 3 年最低 20%；跌深 = 距 250 日高點跌 ≥ 25%。條件第一天視為訊號，"
              "同一檔同一持有期至少間隔持有天數。", "",
              "| 條件 | 持有 | 筆數 | 平均超額 | 勝率 | t | 近半年 筆數 | 平均超額 | 勝率 | t |",
              "|---|---|---|---|---|---|---|---|---|---|"]
        for (v, h), sub in bd.groupby(["variant", "h"], sort=True):
            pre, ho = sub[~sub["hold"]], sub[sub["hold"]]
            L.append(f"| {v} | {h} 日 | {fmt(stats(pre['x'], pre['p']))} | {fmt(stats(ho['x'], ho['p']))} |")
        L += ["", "### 依年份（持有 60 日，回測期）", "", "| 條件 | " + " | ".join(sorted(bd["date"].str[:4].unique())) + " |",
              "|---|" + "---|" * bd["date"].str[:4].nunique()]
        years = sorted(bd["date"].str[:4].unique())
        for v, sub in bd[bd["h"] == 60].groupby("variant"):
            cells = []
            for y in years:
                s = sub[sub["date"].str[:4] == y]["x"]
                cells.append(f"{100 * s.mean():+.1f}% ({len(s)})" if len(s) >= 5 else "–")
            L.append(f"| {v} | " + " | ".join(cells) + " |")
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log(f"[research] report → {a.report} in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
