"""research_s2s.py — "signal to signal" per-stock rules (read-only research; writes a Markdown report).

User request (2026-10-19): buy when the stock's buy signal fires, sell when its sell signal fires — no fixed
holding period (safety cap: 1 year = 250 sessions). Each stock picks its own (buy signal, sell signal) pair
on a walk-forward train window; the pair then trades the next test window it has never seen.

Variants (all side by side):
  per_stop     per-stock buy + sell pair, plus a stop / trailing stop (2.5 ATR)
  per_nostop   per-stock buy + sell pair, sell signal only
  uni_stop     market-wide buy signals (params 'uniform'), per-stock sell signal, plus stop
  uni_nostop   market-wide buy signals, per-stock sell signal only
Execution: signal at close t → open t+1 (skip if open ≥ +9.5% or no valid open); exit decided at a close →
next valid open. One position at a time per stock. Fees + tax (Costs). Excess = net − equal-weight in-universe
open-to-open return over the same days. Walk-forward: train 504 sessions, test 63, the last 126 sessions are a
holdout (selected once on the 504 sessions before it). Trades still open at the end are marked at the last close.

  python backend/tuning/research_s2s.py --source parquet --universe 500 --years 8 --adjust-source exchange
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

from tuning import evaluate as ev_mod  # noqa: E402
from tuning import indicators as ind  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.run_tuning import add_common_args, prepare  # noqa: E402

CAP = 250            # 1 year of sessions
GAP_SKIP = 0.095
ATR_MULT = 2.5
TRAIN, TEST, HOLDOUT = 504, 63, 126
MIN_TRAIN_TRADES = 4
VARIANTS = ("per_stop", "per_nostop", "uni_stop", "uni_nostop")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--universe", type=int, default=500)
    ap.add_argument("--years", type=float, default=8.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--params", default=os.path.join(OUT_DIR, "stock_params.json"))
    ap.add_argument("--report", default=os.path.join(OUT_DIR, "research_s2s.md"))
    add_common_args(ap)
    ap.set_defaults(no_holdings=True)
    return ap.parse_args(argv)


def next_true(mask):
    """nxt[i] = smallest j >= i with mask[j] (len(mask) if none)."""
    n = len(mask)
    idx = np.flatnonzero(mask)
    return idx, np.searchsorted(idx, np.arange(n + 1))


def trades_for(buy_idx, sell_mask, ao, ac, ok, atr, use_stop, entry_ok):
    """Sequential one-position trades. Returns arrays (sig_i, entry_i, exit_i, gross, held, capped, open)."""
    n = len(ao)
    s_idx, s_ptr = next_true(sell_mask)
    rows = []
    t_min = 0
    k = np.searchsorted(buy_idx, t_min)
    while k < len(buy_idx):
        t = buy_idx[k]
        e = t + 1
        if e >= n:
            break
        if not entry_ok[t] or not ok[e] or (np.isfinite(ac[t]) and ao[e] >= ac[t] * (1 + GAP_SKIP)):
            k += 1
            continue
        entry = ao[e]
        # decided close: first sell signal on/after e, the cap, or the stop
        j = s_ptr[e]
        dec = s_idx[j] if j < len(s_idx) else n
        capped = False
        if e + CAP - 1 < dec:
            dec, capped = e + CAP - 1, True
        if use_stop and np.isfinite(atr[t]):
            hi = min(dec, n - 1)
            seg = ac[e:hi + 1]
            if len(seg):
                trail = np.maximum.accumulate(np.where(np.isfinite(seg), seg, -np.inf)) - ATR_MULT * atr[e:hi + 1]
                stop = np.maximum(entry - ATR_MULT * atr[t], np.where(np.isfinite(trail), trail, -np.inf))
                hit = np.flatnonzero(seg <= stop)
                if len(hit) and e + hit[0] < dec:
                    dec, capped = e + hit[0], False
        x = dec + 1
        while x < n and not ok[x]:
            x += 1
        if x >= n:                                   # still open → mark at the last close
            rows.append((t, e, n - 1, ac[n - 1] / entry - 1, n - e, False, True))
            break
        rows.append((t, e, x, ao[x] / entry - 1, dec - e + 1, capped, False))
        k = np.searchsorted(buy_idx, x)              # next signal after the exit was executed
    return rows


def stock_job(a):
    """Builds every candidate's trades, then does the walk-forward choice per variant (in the worker)."""
    sid, g, inu_mask, uni_names, bench_idx, etf, cp, folds, hold = a
    cand, _ = build_candidates(g, inu_mask, uni_names, bench_idx, etf)
    nn = len(cp)
    for c in cand.values():                          # row positions → calendar positions
        c["e"], c["x"] = cp[c["e"]], cp[np.minimum(c["x"], nn - 1)]
    out = {v: ([], []) for v in VARIANTS}
    chosen = {v: {} for v in VARIANTS}
    for v in VARIANTS:
        kind, use_stop = ("per" if v.startswith("per") else "uni"), v.endswith("_stop")
        for which, (lo, hi, te) in [(0, f_) for f_ in folds] + [(1, hold)]:
            best = pick(cand, kind, use_stop, lo, hi)
            if best is None:
                continue
            c = cand[best]
            m = (c["e"] >= hi) & (c["e"] < te)
            if not m.any():
                continue
            chosen[v][best[1:3]] = chosen[v].get(best[1:3], 0) + int(m.sum())
            for i in np.flatnonzero(m):
                out[v][which].append({"stock_id": sid, "p": int(c["e"][i]), "net": float(c["net"][i]),
                                      "xs": float(c["xs"][i]), "held": float(c["held"][i]),
                                      "cap": bool(c["cap"][i]), "open": bool(c["open"][i])})
    return out, chosen


def build_candidates(g, inu_mask, uni_names, bench_idx, etf):
    f = ind.features(g)
    buys, sells = ind.buy_signals(f), ind.sell_signals(f)
    ao, ac = g["aopen"].to_numpy(float), g["aclose"].to_numpy(float)
    ok = (g["fill_ok"] if "fill_ok" in g else pd.Series(True, index=g.index)).to_numpy(bool)
    atr = f["atr"].to_numpy(float)
    n = len(g)
    costs = Costs()
    bi = bench_idx                                   # bench index aligned to g rows (open-to-open)
    uni = np.zeros(n, bool)
    for nm in uni_names:
        if nm in buys:
            uni |= buys[nm].to_numpy(bool)
    sell_masks = {k: v.to_numpy(bool) for k, v in sells.items()}
    buy_sets = {("per", k): np.flatnonzero(v.to_numpy(bool)) for k, v in buys.items()}
    buy_sets[("uni", "uniform")] = np.flatnonzero(uni)
    # every candidate's full trade sequence (excess computed once)
    cand = {}
    for (kind, bname), bidx in buy_sets.items():
        if not len(bidx):
            continue
        for sname, sm in sell_masks.items():
            for use_stop in (True, False):
                rows = trades_for(bidx, sm, ao, ac, ok, atr, use_stop, inu_mask)
                if not rows:
                    continue
                r = np.array(rows, dtype=float)
                e, x = r[:, 1].astype(int), r[:, 2].astype(int)
                # open trades are marked at the close; bench up to the last open is close enough for research
                bret = bi[x] / bi[e] - 1
                net = costs.net(r[:, 3], etf)
                cand[(kind, bname, sname, use_stop)] = {"t": r[:, 0].astype(int), "e": e, "x": x, "gross": r[:, 3],
                                                       "net": net, "xs": net - bret, "held": r[:, 4],
                                                       "cap": r[:, 5].astype(bool), "open": r[:, 6].astype(bool)}
    return cand, n


def pick(cand, kind, use_stop, lo, hi):
    """Best candidate on train window: entry in [lo, hi) and exit < hi; score = total excess (≥ MIN trades,
    mean > 0)."""
    best, best_s = None, 0.0
    for key, c in cand.items():
        if key[0] != kind or key[3] != use_stop:
            continue
        m = (c["e"] >= lo) & (c["e"] < hi) & (c["x"] < hi) & ~c["open"]
        if m.sum() < MIN_TRAIN_TRADES:
            continue
        s = c["xs"][m].sum()
        if s > best_s and c["xs"][m].mean() > 0:
            best, best_s = key, s
    return best


def main(argv=None):
    a = parse_args(argv)
    t0 = time.time()
    with open(a.params, encoding="utf-8") as f:
        pj = json.load(f)
    uni_names = [s["signal"] for s in ((pj.get("uniform") or {}).get("signals") or [])] or ["rs_lead", "rs_high"]
    prep = prepare(a, a.universe, a.years)
    cal = prep["calendar"]
    pos = {d: i for i, d in enumerate(cal)}
    inu = prep["inu"]
    inu_set = set(zip(inu["stock_id"], inu["date"])) if inu is not None else None
    # equal-weight in-universe open-to-open benchmark index on the calendar
    rets = []
    for sid, g in prep["adjusted"].items():
        ao = g["aopen"].to_numpy(float)
        d = g["date"].to_numpy()
        r = np.r_[ao[1:] / ao[:-1] - 1, np.nan]
        keep = np.array([(sid, x) in inu_set for x in d]) if inu_set is not None else np.ones(len(d), bool)
        rets.append(pd.DataFrame({"date": d[keep], "r": r[keep]}))
    br = pd.concat(rets).groupby("date")["r"].mean().reindex(cal).fillna(0.0).clip(-0.2, 0.2)
    bidx_cal = np.r_[1.0, np.cumprod(1 + br.to_numpy())[:-1]]       # value at the OPEN of each calendar day
    log(f"[s2s] benchmark built; {len(prep['adjusted'])} stocks; buy (uniform) = {uni_names}")
    # folds on the calendar (stock row positions are mapped through the calendar)
    n_cal = len(cal)
    end = n_cal - HOLDOUT
    folds, s = [], max(TRAIN, 252)
    while s < end:
        folds.append((s - TRAIN, s, min(s + TEST, end)))
        s += TEST
    hold = (end - TRAIN, end, n_cal)
    jobs = []
    for sid, g in prep["adjusted"].items():
        g = g.reset_index(drop=True)
        d = g["date"].to_numpy()
        m = np.array([(sid, x) in inu_set for x in d]) if inu_set is not None else np.ones(len(d), bool)
        if not m.any():
            continue
        cp = np.array([pos[x] for x in d])
        jobs.append((sid, g, m, uni_names, bidx_cal[cp], prep["meta"][sid]["etf"], cp, folds, hold))
    res = {v: [] for v in VARIANTS}
    hres = {v: [] for v in VARIANTS}
    chosen = {v: {} for v in VARIANTS}
    with ProcessPoolExecutor(a.workers) as ex:
        for out, ch in ex.map(stock_job, jobs, chunksize=4):
            for v in VARIANTS:
                res[v] += out[v][0]
                hres[v] += out[v][1]
                for k, c in ch[v].items():
                    chosen[v][k] = chosen[v].get(k, 0) + c
    log(f"[s2s] simulated in {time.time() - t0:.0f}s")

    def row(tr):
        if len(tr) < 5:
            return f"{len(tr)} | – | – | – | – | – | – | –"
        df = pd.DataFrame(tr)
        t, _ = ev_mod.t_cluster(df["xs"].to_numpy(), (df["p"] // ev_mod.CLUSTER_DAYS).to_numpy())
        per100 = df["xs"].sum() / df["held"].sum() * 100
        return (f"{len(df)} | {100 * df['xs'].mean():+.2f}% | {100 * (df['xs'] > 0).mean():.0f}% | {t:+.1f} | "
                f"{100 * df['net'].mean():+.2f}% | {df['held'].mean():.0f} | {100 * per100:+.2f}% | "
                f"{100 * df['net'].min():.0f}%")

    names = {"per_stop": "每檔各自買賣訊號＋停損", "per_nostop": "每檔各自買賣訊號（只看賣出訊號）",
             "uni_stop": "全市場買進訊號＋每檔賣出訊號＋停損", "uni_nostop": "全市場買進訊號＋每檔賣出訊號（只看賣出訊號）"}
    L = [f"# 訊號到訊號回測（{cal[0]}–{cal[-1]}，最長持有 {CAP} 日）", "",
         "買進訊號出現 → 隔天開盤買；賣出訊號（或停損）出現 → 隔天開盤賣。每檔的買賣訊號組合只用「過去 2 年」挑選，"
         "再拿去交易接下來 3 個月（模型沒看過）。超額 = 扣成本報酬 − 同期間股票池平均。", "",
         "| 方式 | 期間 | 筆數 | 平均超額 | 勝率(贏大盤) | t | 平均報酬 | 平均持有日 | 每 100 持有日超額 | 最差一筆 |",
         "|---|---|---|---|---|---|---|---|---|---|"]
    for v in VARIANTS:
        L.append(f"| {names[v]} | 走動驗證 | {row(res[v])} |")
        L.append(f"| {names[v]} | 最後半年 | {row(hres[v])} |")
    L += ["", "t 值以 20 日區塊分群，|t| ≥ 2 才算明顯。「每 100 持有日超額」把持有時間長短考慮進去。", ""]
    for v in ("per_stop", "per_nostop"):
        top = sorted(chosen[v].items(), key=lambda kv: -kv[1])[:12]
        L += [f"### {names[v]}：最常被選中的組合", "", "| 買進訊號 | 賣出訊號 | 交易筆數 |", "|---|---|---|"]
        L += [f"| {ind.BUY_DESC.get(b, b)} | {ind.SELL_DESC.get(s_, s_)} | {k} |" for (b, s_), k in top]
        L.append("")
    caps = {v: (np.mean([t["cap"] for t in res[v]]) if res[v] else 0) for v in VARIANTS}
    L.append("抱滿一年才賣的比例：" + "、".join(f"{names[v]} {100 * caps[v]:.0f}%" for v in VARIANTS))
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log(f"[s2s] report → {a.report} in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
