"""research_single.py — per stock, every buy × every exit, last half-year as an untouched test (research).

User request (2026-10-19): take one stock (e.g. 2449 京元電子), find when it should be bought and when sold
using all library signals (75 buy / 31 sell), pick the best rule on history only, then check it on the last
half-year. No look-ahead: the test period is never used to choose anything. Then the same for many stocks
(0050 / MSCI Taiwan / high-dividend ETF members) and for 0050 itself.

Candidates: every buy signal × (every sell signal | fixed hold 5 / 10 / 20 / 60 sessions) × (with / without a
2.5 ATR stop + trailing stop). Signal at close → next open (skip a gap-up ≥ 9.5%); one position at a time;
sell signal, stop or cap (250 sessions) → next open. Costs: fee 0.1425% × 0.6 per side + 0.3% tax (0.1% ETF).

Objective
  trade   (stocks) excess per trade = net − 0050 over the same days; rank by t = mean / sd × √n
  period  (0050 itself) whole-phase compounded rule return − buy-and-hold over the same phase (time out of
          the market counts: missing a rally is a loss)
Choosing (inside the training years only):
  explore   first 2/3 of the training years: candidates with enough trades (6, or 4 for 'period'), ranked
  confirm   last 1/3: the top 20 from explore are checked; the best positive one is the chosen rule.
            If none is positive → "don't trade this stock".
  test      the chosen rule, frozen, trades the last 126 sessions (no position carried in).

  python backend/tuning/research_single.py --source parquet --adjust-source exchange --stock 2449
  python backend/tuning/research_single.py --source parquet --adjust-source exchange --groups lists.json
"""
import argparse
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
if __package__ in (None, ""):
    __package__ = "tuning"

from tuning import indicators as ind  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.research_style import trades  # noqa: E402
from tuning.run_tuning import add_common_args, prepare  # noqa: E402

HOLDOUT = 126
HOLDS = (5, 10, 20, 60)
CAP = 250
STOP = 2.5
TOP_EXPLORE = 20
BENCH = "0050"
WARM = 250


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--stock", help="one stock (detailed report)")
    ap.add_argument("--groups", help="JSON {group name: [codes]} → summary report over all of them")
    ap.add_argument("--years", type=float, default=8.0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--test-start", help="YYYYMMDD: test window start (default: the last 126 sessions)")
    ap.add_argument("--test-end", help="YYYYMMDD: data after this date is cut off (default: none)")
    ap.add_argument("--report", default=None)
    add_common_args(ap)
    return ap.parse_args(argv)


def fd(d):
    return f"{d[:4]}/{d[4:6]}/{d[6:]}"


def stats(x):
    x = np.asarray(x, float)
    if not len(x):
        return {"n": 0, "mean": np.nan, "win": np.nan, "t": np.nan}
    sd = x.std(ddof=1) if len(x) > 1 else np.nan
    return {"n": len(x), "mean": x.mean(), "win": (x > 0).mean(),
            "t": x.mean() / sd * np.sqrt(len(x)) if sd and sd > 0 else np.nan}


def p(x, d=2):
    return "–" if x is None or not np.isfinite(x) else f"{100 * x:+.{d}f}%"


def label(c):
    bn, ex, st = c
    sell = f"抱 {ex} 日" if isinstance(ex, int) else ind.SELL_DESC.get(ex, ex)
    return f"{ind.BUY_DESC.get(bn, bn)} → {sell}{'＋停損' if st else ''}"


def study(job):
    """One stock. job = (sid, name, g, bench_open aligned to g, etf, test_start, test_end). Returns a result dict."""
    sid, name, g, b, etf, t_start, t_end = job
    g = g.reset_index(drop=True)
    if t_end:                                          # nothing after the test window exists for this run
        keep = (g["date"].astype(str) <= t_end).to_numpy()
        g, b = g[keep].reset_index(drop=True), b[keep]
    objective = "period" if sid == BENCH else "trade"
    f = ind.features(g)
    buys, sells = ind.buy_signals(f), ind.sell_signals(f)
    ao, ac = g["aopen"].to_numpy(float), g["aclose"].to_numpy(float)
    ok = (g["fill_ok"] if "fill_ok" in g else pd.Series(True, index=g.index)).to_numpy(bool)
    atr = f["atr"].to_numpy(float)
    dates = g["date"].astype(str).to_numpy()
    n = len(g)
    hs = int(np.searchsorted(dates, t_start)) if t_start else n - HOLDOUT
    if hs - WARM < 300 or hs >= n:
        return {"sid": sid, "name": name, "skip": f"歷史太短（{n} 日）"}
    ex_end = WARM + (hs - WARM) * 2 // 3
    costs = Costs()

    def run(c, lo, hi, carry_ok=False):
        bname, exit_, stop = c
        bm = buys[bname].to_numpy(bool).copy()
        bm[:lo] = False
        bm[hi:] = False
        if isinstance(exit_, int):
            sm, cap = np.zeros(n, bool), exit_
        else:
            sm, cap = sells[exit_].to_numpy(bool), CAP
        out = []
        for t, e, x, gross, held, op in trades(bm, sm, ao, ac, ok, atr, cap, STOP if stop else None, np.ones(n, bool)):
            t, e, x = int(t), int(e), int(x)
            if not carry_ok and (x >= hi or op):
                continue
            net = costs.net(gross, etf)
            out.append({"sig": dates[t], "in": dates[e], "out": dates[x], "pin": ao[e],
                        "pout": ac[x] if op else ao[x], "held": held, "net": net,
                        "xs": net - (b[x] / b[e] - 1), "open": bool(op)})
        return out

    def phase(tr, lo, hi):
        """Score dict for one phase. 'period' = compounded rule − buy-and-hold of this stock over [lo, hi)."""
        s = stats([r["xs"] for r in tr])
        comp = float(np.prod([1 + r["net"] for r in tr]) - 1) if tr else 0.0
        end = min(hi, n - 1)
        bh = ac[end] / ao[lo] - 1 if hi >= n else ao[end] / ao[lo] - 1
        s.update({"comp": comp, "bh": bh, "vs_bh": comp - bh})
        s["score"] = s["vs_bh"] if objective == "period" else s["t"]
        return s

    exits = list(sells) + list(HOLDS)
    cands = [(bn, ex, st) for bn in buys for ex in exits for st in (False, True)]
    need = 4 if objective == "period" else 6
    explored = []
    for c in cands:
        s = phase(run(c, WARM, ex_end), WARM, ex_end)
        if s["n"] >= need and np.isfinite(s["score"]):
            explored.append((s["score"], c, s))
    explored.sort(key=lambda z: -z[0])
    confirmed = [(c, s, phase(run(c, ex_end, hs), ex_end, hs)) for _, c, s in explored[:TOP_EXPLORE]]
    key = "vs_bh" if objective == "period" else "mean"
    ok_c = [z for z in confirmed if z[2]["n"] >= (2 if objective == "period" else 3) and z[2][key] > 0]
    chosen = max(ok_c, key=lambda z: z[2][key]) if ok_c else None
    res = {"sid": sid, "name": name, "objective": objective, "n_cands": len(cands), "n_explored": len(explored),
           "periods": (dates[WARM], dates[ex_end - 1], dates[ex_end], dates[hs - 1], dates[hs], dates[-1]),
           "test_bh": ac[-1] / ao[hs] - 1, "test_b0050": b[-1] / b[hs] - 1, "top": []}
    for c, s, cs in confirmed[:10]:
        res["top"].append((label(c), s, cs, phase(run(c, hs, n, carry_ok=True), hs, n)))
    if chosen:
        c, s, cs = chosen
        test = run(c, hs, n, carry_ok=True)
        res.update({"rule": label(c), "explore": s, "confirm": cs, "test": phase(test, hs, n), "trades": test})
    return res


def single_report(r):
    pe = r["periods"]
    L = [f"# {r['sid']} {r['name']}：個股買賣訊號回測（驗證期沒看過）", "",
         f"全部組合 {r['n_cands']:,} 種（向上訊號 × 向下訊號或固定持有天數 × 有無停損）。"
         + ("**0050 本身**：比的是「整段期間照規則進出」vs「一直抱著」，空手錯過上漲也算輸。" if r["objective"] == "period"
            else "超額 = 扣成本報酬 − 同期間 0050。"), "",
         "| 階段 | 期間 | 用途 |", "|---|---|---|",
         f"| 探索 | {fd(pe[0])}～{fd(pe[1])} | 所有組合排名 |",
         f"| 確認 | {fd(pe[2])}～{fd(pe[3])} | 探索前 {TOP_EXPLORE} 名再考一次，選最好且為正的 |",
         f"| **驗證（沒看過）** | {fd(pe[4])}～{fd(pe[5])} | 選定的規則凍結，直接交易 |", ""]
    if "rule" not in r:
        L += ["## 結果：沒有規則通過確認", "", "建議這檔不用訊號交易。", ""]
    else:
        L += ["## 選出的規則（只用驗證期以前的資料）", "", f"**{r['rule']}**", "",
              "| 階段 | 筆數 | 每筆平均超額 | 勝率 | 照規則累計 | 一直抱 | 規則 − 一直抱 |", "|---|---|---|---|---|---|---|"]
        for nm, k in (("探索", "explore"), ("確認", "confirm"), ("**驗證**", "test")):
            s = r[k]
            L.append(f"| {nm} | {s['n']} | {p(s['mean'])} | {p(s['win'], 0).lstrip('+')} | {p(s['comp'], 1)} | "
                     f"{p(s['bh'], 1)} | {p(s['vs_bh'], 1)} |")
        L += ["", f"驗證期一直抱 0050：{p(r['test_b0050'], 1)}。", "", "### 驗證期每一筆", "",
              "| 訊號日 | 買進日 | 買價 | 賣出日 | 賣價 | 持有日 | 報酬(扣成本) | 比 0050 |", "|---|---|---|---|---|---|---|---|"]
        for t in r["trades"]:
            L.append(f"| {fd(t['sig'])} | {fd(t['in'])} | {t['pin']:.2f} | {fd(t['out'])}{'（未賣，以收盤計）' if t['open'] else ''} | "
                     f"{t['pout']:.2f} | {int(t['held'])} | {p(t['net'], 1)} | {p(t['xs'], 1)} |")
        if not r["trades"]:
            L.append("| 驗證期沒有出現買進訊號 | | | | | | | |")
        L.append("")
    L += ["## 探索期前 10 名後來的表現（事後對照）", "",
          "| 名次 | 規則 | 探索 筆數/平均超額/規則−抱 | 確認 筆數/規則−抱 | 驗證 筆數/規則−抱 |", "|---|---|---|---|---|"]
    for i, (lb, s, cs, ts) in enumerate(r["top"], 1):
        L.append(f"| {i} | {lb} | {s['n']} / {p(s['mean'])} / {p(s['vs_bh'], 1)} | {cs['n']} / {p(cs['vs_bh'], 1)} | "
                 f"{ts['n']} / {p(ts['vs_bh'], 1)} |")
    L.append("")
    return L


def main(argv=None):
    a = parse_args(argv)
    groups = {"單一": [a.stock]} if a.stock else json.load(open(a.groups, encoding="utf-8"))
    codes = sorted({c for v in groups.values() for c in v} | {BENCH})
    a.no_holdings, a.holdings = False, BENCH
    prep = prepare(a, 0, a.years, codes=codes)
    adj = prep["adjusted"]
    bser = adj[BENCH].set_index("date")["aopen"]
    jobs = [(c, prep["meta"].get(c, {}).get("name", ""), adj[c],
             bser.reindex(adj[c]["date"]).ffill().bfill().to_numpy(float), bool(prep["meta"].get(c, {}).get("etf")), a.test_start, a.test_end)
            for c in codes if c in adj and (a.stock or c != BENCH or BENCH in groups.get("0050 本身", []))]
    log(f"[single] {len(jobs)} stocks")
    with ProcessPoolExecutor(a.workers) as ex:
        res = {r["sid"]: r for r in ex.map(study, jobs)}
    if a.stock:
        L = single_report(res[a.stock])
        rp = a.report or os.path.join(OUT_DIR, f"research_single_{a.stock}.md")
    else:
        L = summary_report(res, groups)
        rp = a.report or os.path.join(OUT_DIR, "research_single_groups.md")
        for sid in groups.get("0050 本身", []):
            L += ["", "---", ""] + single_report(res[sid])
    os.makedirs(os.path.dirname(rp), exist_ok=True)
    with open(rp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    log(f"[single] report → {rp}")


def summary_report(res, groups):
    rows = [r for r in res.values() if r["sid"] != BENCH and "skip" not in r]
    any_r = next(iter(rows))
    pe = any_r["periods"]
    L = ["# 每檔各自找最佳買賣訊號：多檔比較（驗證期沒看過）", "",
         f"每檔都試 {any_r['n_cands']:,} 種組合；探索 {fd(pe[0])}～{fd(pe[1])}、確認 {fd(pe[2])}～{fd(pe[3])} 選出規則，"
         f"**驗證 {fd(pe[4])}～{fd(pe[5])} 完全沒看過（之後的資料不存在於這次計算）**。超額 = 扣成本報酬 − 同期間 0050。", ""]

    def agg(rs):
        ch = [r for r in rs if "rule" in r]
        tr = [t for r in ch for t in r["trades"]]
        s = stats([t["xs"] for t in tr])
        return {"n": len(rs), "chosen": len(ch), "trades": len(tr), "mean": s["mean"], "win": s["win"],
                "beat_bh": np.mean([r["test"]["vs_bh"] > 0 for r in ch]) if ch else np.nan,
                "pos": np.mean([r["test"]["comp"] > 0 for r in ch]) if ch else np.nan,
                "conf": np.mean([r["confirm"]["mean"] for r in ch]) if ch else np.nan}

    L += ["## 各組總結", "",
          "| 名單 | 檔數 | 有選出規則 | 驗證期交易數 | 確認期每筆平均超額 | **驗證期每筆平均超額** | 驗證期勝率(贏0050) | 規則贏「一直抱該股」的比例 |",
          "|---|---|---|---|---|---|---|---|"]
    for gname, codes in list(groups.items()) + [("全部（不重複）", sorted(res))]:
        if gname == "0050 本身":
            continue
        rs = [res[c] for c in codes if c in res and c != BENCH and "skip" not in res[c]]
        if not rs:
            continue
        s = agg(rs)
        L.append(f"| {gname} | {s['n']} | {s['chosen']} | {s['trades']} | {p(s['conf'])} | **{p(s['mean'])}** | "
                 f"{p(s['win'], 0).lstrip('+')} | {p(s['beat_bh'], 0).lstrip('+')} |")
    L += ["", "「確認期」是選規則時看得到的成績；「驗證期」才是真正沒看過的。兩者落差越大，代表挑出來的越是運氣。", "",
          "## 每檔明細（依驗證期規則 − 一直抱排序）", "",
          "| 股票 | 選出的規則 | 確認 筆數/平均超額 | 驗證 筆數 | 驗證 照規則 | 驗證 一直抱 | 規則 − 抱 | 驗證 0050 |",
          "|---|---|---|---|---|---|---|---|"]
    ch = sorted([r for r in rows if "rule" in r], key=lambda r: -r["test"]["vs_bh"])
    for r in ch:
        t = r["test"]
        L.append(f"| {r['sid']} {r['name']} | {r['rule']} | {r['confirm']['n']} / {p(r['confirm']['mean'])} | {t['n']} | "
                 f"{p(t['comp'], 1)} | {p(t['bh'], 1)} | {p(t['vs_bh'], 1)} | {p(r['test_b0050'], 1)} |")
    none = [r for r in rows if "rule" not in r]
    if none:
        L += ["", "沒有規則通過確認（建議不用訊號交易）：" + "、".join(f"{r['sid']} {r['name']}" for r in none)]
    skip = [r for r in res.values() if "skip" in r]
    if skip:
        L += ["", "歷史太短略過：" + "、".join(f"{r['sid']} {r['name']}" for r in skip)]
    return L


if __name__ == "__main__":
    main()
