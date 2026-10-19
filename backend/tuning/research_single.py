"""research_single.py — one stock, every buy × every exit, last half-year as an untouched test (research).

User request (2026-10-19): take one stock (e.g. 2449 京元電子), find when it should be bought and when sold
using all library signals (75 buy / 31 sell), pick the best rule on history only, then check it on the last
half-year. No look-ahead: the test period is never used to choose anything.

Candidates: every buy signal × (every sell signal | fixed hold 5 / 10 / 20 / 60 sessions) × (with / without a
2.5 ATR stop + trailing stop). Signal at close → next open (skip a gap-up ≥ 9.5%); one position at a time;
sell signal, stop or cap (250 sessions) → next open. Costs: fee 0.1425% × 0.6 per side + 0.3% tax.
Excess per trade = net − 0050 (open to open, dividends included) over the same days.

Choosing (inside the training years only):
  explore   first 2/3 of the training years: candidates with ≥ 6 trades, ranked by t = mean / sd × √n
  confirm   last 1/3: the top 20 from explore are checked; the best confirm mean excess (≥ 3 trades, > 0)
            is the chosen rule. If none is positive → the rule is "don't trade this stock".
  test      the chosen rule, frozen, trades the last 126 sessions (no position carried in).

  python backend/tuning/research_single.py --source parquet --adjust-source exchange --stock 2449
"""
import argparse
import os
import sys

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


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--stock", required=True)
    ap.add_argument("--years", type=float, default=8.0)
    ap.add_argument("--report", default=None)
    add_common_args(ap)
    return ap.parse_args(argv)


def fd(d):
    return f"{d[:4]}/{d[4:6]}/{d[6:]}"


def stats(x):
    x = np.asarray(x, float)
    if not len(x):
        return {"n": 0, "mean": np.nan, "win": np.nan, "t": np.nan, "sum": 0.0}
    sd = x.std(ddof=1) if len(x) > 1 else np.nan
    return {"n": len(x), "mean": x.mean(), "win": (x > 0).mean(), "sum": x.sum(),
            "t": x.mean() / sd * np.sqrt(len(x)) if sd and sd > 0 else np.nan}


def p(x, d=2):
    return "–" if x is None or not np.isfinite(x) else f"{100 * x:+.{d}f}%"


def main(argv=None):
    a = parse_args(argv)
    sid = a.stock
    a.no_holdings, a.holdings = False, BENCH
    prep = prepare(a, 0, a.years, codes=[sid])
    g = prep["adjusted"][sid].reset_index(drop=True)
    name = prep["meta"].get(sid, {}).get("name", "")
    b = prep["adjusted"][BENCH].set_index("date")["aopen"].reindex(g["date"]).ffill().bfill().to_numpy(float)
    f = ind.features(g)
    buys, sells = ind.buy_signals(f), ind.sell_signals(f)
    ao, ac = g["aopen"].to_numpy(float), g["aclose"].to_numpy(float)
    ok = (g["fill_ok"] if "fill_ok" in g else pd.Series(True, index=g.index)).to_numpy(bool)
    atr = f["atr"].to_numpy(float)
    dates = g["date"].astype(str).to_numpy()
    n = len(g)
    hs = n - HOLDOUT                                   # first test session
    warm = 250                                         # indicators need ~1 year
    ex_end = warm + (hs - warm) * 2 // 3               # explore [warm, ex_end), confirm [ex_end, hs)
    costs = Costs()
    log(f"[single] {sid} {name}: {n} sessions {dates[0]}..{dates[-1]}; explore {dates[warm]}..{dates[ex_end - 1]}, "
        f"confirm {dates[ex_end]}..{dates[hs - 1]}, test {dates[hs]}..{dates[-1]}")

    def run(bname, exit_, stop, lo, hi, carry_ok=False):
        """Trades whose signal day is in [lo, hi); in training the exit must also happen before hi."""
        bm = buys[bname].to_numpy(bool).copy()
        bm[:lo] = False
        bm[hi:] = False
        if isinstance(exit_, int):
            sm, cap = np.zeros(n, bool), exit_
        else:
            sm, cap = sells[exit_].to_numpy(bool), CAP
        rows = trades(bm, sm, ao, ac, ok, atr, cap, STOP if stop else None, np.ones(n, bool))
        out = []
        for t, e, x, gross, held, op in rows:
            t, e, x = int(t), int(e), int(x)
            if not carry_ok and (x >= hi or op):
                continue
            net = costs.net(gross, False)
            out.append({"sig": dates[t], "in": dates[e], "out": dates[x], "pin": ao[e],
                        "pout": ac[x] if op else ao[x], "held": held, "net": net,
                        "xs": net - (b[x] / b[e] - 1), "open": bool(op)})
        return out

    exits = list(sells) + list(HOLDS)
    cands = [(bn, ex, st) for bn in buys for ex in exits for st in (False, True)]
    explored = []
    for c in cands:
        tr = run(*c, warm, ex_end)
        s = stats([r["xs"] for r in tr])
        if s["n"] >= 6 and np.isfinite(s["t"]):
            explored.append((s["t"], c, s))
    explored.sort(key=lambda z: -z[0])
    top = explored[:TOP_EXPLORE]
    confirmed = []
    for t_, c, s in top:
        cs = stats([r["xs"] for r in run(*c, ex_end, hs)])
        confirmed.append((c, s, cs))
    ok_c = [z for z in confirmed if z[2]["n"] >= 3 and z[2]["mean"] > 0]
    chosen = max(ok_c, key=lambda z: z[2]["mean"]) if ok_c else None

    def label(c):
        bn, ex, st = c
        sell = f"抱 {ex} 日" if isinstance(ex, int) else ind.SELL_DESC.get(ex, ex)
        return f"{ind.BUY_DESC.get(bn, bn)} → {sell}{'＋停損' if st else ''}"

    bh = ac[-1] / ao[hs] - 1
    b_bh = b[-1] / b[hs] - 1
    L = [f"# {sid} {name}：個股買賣訊號回測（最後半年驗證）", "",
         f"資料 {fd(dates[0])}～{fd(dates[-1])}（{n} 個交易日）。全部組合 {len(cands):,} 種："
         f"向上訊號 {len(buys)} 個 ×（向下訊號 {len(sells)} 個 或 固定抱 {'/'.join(map(str, HOLDS))} 日）× 有無停損。", "",
         "| 階段 | 期間 | 用途 |", "|---|---|---|",
         f"| 探索 | {fd(dates[warm])}～{fd(dates[ex_end - 1])} | 所有組合排名（至少 6 筆，看 t 值＝穩定度） |",
         f"| 確認 | {fd(dates[ex_end])}～{fd(dates[hs - 1])} | 探索前 {TOP_EXPLORE} 名再考一次，選平均超額最高且為正的 |",
         f"| **驗證（沒看過）** | {fd(dates[hs])}～{fd(dates[-1])} | 選定的規則凍結，直接交易 |", "",
         f"超額 = 扣成本報酬 − 同期間 0050。探索前 {len(explored):,} 種組合有足夠筆數。", ""]
    if chosen is None:
        L += ["## 結果：沒有規則通過確認", "", "探索期前 20 名在確認期沒有一個平均贏 0050，建議這檔不用訊號交易。", ""]
    else:
        c, s, cs = chosen
        test = run(*c, hs, n, carry_ok=True)
        ts = stats([r["xs"] for r in test])
        comp = np.prod([1 + r["net"] for r in test]) - 1 if test else 0.0
        L += ["## 選出的規則（只用驗證期以前的資料）", "", f"**{label(c)}**", "",
              "| 階段 | 筆數 | 每筆平均超額 | 勝率(贏0050) | t | 每筆平均報酬 |", "|---|---|---|---|---|---|",
              f"| 探索 | {s['n']} | {p(s['mean'])} | {p(s['win'], 0).lstrip('+')} | {s['t']:+.1f} | – |",
              f"| 確認 | {cs['n']} | {p(cs['mean'])} | {p(cs['win'], 0).lstrip('+')} | {cs['t']:+.1f} | – |",
              f"| **驗證** | {ts['n']} | {p(ts['mean'])} | {p(ts['win'], 0).lstrip('+')} | "
              f"{ts['t']:+.1f} | {p(np.mean([r['net'] for r in test]) if test else np.nan)} |", "",
              f"驗證期累計（依序複利）：規則 {p(comp, 1)}；同期間一直抱 {sid} {p(bh, 1)}；一直抱 0050 {p(b_bh, 1)}。", "",
              "### 驗證期每一筆", "", "| 訊號日 | 買進日 | 買價 | 賣出日 | 賣價 | 持有日 | 報酬(扣成本) | 比 0050 |",
              "|---|---|---|---|---|---|---|---|"]
        for r in test:
            L.append(f"| {fd(r['sig'])} | {fd(r['in'])} | {r['pin']:.2f} | {fd(r['out'])}{'（未賣，以收盤計）' if r['open'] else ''} | "
                     f"{r['pout']:.2f} | {int(r['held'])} | {p(r['net'], 1)} | {p(r['xs'], 1)} |")
        if not test:
            L.append("| 驗證期沒有出現買進訊號 | | | | | | | |")
        L.append("")
    L += ["## 探索期前 10 名，在確認期與驗證期的表現（事後對照，不是用來挑的）", "",
          "| 名次 | 規則 | 探索 筆數/平均超額/t | 確認 筆數/平均超額 | 驗證 筆數/平均超額 |", "|---|---|---|---|---|"]
    for i, (c, s, cs) in enumerate(confirmed[:10], 1):
        ts = stats([r["xs"] for r in run(*c, hs, n, carry_ok=True)])
        L.append(f"| {i} | {label(c)} | {s['n']} / {p(s['mean'])} / {s['t']:+.1f} | {cs['n']} / {p(cs['mean'])} | "
                 f"{ts['n']} / {p(ts['mean'])} |")
    L += ["", f"注意：試了 {len(cands):,} 種組合，就算全部無效，探索期也一定會有幾組看起來很好；"
          "所以要看確認期與驗證期是否還撐得住。驗證期只有半年、筆數少，單一結果要保守看。", ""]
    rp = a.report or os.path.join(OUT_DIR, f"research_single_{sid}.md")
    os.makedirs(os.path.dirname(rp), exist_ok=True)
    with open(rp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    log(f"[single] report → {rp}")


if __name__ == "__main__":
    main()
