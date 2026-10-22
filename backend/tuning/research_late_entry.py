"""research_late_entry.py — is a late buy of a combo position still OK, and do the price lights help?

Pre-registered in .omc/plans/debate/preregistration_late_entry.md (thresholds fixed before running).
Live rule only (buy 'main', sell 'strict3'). For each paper trade (entry = open after the signal, price P0),
every session d = e+1 … e+19 on which the paper position is still held (sell not decided by close d) gives a
late buy at the next open d+1 (open-to-open, so the benchmark lines up). The late buy exits at the first close
with the strict3 sell or with close ≤ late price × 0.90 (the user's own stop), at the next open. Buckets by
close[d] / P0 − 1 (what the user sees): green −5%…+3%, yellow +3%…+8%, red_hi > +8%, red_lo < −5%; red_lo is
split by "still supported" (stage-2 env and no structure break in 10 sessions). Baseline: on-time paper trades.

  python backend/tuning/research_late_entry.py --source parquet --universe 500 --years 8 --adjust-source exchange
"""
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
from tuning import research_combo as rc  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.run_tuning import prepare  # noqa: E402

LATE = 19                                    # 2nd … 20th session after the paper entry
BUCKETS = (("green", "綠：−5%～+3%", -0.05, 0.03), ("yellow", "黃：+3%～+8%", 0.03, 0.08),
           ("red_hi", "紅：高於 +8%", 0.08, np.inf), ("red_lo", "紅：低於 −5%", -np.inf, -0.05))


def bucket(r):
    r = round(r, 6)                          # same boundary rounding as the website
    for key, _, lo, hi in BUCKETS:
        if (key == "green" and lo <= r <= hi) or (key != "green" and lo < r <= hi):
            return key
    return "red_lo"


def exit_from(sell, ac, ao, ok, start, entry, stop):
    """First close ≥ start with sell or close ≤ entry × (1 − stop) → next tradable open (or open at the end)."""
    n = len(ac)
    end = min(n, start + rc.CAP)
    seg = sell[start:end] | (ac[start:end] <= entry * (1 - stop))
    hit = np.flatnonzero(seg)
    dec = start + hit[0] if len(hit) else end - 1
    x = dec + 1
    while x < n and not ok[x]:
        x += 1
    if x >= n:
        return n - 1, ao[n - 1] / entry - 1, True, dec          # still open: value at the last open (benchmark is open-based)
    return x, ao[x] / entry - 1, False, dec


def stock_job(a):
    sid, g, inu_mask, bench_idx, etf, cp = a
    g = g.reset_index(drop=True)
    buy, sell, _, S, _, det = rc.masks(g, detail=True)
    bm, sm = buy["main"], sell["strict3"]
    lay = det["layers"]
    support = np.asarray(lay["env"], bool) & (np.asarray(lay["n_struct"]) == 0)
    ao, ac, ok = S.o, S.c, S.ok
    costs = Costs()
    rows = []
    for t, e, x, gross, held, is_open in rc.stop_trades(bm, sm, ao, ac, ok, inu_mask, rc.STOP):
        t, e, x = int(t), int(e), int(x)
        p0 = ao[e]
        if is_open:
            gross = ao[x] / p0 - 1                     # still open: value at the last open, like the late buys
        _, _, _, dec = exit_from(sm, ac, ao, ok, e, p0, rc.STOP)     # paper sell decision close
        net = costs.net(gross, etf)
        rows.append(("ontime", 0, e, x, net, net - (bench_idx[x] / bench_idx[e] - 1), x - e, is_open, t))
        for d in range(e + 1, min(e + 1 + LATE, dec, len(ac) - 1)):
            f = d + 1
            # same entry eligibility as the paper buy: tradable, and no limit-up gap open
            if not np.isfinite(ac[d]) or not ok[f] or not np.isfinite(ao[f]) or ao[f] >= ac[d] * 1.095:
                continue
            entry = ao[f]
            lx, lg, lopen, _ = exit_from(sm, ac, ao, ok, f, entry, rc.STOP)
            lnet = costs.net(lg, etf)
            b = bucket(ac[d] / p0 - 1)
            if b == "red_lo":
                b = "red_lo_S" if support[d] else "red_lo_notS"
            rows.append((b, d - e, d, lx, lnet, lnet - (bench_idx[lx] / bench_idx[f] - 1), lx - f, lopen, t))
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["bucket", "lag", "i", "xi", "net", "xs", "held", "open", "sig"])
    d = g["date"].astype(str).to_numpy()
    df["date"] = d[df["i"].to_numpy()]
    df["p"] = cp[df["sig"].to_numpy()]            # cluster by the paper trade's signal day: its late buys stay together
    df["sid"] = sid
    return df


def main(argv=None):
    a = rc.parse_args(argv)
    a.report = os.path.join(OUT_DIR, "research_late_entry.md")
    t0 = time.time()
    prep = prepare(a, a.universe, a.years)
    cal = list(prep["calendar"])
    pos = {d: i for i, d in enumerate(cal)}
    adj, inu = prep["adjusted"], prep["inu"]
    inu_set = set(zip(inu["stock_id"], inu["date"]))
    rets = []
    for sid, g in adj.items():
        o, d = g["aopen"].to_numpy(float), g["date"].to_numpy()
        keep = np.array([(sid, x) in inu_set for x in d])
        rets.append(pd.DataFrame({"date": d[keep], "r": np.r_[o[1:] / o[:-1] - 1, np.nan][keep]}))
    br = pd.concat(rets).groupby("date")["r"].mean().reindex(cal).fillna(0.0).clip(-0.2, 0.2)
    bidx_cal = np.r_[1.0, np.cumprod(1 + br.to_numpy())[:-1]]
    jobs = []
    for sid, g in adj.items():
        g = g.reset_index(drop=True)
        d = g["date"].to_numpy()
        m = np.array([(sid, x) in inu_set for x in d])
        if not m.any():
            continue
        cp = np.array([pos[x] for x in d])
        jobs.append((sid, g, m, bidx_cal[cp], prep["meta"][sid]["etf"], cp))
    log(f"[late] {len(jobs)} stocks prepared in {time.time() - t0:.0f}s")
    with ProcessPoolExecutor(a.workers) as ex:
        res = [r for r in ex.map(stock_job, jobs, chunksize=4) if r is not None]
    df = pd.concat(res, ignore_index=True)
    log(f"[late] simulated {len(df):,} rows in {time.time() - t0:.0f}s")

    def stat(s):
        if len(s) < 10:
            return None
        t, k = ev_mod.t_cluster(s["xs"].to_numpy(), (s["p"] // ev_mod.CLUSTER_DAYS).to_numpy())
        return {"n": len(s), "trades": s[["sid", "sig"]].drop_duplicates().shape[0], "net": s["net"].mean(),
                "xs": s["xs"].mean(), "t": t, "win": (s["xs"] > 0).mean(), "held": s["held"].mean()}

    def line(label, s):
        if s is None:
            return f"| {label} | – | – | – | – | – | – |"
        return (f"| {label} | {s['n']:,}（{s['trades']:,} 筆交易） | {100 * s['net']:+.2f}% | {100 * s['xs']:+.2f}% | "
                f"{s['t']:+.1f} | {100 * s['win']:.0f}% | {s['held']:.0f} 日 |")

    head = "| 進場方式 | 樣本 | 平均淨報酬 | 平均超額 | t | 勝率（超額>0） | 平均持有 |\n|---|---|---|---|---|---|---|"
    labels = ([("ontime", "準時（訊號隔天開盤，基準）")] + [(b[0], f"晚買 {b[1]}") for b in BUCKETS[:3]]
              + [("red_lo_S", "晚買 紅：低於 −5%，但季線多頭且結構沒壞"), ("red_lo_notS", "晚買 紅：低於 −5%，且季線或結構已轉壞")])
    L = [f"# 組合拳：晚幾天才買還行嗎？（資料 {cal[0]}–{cal[-1]}）", "",
         "事前登記：.omc/plans/debate/preregistration_late_entry.md（門檻 −5%／+3%／+8% 先定好，跑完不改）。", "",
         "晚買 = 紙上部位持有中的第 2～20 個交易日收盤看燈號，隔天開盤買；"
         "之後照嚴格 B 賣出訊號賣，或跌到自己買價 × 0.9 停損。燈號依「買價 ÷ 紙上買價 − 1」分組。"
         "同一筆紙上交易會產生很多個晚買樣本（彼此不獨立），t 值以紙上交易的訊號日 20 日分群校正。", "",
         "## 全期", "", head]
    whole = {k: stat(df[df["bucket"] == k]) for k, _ in labels}
    L += [line(lab, whole[k]) for k, lab in labels]
    per = {}
    for name, lo, hi in rc.PERIODS:
        sub = df[(df["date"] >= lo) & (df["date"] <= hi)]
        per[name] = {k: stat(sub[sub["bucket"] == k]) for k, _ in labels}
        L += ["", f"## {name}", "", head] + [line(lab, per[name][k]) for k, lab in labels]
    L += ["", "## 依晚幾天分組（全部燈號合併）", "", head]
    for lo, hi in ((1, 5), (6, 10), (11, 19)):
        L.append(line(f"第 {lo + 1}～{hi + 1} 個交易日", stat(df[(df["bucket"] != "ontime") & df["lag"].between(lo, hi)])))

    def passes(s):
        g, rh, ls, ln = s.get("green"), s.get("red_hi"), s.get("red_lo_S"), s.get("red_lo_notS")
        rl = [x for x in (ls, ln) if x]
        rl_xs = sum(x["xs"] * x["n"] for x in rl) / sum(x["n"] for x in rl) if rl else None
        return [bool(g) and g["xs"] > 0 and g["t"] >= 2, bool(g and rh) and g["xs"] > rh["xs"],
                bool(g) and rl_xs is not None and g["xs"] > rl_xs,
                bool(ls and ln) and ls["xs"] > 0 and ls["t"] >= 2 and ls["xs"] > ln["xs"]]

    pw = passes(whole)
    pp = {name: passes(per[name]) for name in per}
    crit = ["綠燈平均超額 > 0 且 t ≥ 2", "綠燈 > 紅（高於 +8%）", "綠燈 > 紅（低於 −5%，兩組合併）",
            "補充：低於 −5% 但結構沒壞 > 0 且 t ≥ 2，並 > 結構已壞"]
    L += ["", "## 事前登記的通過標準", ""]
    for i, c in enumerate(crit):
        n_ok = sum(pp[name][i] for name in pp)
        L.append(f"- {c}：全期 {'通過' if pw[i] else '未通過'}；三個時期中 {n_ok}／3 個通過")
    ok_all = all(pw[:3]) and all(sum(pp[name][i] for name in pp) >= 2 for i in range(3))
    ok_s = pw[3] and sum(pp[name][3] for name in pp) >= 2
    L += ["", f"**補充條件：{'通過，跌超過 5% 但結構沒壞的改顯示可以買' if ok_s else '未通過，跌超過 5% 一律維持紅燈'}**"]
    L += ["", f"**結論：{'燈號照設計上線' if ok_all else '未全部通過，交由使用者決定'}**", ""]
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L))
    log(f"[late] report → {a.report} ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
