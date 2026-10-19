"""research_rank.py — monthly market-wide ranking portfolios (read-only research; writes a Markdown report).

User question (2026-10-19): per-stock signals lose to the market once look-ahead is removed — what can win?
Instead of waiting for per-stock signals, rank the WHOLE point-in-time universe once a month and hold the top N
equal-weighted until the next month. Factors are fixed in advance (not tuned), each uses only data known at the
close of the rebalance day:
  mom      12-1 month momentum: close[t-21] / close[t-252] - 1
  rev      latest PUBLISHED monthly revenue YoY (as-of avail_date)
  rev_acc  revenue YoY minus the YoY ~3 months earlier (growth speeding up)
  combo    mean percentile of mom and rev
  combo_v  mean percentile of mom, rev and cheapness (low P/B)
  combo_f  combo, but all cash when the equal-weight universe index is below its 200-day average
Execution: score at close t → buy at open t+1; hold to the next rebalance open (21 sessions). Costs on the
replaced names only (fees + tax). A name with no price at exit is valued at its last close.
Benchmarks: equal-weight universe (same days, no cost) and 0050 / 006208 buy-and-hold (total return).

  python backend/tuning/research_rank.py --source parquet --universe 500 --years 8 --adjust-source exchange
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

from tuning.data import OUT_DIR, log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.run_tuning import add_common_args, prepare  # noqa: E402

EVERY = 21
LOOK = 252
TOPS = (10, 20, 30)
ETFS = ("0050", "006208")
STRATS = ("mom", "rev", "rev_acc", "combo", "combo_v", "combo_f")
NAMES = {"mom": "強勢股（過去 12 個月漲最多，扣掉最近 1 個月）", "rev": "月營收年增率最高",
         "rev_acc": "月營收成長在加速", "combo": "強勢股＋營收成長（合併排名）",
         "combo_v": "強勢股＋營收成長＋股價淨值比低", "combo_f": "強勢股＋營收成長，大盤跌破 200 日線就空手"}


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--universe", type=int, default=500)
    ap.add_argument("--years", type=float, default=8.0)
    ap.add_argument("--workers", type=int, default=4)   # accepted for the shared workflow; unused
    ap.add_argument("--report", default=os.path.join(OUT_DIR, "research_rank.md"))
    add_common_args(ap)
    ap.set_defaults(no_holdings=False, holdings=",".join(ETFS))
    return ap.parse_args(argv)


def wide(adjusted, col, cal):
    return pd.DataFrame({sid: g.set_index("date")[col] for sid, g in adjusted.items()
                         if col in g}).reindex(cal).astype(float)


def pct_rank(s):
    return s.rank(pct=True)


def scores_at(t, P, member):
    """Factor scores for in-universe names on calendar position t (NaN = not rankable)."""
    m = member[t]
    out = {}
    mom = P["close_ff"].iloc[t - 21] / P["close_ff"].iloc[t - LOOK] - 1
    rev = P["rev_yoy"].iloc[t]
    acc = rev - P["rev_yoy"].iloc[t - 63]
    pb = P["pb"].iloc[t]
    live = m & P["close"].iloc[t].notna()
    mom, rev, acc = mom.where(live), rev.where(live), acc.where(live)
    pb = pb.where(live & (pb > 0))
    out["mom"], out["rev"], out["rev_acc"] = mom, rev, acc
    both = pd.concat([pct_rank(mom), pct_rank(rev)], axis=1)
    out["combo"] = both.mean(axis=1, skipna=False)
    out["combo_v"] = pd.concat([pct_rank(mom), pct_rank(rev), pct_rank(-pb)], axis=1).mean(axis=1, skipna=False)
    return out


def period_ret(P, sids, e, x):
    """Gross open(e) → open(x) per name; missing exit open → last close before x; missing entry → dropped."""
    po = P["open"].iloc[e][sids]
    px = P["open"].iloc[x][sids] if x < len(P["open"]) else P["close_ff"].iloc[-1][sids]
    px = px.fillna(P["close_ff"].iloc[min(x, len(P["open"])) - 1][sids])
    r = px / po - 1
    return r[po.notna() & (po > 0)].clip(-0.95, 5)


def main(argv=None):
    a = parse_args(argv)
    t0 = time.time()
    prep = prepare(a, a.universe, a.years)
    cal = list(prep["calendar"])
    adj = prep["adjusted"]
    P = {"open": wide(adj, "aopen", cal), "close": wide(adj, "aclose", cal),
         "rev_yoy": wide(adj, "rev_yoy", cal), "pb": wide(adj, "pb", cal)}
    P["close_ff"] = P["close"].ffill()
    cols = P["open"].columns
    inu = prep["inu"]
    M = (inu.assign(v=True).pivot_table(index="date", columns="stock_id", values="v", aggfunc="any")
         .reindex(index=cal, columns=cols).fillna(False).astype(bool))
    member = {i: M.iloc[i] for i in range(len(cal))}
    # equal-weight universe index (close) for the 200-day filter
    cr = P["close"].pct_change(fill_method=None).where(M)
    ew_close = (1 + cr.mean(axis=1).fillna(0).clip(-0.2, 0.2)).cumprod()
    ew_ma200 = ew_close.rolling(200, min_periods=150).mean()
    costs = Costs()
    rt = costs.buy() + costs.sell(False)
    log(f"[rank] {len(cols)} names, {len(cal)} days, round-trip cost {100 * rt:.3f}% in {time.time() - t0:.0f}s")

    rebals = list(range(LOOK, len(cal) - 1, EVERY))
    rows = []                                         # one row per (period)
    held = {(s, n): set() for s in STRATS for n in TOPS}
    for k, t in enumerate(rebals):
        e = t + 1
        x = (rebals[k + 1] + 1) if k + 1 < len(rebals) else len(cal) - 1
        if x <= e:
            break
        sc = scores_at(t, P, member)
        sc["combo_f"] = sc["combo"]
        # revenue starts 2021, valuation 2022: compare all strategies only once every factor can rank
        ready = all(sc[s].notna().sum() >= 2 * max(TOPS) for s in STRATS)
        filt_on = bool(ew_close.iloc[t] < ew_ma200.iloc[t]) if pd.notna(ew_ma200.iloc[t]) else False
        uni = [c for c in cols if member[t][c]]
        bench = period_ret(P, uni, e, x).mean()
        row = {"d": cal[e], "days": x - e, "bench": bench, "filter": filt_on, "ready": ready}
        for etf in ETFS:
            if etf in cols:
                r = period_ret(P, [etf], e, x)
                row[etf] = float(r.iloc[0]) if len(r) else np.nan
        for s in STRATS:
            ranked = sc[s].dropna().sort_values(ascending=False)
            for n in TOPS:
                if s == "combo_f" and filt_on:
                    pick = set()
                else:
                    pick = set(ranked.index[:n])
                prev = held[(s, n)]
                # replaced fraction of the book (equal weight): sells + buys of changed names
                denom = max(len(pick), len(prev), 1)
                turn = (len(prev - pick) * costs.sell(False) + len(pick - prev) * costs.buy()) / denom
                gross = period_ret(P, sorted(pick), e, x).mean() if pick else 0.0
                row[f"{s}_{n}"] = (1 + (0.0 if np.isnan(gross) else gross)) * (1 - turn) - 1
                row[f"{s}_{n}_turn"] = (len(pick - prev) / denom) if denom else 0.0
                held[(s, n)] = pick
        rows.append(row)
    full = pd.DataFrame(rows)
    start = full.index[full["ready"]].min()
    df = full.loc[start:].reset_index(drop=True)
    log(f"[rank] {len(full)} months simulated, common window from {df['d'].iloc[0]} in {time.time() - t0:.0f}s")

    def stats(r, b=None):
        r = r.dropna()
        if len(r) < 6:
            return None
        yrs = len(r) * EVERY / 252
        tot = (1 + r).prod()
        cagr = tot ** (1 / yrs) - 1
        curve = (1 + r).cumprod()
        mdd = (curve / curve.cummax() - 1).min()
        out = {"cagr": cagr, "mdd": mdd, "n": len(r)}
        if b is not None:
            bb = b.reindex(r.index)
            ex = r - bb
            out["win"] = (ex > 0).mean()
            out["t"] = ex.mean() / (ex.std(ddof=1) / np.sqrt(len(ex))) if ex.std(ddof=1) > 0 else 0.0
            btot = (1 + bb).prod()
            out["xs"] = cagr - (btot ** (1 / yrs) - 1)
        return out

    first, last = df["d"].iloc[0], df["d"].iloc[-1]
    L = [f"# 每月全市場排名選股回測（{first}–{last}，約 {len(df) * EVERY / 252:.1f} 年）", "",
         f"每個月（21 個交易日）把當時成交量前 {a.universe} 大的股票全部排名一次，買前 N 名、每檔一樣多錢，持有到下個月再換。"
         "排名只用當天收盤前已知的資料（營收用已公布的月份），隔天開盤買。換股時扣手續費（6 折）與證交稅；"
         "沒換到的股票不扣。排名條件事先固定，沒有拿結果回頭調整。"
         "所有挑法從營收、淨值比資料都齊全的月份才開始比。", ""]
    bench = df.set_index("d")["bench"]
    b_st = stats(bench)
    L += ["## 比較基準", "", "| 基準 | 年化報酬 | 最大跌幅 |", "|---|---|---|",
          f"| 股票池平均（{a.universe} 檔各買一樣多，不扣成本） | {100 * b_st['cagr']:+.1f}% | {100 * b_st['mdd']:.0f}% |"]
    for etf in ETFS:
        if etf in df:
            s_ = stats(df.set_index("d")[etf])
            if s_:
                L.append(f"| {etf}（買進抱著，含配息） | {100 * s_['cagr']:+.1f}% | {100 * s_['mdd']:.0f}% |")
    L.append("")
    ref = "0050" if "0050" in df and df["0050"].notna().sum() > 6 else None
    for n in TOPS:
        L += [f"## 買前 {n} 名", "",
              "| 挑法 | 年化報酬 | 每年贏股票池 | 每月贏股票池比例 | t | " + (f"每年贏 {ref} | " if ref else "")
              + "最大跌幅 | 每月換掉比例 |",
              "|---|---|---|---|---|" + ("---|" if ref else "") + "---|---|"]
        for s in STRATS:
            r = df.set_index("d")[f"{s}_{n}"]
            st = stats(r, bench)
            if st is None:
                continue
            extra = ""
            if ref:
                sr = stats(r, df.set_index("d")[ref])
                extra = f"{100 * sr['xs']:+.1f}% | "
            L.append(f"| {NAMES[s]} | {100 * st['cagr']:+.1f}% | {100 * st['xs']:+.1f}% | {100 * st['win']:.0f}% | "
                     f"{st['t']:+.1f} | {extra}{100 * st['mdd']:.0f}% | {100 * df[f'{s}_{n}_turn'].iloc[1:].mean():.0f}% |")
        L.append("")
    # yearly breakdown (top 20)
    df["y"] = df["d"].astype(str).str[:4]
    L += ["## 每年報酬（買前 20 名）", "", "| 年 | 股票池 | " + (f"{ref} | " if ref else "")
          + " | ".join(NAMES[s].split("（")[0] for s in STRATS) + " |",
          "|---|---|" + ("---|" if ref else "") + "---|" * len(STRATS)]
    for y, g in df.groupby("y"):
        cells = [f"{100 * ((1 + g['bench']).prod() - 1):+.0f}%"]
        if ref:
            cells.append(f"{100 * ((1 + g[ref].fillna(0)).prod() - 1):+.0f}%")
        cells += [f"{100 * ((1 + g[f'{s}_20']).prod() - 1):+.0f}%" for s in STRATS]
        L.append(f"| {y} | " + " | ".join(cells) + " |")
    fb = full.set_index("d")
    if len(full) > len(df):
        L += ["", f"### 只用股價的強勢股，拉長到 {full['d'].iloc[0]} 起（營收、淨值比資料較晚才有，其他挑法無法比這麼久）", "",
              "| 檔數 | 年化報酬 | 股票池 | 每年贏股票池 | t | " + (f"{ref} | " if ref else "") + "最大跌幅 |",
              "|---|---|---|---|---|" + ("---|" if ref else "") + "---|"]
        fbs = stats(fb["bench"])
        fref = stats(fb[ref]) if ref else None
        for n in TOPS:
            st = stats(fb[f"mom_{n}"], fb["bench"])
            L.append(f"| {n} | {100 * st['cagr']:+.1f}% | {100 * fbs['cagr']:+.1f}% | {100 * st['xs']:+.1f}% | "
                     f"{st['t']:+.1f} | " + (f"{100 * fref['cagr']:+.1f}% | " if fref else "") + f"{100 * st['mdd']:.0f}% |")
    L += ["", f"大盤濾網空手的月份：{int(df['filter'].sum())} / {len(df)}。",
          "t 值看每月超額報酬，|t| ≥ 2 才算明顯（試了 6 種挑法 × 3 種檔數，單一個剛好 2 左右要保守看）。", ""]
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log(f"[rank] report → {a.report} in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
