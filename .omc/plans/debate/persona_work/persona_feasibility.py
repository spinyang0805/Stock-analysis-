"""Event COUNTS only. No prices/returns used beyond signal construction."""
import os
import pickle
import sys

import numpy as np
import pandas as pd

REPO = r"D:\個人資料\投資理財\股票網站分析\Stock-analysis-"
sys.path.insert(0, os.path.join(REPO, "backend"))
from tuning import obvs  # noqa: E402
from tuning import research_combo as rc  # noqa: E402
from tuning.run_tuning import prepare  # noqa: E402

SP = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(SP, "persona_events.pkl")
SITS = ["C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8"]


def shift(x):
    return np.r_[False, x[:-1]]


def edge(x):
    x = np.asarray(x, bool)
    return x & ~shift(x)


def obvs_on(S, p, n):
    """Raw turn-on days of obvs.trades' state machine (signal only: no position, tp=sp=trail=0)."""
    c, h, l = S.c, S.h, S.l
    mode, pu, pdn = p["mode"], p["pu"] / 100, p["pd"] / 100
    ef, es = S.ema(p["fast"]), S.ema(p["slow"])
    if mode == 2:
        hi, lo = S.channel(p["chN"], p["chM"])
    if mode == 3:
        rv = S.rsi(p["rsi_len"])
    st_b = st_c = st_h = False
    pdir, ext = 1, np.nan
    bull_prev = obv_bull_prev = False
    on = np.zeros(n, bool)
    for i in range(n):
        ci = c[i]
        if not np.isfinite(ci):
            continue
        obv_bull = ef[i] > es[i]
        obv_on = obv_bull and not obv_bull_prev
        if mode == 2 and np.isfinite(hi[i]):
            if S.obv[i] > hi[i]:
                st_b = True
            elif S.obv[i] < lo[i]:
                st_b = False
        if mode == 3 and i > 0:
            if rv[i] > p["rsi_up"] >= rv[i - 1]:
                st_c = True
            elif rv[i] < p["rsi_dn"] <= rv[i - 1]:
                st_c = False
        prev_dir = pdir
        if not np.isfinite(ext):
            ext = ci
        if pdir == 1:
            ext = max(ext, h[i])
            if ci < ext * (1 - pdn):
                pdir, ext = -1, l[i]
        else:
            ext = min(ext, l[i])
            if ci > ext * (1 + pu):
                pdir, ext = 1, h[i]
        p_up = pdir == 1 and prev_dir != 1
        if not st_h and (p_up or (mode == 5 and obv_on)):
            st_h = True
        elif st_h and (pdir == -1 and not obv_bull):
            st_h = False
        bull = obv_bull if mode == 1 else st_b if mode == 2 else st_c if mode == 3 else st_h
        on[i] = bull and not bull_prev
        bull_prev, obv_bull_prev = bull, obv_bull
    return on


def dedupe(idx, gap=20):
    keep, last = [], -10 ** 9
    for i in idx:
        if i - last >= gap:
            keep.append(i)
            last = i
    return keep


def build():
    a = rc.parse_args(["--source", "parquet", "--universe", "500", "--years", "8", "--adjust-source", "exchange"])
    prep = prepare(a, a.universe, a.years)
    inu = prep["inu"]
    inu_set = set(zip(inu["stock_id"], inu["date"]))
    rows, sy = [], {}
    for k, (sid, g) in enumerate(prep["adjusted"].items()):
        g = g.reset_index(drop=True)
        d = g["date"].to_numpy()
        m = np.array([(sid, x) in inu_set for x in d])
        if not m.any():
            continue
        n = len(g)
        buy, _, _, S, _, det = rc.masks(g, detail=True)
        L = det["layers"]
        env, trig, money, other, stage2 = L["env"], L["trig"], L["money"], L["other"], L["stage2"]
        E = {mm: obvs_on(S, dict(obvs.DEFAULT, mode=mm, tp=0, sp=0, trail=0), n) for mm in (1, 2, 3, 4, 5)}
        ev = {"C1": buy["main"],
              "C2": edge(trig & money & other & ~env),
              "C3": edge(env & trig & money & ~other),
              "C4": E[1] & L["state"],
              "C5": E[2] & env & money,
              "C6": E[3] & env & money,
              "C7": E[4] & env & trig & money,
              "C8": E[5] & stage2 & trig & money}
        sy[sid] = m.sum() / 252.0
        for s in SITS:
            idx = np.flatnonzero(np.asarray(ev[s], bool) & m)   # events on in-universe days only
            for i in dedupe(idx):
                rows.append((sid, s, d[i]))
        if k % 50 == 0:
            print(k, sid, flush=True)
    df = pd.DataFrame(rows, columns=["sid", "sit", "date"])
    df["date"] = pd.to_datetime(df["date"].astype(str))
    pickle.dump((df, sy), open(CACHE, "wb"))
    return df, sy


def main():
    df, sy = pickle.load(open(CACHE, "rb")) if os.path.exists(CACHE) else build()
    L = ["# Persona feasibility (event counts only, no returns)", "",
         "Universe: point-in-time top-500, events only on in-universe days; 8y parquet, exchange-adjusted. "
         "Dedupe: >=20 trading days after previous kept event per stock per situation.", "",
         "## Per situation", "",
         "| sit | events | stocks with >=1 | stocks in universe | events/stock-yr mean | median | pooled |",
         "|---|---|---|---|---|---|---|"]
    allst = list(sy)
    syr = pd.Series(sy).reindex(allst)
    for s in SITS:
        e = df[df.sit == s]
        cnt = e.groupby("sid").size().reindex(allst).fillna(0)
        rate = (cnt / syr)[syr > 0]
        L.append(f"| {s} | {len(e)} | {e.sid.nunique()} | {len(allst)} | {rate.mean():.3f} | {rate.median():.3f} | "
                 f"{len(e) / sum(sy.values()):.3f} |")
    L += ["", "## Deduped events per year", "", "| sit | " + " | ".join(str(y) for y in range(2019, 2027)) + " |",
          "|---|" + "---|" * 8]
    for s in SITS:
        yc = df[df.sit == s].date.dt.year.value_counts()
        L.append(f"| {s} | " + " | ".join(str(int(yc.get(y, 0))) for y in range(2019, 2027)) + " |")
    L += ["", "## Eligible cells per month-end (trailing 36m: N>=30 and >=15 in each 18m half)", ""]
    months = pd.period_range("2022-12", "2026-09", freq="M")
    grp = {k: pd.DatetimeIndex(v.date) for k, v in df.groupby(["sid", "sit"])}
    elig_by_month, tab = {}, []
    for mth in months:
        end = mth.to_timestamp(how="end").normalize()
        st = end - pd.DateOffset(months=36)
        mid = end - pd.DateOffset(months=18)
        per = {s: 0 for s in SITS}
        el = set()
        for (sid, s), dd in grp.items():
            w = dd[(dd > st) & (dd <= end)]
            if len(w) >= 30 and (w <= mid).sum() >= 15 and (w > mid).sum() >= 15:
                per[s] += 1
                el.add((sid, s))
        elig_by_month[end] = el
        tab.append((str(mth), per, sum(per.values())))
    L += ["| month-end | " + " | ".join(SITS) + " | total |", "|---|" + "---|" * 9]
    for m, per, t in tab:
        L.append(f"| {m} | " + " | ".join(str(per[s]) for s in SITS) + f" | {t} |")
    ends = sorted(elig_by_month)
    tot, bysit = 0, {s: 0 for s in SITS}
    for r in df[df.date >= "2023-01-01"].itertuples():
        prev = [e for e in ends if e < r.date]
        if prev and (r.sid, r.sit) in elig_by_month[prev[-1]]:
            tot += 1
            bysit[r.sit] += 1
    L += ["", "## Max labelled events 2023-01..2026-09", "",
          f"Deduped events in 2023+ whose (stock, situation) cell was eligible at the preceding month-end: "
          f"total **{tot}**; by situation {bysit}"]
    ever = set().union(*elig_by_month.values())
    L += ["", f"Distinct cells ever eligible: {len(ever)}"]
    open(os.path.join(SP, "persona_feasibility.md"), "w", encoding="utf-8").write("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
