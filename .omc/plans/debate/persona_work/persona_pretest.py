"""Past-only pretest: type stability/counts, confounds, field audit, delisted coverage. No post-event returns."""
import glob
import json
import os
import pickle
import sys
import urllib.request

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency

REPO = r"D:\個人資料\投資理財\股票網站分析\Stock-analysis-"
sys.path.insert(0, os.path.join(REPO, "backend"))
from tuning import research_combo as rc  # noqa: E402
from tuning import research_persona as rp  # noqa: E402
from tuning.run_tuning import prepare  # noqa: E402

SP = os.path.dirname(os.path.abspath(__file__))
df, _ = pickle.load(open(os.path.join(SP, "persona_events.pkl"), "rb"))
c1 = df[df.sit == "C1"].copy()
a = rc.parse_args(["--source", "parquet", "--universe", "500", "--years", "8", "--adjust-source", "exchange"])
prep = prepare(a, a.universe, a.years)
L = ["# Persona pretest (past-only; no post-event returns)", ""]
print(len(c1), "C1 events")

ME = pd.date_range("2021-12-31", "2026-09-30", freq="ME")


def ds(t):
    return t.strftime("%Y%m%d")


types = {}
for m in ME:
    lo, hi = m - pd.DateOffset(months=36), m + pd.Timedelta(days=1)
    types[m] = rp.traits(prep, ds(lo), ds(hi))
    print(m.date(), len(types[m]), flush=True)

# 1. counts
rows = []
for m in ME:
    lo, mid = m - pd.DateOffset(months=36), m - pd.DateOffset(months=18)
    w = c1[(c1.date > lo) & (c1.date <= m)].copy()
    w["t"] = w.sid.map(types[m])
    w = w.dropna(subset=["t"])
    for t, g in w.groupby("t"):
        h1, h2 = (g.date <= mid).sum(), (g.date > mid).sum()
        n, s = len(g), g.sid.nunique()
        rows.append((m.strftime("%Y-%m"), t, n, s, h1, h2, n >= 200, s >= 20, min(h1, h2) >= 100))
cnt = pd.DataFrame(rows, columns=["month", "type", "events", "stocks", "h1", "h2", "ok200", "ok20", "ok100h"])
cnt["pass"] = cnt.ok200 & cnt.ok20 & cnt.ok100h
L += ["## 1. Per-type trailing-36m C1 counts (type as of month-end, h1/h2 = older/newer 18m)", "",
      "Summary by type over month-ends (min / median / max, months passing each threshold):", "",
      "| type | events min/med/max | stocks min/med/max | min-half min/med/max | pass>=200 | pass>=20 stk | pass>=100/half | pass all | months |",
      "|---|---|---|---|---|---|---|---|---|"]
for t, g in cnt.groupby("type"):
    def f(c):
        return f"{g[c].min()}/{int(g[c].median())}/{g[c].max()}"
    mh = g[["h1", "h2"]].min(axis=1)
    L.append(f"| {t} | {f('events')} | {f('stocks')} | {mh.min()}/{int(mh.median())}/{mh.max()} | {g.ok200.sum()} | {g.ok20.sum()} | {g.ok100h.sum()} | {g['pass'].sum()} | {len(g)} |")
L += ["", "Per-month table (quarter-ends shown; all months in persona_pretest_counts.csv). Flags: E=<200 events, S=<20 stocks, H=<100 in a half", "",
      "| month | type | events | stocks | h1 | h2 | flags |", "|---|---|---|---|---|---|---|"]
cnt.to_csv(os.path.join(SP, "persona_pretest_counts.csv"), index=False)
for r in cnt.itertuples():
    if r.month[5:] in ("03", "06", "09", "12"):
        fl = ("E" if not r.ok200 else "") + ("S" if not r.ok20 else "") + ("H" if not r.ok100h else "")
        L.append(f"| {r.month} | {r.type} | {r.events} | {r.stocks} | {r.h1} | {r.h2} | {fl or 'ok'} |")
# churn
ch = []
for p, m in zip(ME[:-1], ME[1:]):
    both = set(types[p]) & set(types[m])
    ch.append((m.strftime("%Y-%m"), sum(types[p][s] != types[m][s] for s in both) / len(both), len(both)))
chd = pd.DataFrame(ch, columns=["month", "churn", "n"])
L += ["", f"### Monthly type churn (share of stocks whose type changed vs previous month): mean {chd.churn.mean():.3f}, median {chd.churn.median():.3f}, min {chd.churn.min():.3f}, max {chd.churn.max():.3f}", "",
      "| " + " | ".join(chd.month[::6]) + " |", "|" + "---|" * len(chd.month[::6]), "| " + " | ".join(f"{x:.2f}" for x in chd.churn[::6]) + " |"]
y12 = []
for m in ME[12:]:
    pm = ME[list(ME).index(m) - 12]
    both = set(types[pm]) & set(types[m])
    y12.append(sum(types[pm][s] != types[m][s] for s in both) / len(both))
L.append(f"\n12-month type change share (mean over month-ends): {np.mean(y12):.3f}")

# 2. confounds
ev = c1[(c1.date >= "2023-01-01") & (c1.date <= "2026-09-30")].copy()
ev["pme"] = ev.date.map(lambda d: (d.to_period("M") - 1).to_timestamp("M"))
ev["t"] = [types.get(p, {}).get(s) for p, s in zip(ev.pme, ev.sid)]
ev = ev.dropna(subset=["t"])
tw = {x["code"]: x["industry"] for x in json.load(open(os.path.join(REPO, "public/data/stocklist.json"), encoding="utf8"))
      if x["industry"].isdigit()}
tp_f = os.path.join(SP, "tpex_ind.json")
if not os.path.exists(tp_f):
    raw = urllib.request.urlopen("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O", timeout=60).read()
    open(tp_f, "wb").write(raw)
tp = {r["SecuritiesCompanyCode"]: r["SecuritiesIndustryCode"] for r in json.load(open(tp_f, encoding="utf8"))}
ev["ind"] = ev.sid.map(lambda s: tw.get(s) or tp.get(s))
miss = ev.ind.isna().mean()


def cv(ct):
    chi2 = chi2_contingency(ct)[0]
    n = ct.values.sum()
    return np.sqrt(chi2 / (n * (min(ct.shape) - 1)))


ei = ev.dropna(subset=["ind"])
ct = pd.crosstab(ei.t, ei.ind)
Lc = ["", "## 2. Confounds (past-only)", "", f"C1 events 2023-01..2026-09 with type: {len(ev)}; industry coverage {1-miss:.1%} (TWSE stocklist + TPEx cache).", "",
      f"Type x industry Cramer's V = **{cv(ct):.3f}** ({ct.shape[1]} industries; not bias-corrected, upward-biased with sparse cells)", "",
      "Crosstab type x industry (industries with >=150 events; rest pooled as other):", ""]
big = ct.sum()[ct.sum() >= 150].index
ct2 = ct[big].assign(other=ct.drop(columns=big).sum(axis=1))
Lc += ["| type | " + " | ".join(ct2.columns) + " |", "|" + "---|" * (len(ct2.columns) + 1)]
for t, r in ct2.iterrows():
    Lc.append(f"| {t} | " + " | ".join(str(v) for v in r) + " |")
ac = {s: g.set_index("date")["aclose"] for s, g in prep["adjusted"].items()}


def r12(row):
    g = ac.get(row.sid)
    if g is None:
        return np.nan
    i = g.index.get_indexer([ds(row.date)])[0]
    if i < 253:
        return np.nan
    return g.iloc[i - 1] / g.iloc[i - 253] - 1   # strictly pre-event


ev["r12"] = [r12(r) for r in ev.itertuples()]
er = ev.dropna(subset=["r12"]).copy()
er["q"] = pd.qcut(er.r12, 5, labels=["Q1(low)", "Q2", "Q3", "Q4", "Q5(high)"])
ct = pd.crosstab(er.t, er.q)
Lc += ["", f"Type x trailing-12m-return quintile (pooled quintiles, return up to day before event; n={len(er)}): Cramer's V = **{cv(ct):.3f}**", "",
       "| type | " + " | ".join(ct.columns) + " |", "|" + "---|" * (len(ct.columns) + 1)]
for t, r in ct.iterrows():
    Lc.append(f"| {t} | " + " | ".join(str(v) for v in r) + " |")
L += Lc

# 3. audit
L += ["", "## 3. X1 strict3 field audit (backend/tuning/research_combo.py)", "",
      "strict3 = ((n_struct>=2) & (n_conf>=2)) | stage4. Data: SBL history/sbl_YYYY.parquet from 2022-01-03; TDCC history/tdcc.parquet from 2025-10-23 (weekly). "
      "Before those dates the SBL/TDCC signals cannot fire (column NaN, signal absent), silently weakening the chips leg.", "",
      "| item | signals | SBL (2022+) | TDCC (2025-10+) |", "|---|---|---|---|",
      "| STRUCT (n_struct) = BREAK minus s_atr_25 | s_pat_lh, s_pat_ll, s_pat_bear_struct, s_pat_n_down, s_pat_m_top, s_tl_up_break, s_ma_bear_align | no | no |",
      "| CONF money | s_vol_div + OBV death cross | no | no |",
      "| CONF chips | s_foreign_sell_5, s_whale_disp, s_sbl_surge, s_margin_surge | **s_sbl_surge** | **s_whale_disp** |",
      "| CONF env (WORSE) | s_mkt_turn_bear, s_rs_low, s_rev_yoy_neg | no | no |",
      "| stage4 | close<MA60 & MA60<MA60[-20] | no | no |",
      "| C1 buy trig | brk_20, brk_55, pat_w_bottom, pat_n_up, tl_down_break, ma_squeeze_brk, bb_squeeze_brk, gap_up | no | no |",
      "| C1 money | obv_high, vol_20_up, vol_15_up, dry_expand, OBV golden cross | no | no |",
      "| C1 other CHIPS | foreign_streak_3/5, trust_streak_3/5, trust_first, chip_streak_3/5, chip_p80/p90, whale_conc, whale_400_jump, margin_drop_up | no | **whale_conc, whale_400_jump** |",
      "| C1 other RS / REV | rs_high, rs_lead, rs_resilient / rev_high12, rev_yoy_accel, rev_yoy_3up | no | no |",
      "", "Implication: C1 depends on TDCC only through two alternatives inside the 'other' OR-leg (negligible before 2025-10). strict3 chips leg has 2 of 4 members (s_foreign_sell_5, s_margin_surge) before 2022, 3 of 4 in 2022..2025-09, all 4 from 2025-10; "
      "so strict3 is not time-homogeneous across 2021-2026."]

# 4. delisted
fr = []
fl_ = sorted(glob.glob(os.path.join(REPO, "history", "prices_*.parquet")))
for f in fl_:
    d = pd.read_parquet(f, columns=["stock_id", "date"])
    fr.append(d.groupby("stock_id")["date"].agg(["min", "max"]))
al = pd.concat(fr)
ag = al.groupby(level=0).agg(first=("min", "min"), last=("max", "max"))
mx = ag["last"].max()
dead = ag[ag["last"] < (pd.Timestamp(mx) - pd.Timedelta(days=30)).strftime("%Y%m%d")].copy()
dead = dead[dead.index.str.fullmatch(r"\d{4}")]
dead["yr"] = dead["last"].str[:4]
L += ["", "## 4. Delisted / stopped-trading coverage", "",
      f"Files: {', '.join(os.path.basename(f) for f in fl_)}. Latest date {mx}. Codes: {len(ag)} (4-digit: {ag.index.str.fullmatch(r'[0-9]{4}').sum()}).",
      f"4-digit codes whose last price date is >30d before {mx}: **{len(dead)}**", "", "| last-trade year | codes |", "|---|---|"]
for y_, n_ in dead.groupby("yr").size().items():
    L.append(f"| {y_} | {n_} |")
L.append("")
L.append("Examples: " + ", ".join(f"{i}({r['last']})" for i, r in dead.sort_values('last').head(12).iterrows()))
in_prep = [s for s in dead.index if s in prep["adjusted"]]
L.append(f"Of these, in the adjusted set used by events: {len(in_prep)}; C1 events on them: {int(c1.sid.isin(dead.index).sum())}.")
open(os.path.join(SP, "persona_pretest.md"), "w", encoding="utf8").write("\n".join(L))
print("done")
