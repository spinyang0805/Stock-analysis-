"""export_etf_history.py — refresh public/data/etf_history.json (read by the #/loan 實際走勢 tab; public data, no amounts).

  tr           : 0050 total-return month-end index, normalised to 1 at 2003-06 (research_etf_long.adjusted: FinMind
                 prices + cash dividends + splits, back-adjusted; the same logic that produced the current file)
  taiex_price  : TAIEX month-end close from history/market_index.parquet (price index, no dividends)
Only COMPLETED months (before the current Taipei month) are written. The run refuses (exit 1, file untouched) when
the new file would have fewer months than the old one or any existing value moves by more than 0.5%.

  python backend/tuning/export_etf_history.py [--out public/data/etf_history.json]
  python backend/tuning/export_etf_history.py --selftest
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(REPO, "public", "data", "etf_history.json")
TOL = 0.005
SOURCE = "0050 total return (FinMind prices + cash dividends, split-adjusted), month-end"
TAIEX_NOTE = "TAIEX price index (no dividends), month-end; used only for the 2000 dot-com analog"


def month_ends(series, current_ym):
    """series: pandas Series indexed by Timestamp -> (months, values) of each month's last value, months < current_ym."""
    m = series.groupby(series.index.to_period("M")).last()
    m = m[[str(p) < current_ym for p in m.index]]
    return [str(p) for p in m.index], [float(v) for v in m.values]


def build(tr_close, taiex_close, current_ym, today):
    tm, tv = month_ends(tr_close, current_ym)
    xm, xv = month_ends(taiex_close, current_ym)
    return {"source": SOURCE, "generated": today, "months": tm, "tr": [round(v / tv[0], 6) for v in tv],
            "taiex_months": xm, "taiex_price": [round(v, 2) for v in xv], "taiex_note": TAIEX_NOTE}


def check(old, new):
    """Return a list of problems (empty = safe to write)."""
    bad = []
    for mk, vk in (("months", "tr"), ("taiex_months", "taiex_price")):
        if len(new[mk]) < len(old[mk]):
            bad.append(f"{mk}: {len(new[mk])} months < existing {len(old[mk])}")
        nv = dict(zip(new[mk], new[vk]))
        for m, v in zip(old[mk], old[vk]):
            if m not in nv:
                bad.append(f"{mk}: month {m} missing in new data")
            elif abs(nv[m] / v - 1) > TOL:
                bad.append(f"{vk} {m}: {v} -> {nv[m]} ({nv[m] / v - 1:+.2%})")
    return bad


def summary(old, new):
    return (f"months {len(old['months'])} -> {len(new['months'])} (last {new['months'][-1]}); "
            f"taiex_months {len(old['taiex_months'])} -> {len(new['taiex_months'])} (last {new['taiex_months'][-1]})")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args(argv)
    if a.selftest:
        return selftest()
    sys.path.insert(0, os.path.join(REPO, "backend"))
    import pandas as pd
    from tuning.data import HISTORY_DIR
    from tuning.research_etf_long import adjusted
    now = datetime.now(timezone(timedelta(hours=8)))
    e50 = adjusted("0050")["close"]
    ix = pd.read_parquet(os.path.join(HISTORY_DIR, "market_index.parquet"))
    tx = ix[ix["index"] == "TAIEX"]
    taiex = pd.Series(tx["close"].astype(float).to_numpy(), index=pd.to_datetime(tx["date"].astype(str))).sort_index()
    new = build(e50, taiex, now.strftime("%Y-%m"), now.strftime("%Y-%m-%d"))
    old = None
    if os.path.exists(a.out):
        with open(a.out, encoding="utf-8") as f:
            old = json.load(f)
        bad = check(old, new)
        print("[export_etf_history]", summary(old, new))
        if bad:
            print("[export_etf_history] REFUSED, file untouched:")
            for b in bad[:20]:
                print("  -", b)
            return 1
        if (old["months"], old["tr"], old["taiex_months"], old["taiex_price"]) == \
                (new["months"], new["tr"], new["taiex_months"], new["taiex_price"]):
            print("[export_etf_history] no change")
            return 0
    tmp = a.out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(new, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, a.out)
    print("[export_etf_history] wrote", a.out)
    return 0


def selftest():
    import pandas as pd
    idx = pd.to_datetime(["2026-01-15", "2026-01-30", "2026-02-27", "2026-03-31", "2026-04-10"])
    s = pd.Series([10.0, 11.0, 12.0, 13.0, 14.0], index=idx)
    new = build(s, s * 100, "2026-04", "2026-04-11")
    assert new["months"] == ["2026-01", "2026-02", "2026-03"], new["months"]      # current month excluded
    assert new["tr"] == [1.0, round(12 / 11, 6), round(13 / 11, 6)], new["tr"]
    assert new["taiex_price"] == [1100.0, 1200.0, 1300.0]
    assert check(new, new) == []
    shorter = dict(new, months=new["months"][:2], tr=new["tr"][:2])
    assert check(new, shorter), "fewer months must be refused"
    moved = dict(new, tr=[1.0, 1.2, 1.3])
    assert any("2026-02" in b or "2026-03" in b for b in check(new, moved)), "> 0.5% change must be refused"
    near = dict(new, tr=[1.0, new["tr"][1] * 1.004, new["tr"][2]])
    assert check(new, near) == [], "0.4% change is tolerated"
    print("export_etf_history selftest OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
