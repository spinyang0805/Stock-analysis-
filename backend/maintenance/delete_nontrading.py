"""Delete stock_daily / chip_daily rows stamped with non-trading dates.

Root cause: TWSE STOCK_DAY_ALL / TPEx insti / TPEx margin silently return the LATEST trading
day's data for weekends/holidays, and old code wrote it under the requested date.

Default = dry run (READ ONLY transaction): lists affected dates, per-source counts, samples
(2330/2317/0050 with the real date they duplicate) and conflicts. Also rewrites
nontrading_dates.txt (the verified list, input for the later history-store cleanup).
--apply = DELETE ... WHERE date = %s per date per table (commit per date), then VACUUM (ANALYZE)
on a session-mode (port 5432) connection. Never VACUUM FULL.

Non-trading = weekend (except known make-up Saturdays) or TWSE holiday per trading_calendar.
Weekday holidays are only recognised for years the calendar covers (the TWSE API only serves the
current year; older years accumulate in backend/.cache/holidays.json). Dates having rows from
per-day official sources (TWSE STOCK_DAY / TPEx tradingStock / TPEx dailyCloseQuotes) are never
deleted; they are printed as conflicts.
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.dirname(HERE)
sys.path.insert(0, BACKEND)

import psycopg2  # noqa: E402
import trading_calendar as cal  # noqa: E402
from export_static_json import load_env_file  # noqa: E402

LIST_FILE = os.path.join(HERE, "nontrading_dates.txt")
TABLES = ("stock_daily", "chip_daily")
KEEP = {"20260102", "20260211", "20260223", "20170930"}  # first/last trading-day markers, make-up Saturday
OFFICIAL = ("TWSE STOCK_DAY", "TPEx tradingStock", "TPEx dailyCloseQuotes")
SAMPLE_IDS = ("2330", "2317", "0050")
# columns compared to find which real date a stamped row duplicates
CMP_COLS = {
    "stock_daily": ("close", "volume"),
    "chip_daily": ("foreign_buy", "investment_trust_buy", "dealer_buy", "margin_balance"),
}


def connect(session=False):
    load_env_file()
    url = os.environ.get("DATABASE_URL")
    if session:
        url = os.environ.get("DIRECT_DATABASE_URL") or url
    if not url:
        raise SystemExit("DATABASE_URL not set")
    if session:
        url = url.replace(":6543/", ":5432/")
    conn = psycopg2.connect(url)
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '10min'")
        cur.execute("SET lock_timeout = '60s'")
    conn.commit()
    return conn


def is_bad(d):
    if d in KEEP or cal.is_trading_day(d):
        return False
    from datetime import datetime
    weekend = datetime.strptime(d, "%Y%m%d").weekday() >= 5
    return weekend or cal.covers(d)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="really delete (default: dry run)")
    args = ap.parse_args()

    src = cal.calendar_source()
    print(f"calendar_source={src} mode={'APPLY' if args.apply else 'DRY RUN'}")
    if src == "fallback":
        raise SystemExit("no holiday data (API unreachable, no cache); refusing to continue")

    conn = connect()
    conn.set_session(readonly=True)
    cur = conn.cursor()

    per = {t: {} for t in TABLES}  # table -> date -> {source: n}
    for t in TABLES:
        cur.execute(f"SELECT date, COALESCE(source, ''), COUNT(*) FROM {t} GROUP BY date, source ORDER BY date")
        for d, s, n in cur.fetchall():
            if is_bad(d):
                per[t].setdefault(d, {})[s] = n

    conflicts = {}
    for t in TABLES:
        for d, srcs in per[t].items():
            if any(s in OFFICIAL for s in srcs):
                conflicts.setdefault(d, set()).add(t)
    final = {t: sorted(d for d in per[t] if d not in conflicts) for t in TABLES}
    final_dates = sorted({d for t in TABLES for d in final[t]})

    totals = {t: 0 for t in TABLES}
    for t in TABLES:
        print(f"\n=== {t}: {len(final[t])} non-trading dates ===")
        cols = CMP_COLS[t]
        for d in final[t]:
            srcs = per[t][d]
            n = sum(srcs.values())
            totals[t] += n
            print(f"{d}  rows={n}  sources={dict(sorted(srcs.items()))}")
            for sid in SAMPLE_IDS:
                cur.execute(f"SELECT {', '.join(cols)} FROM {t} WHERE stock_id=%s AND date=%s", (sid, d))
                row = cur.fetchone()
                if not row or all(v is None for v in row):
                    continue
                cond = " AND ".join(f"{c} IS NOT DISTINCT FROM %s" for c in cols)
                cur.execute(f"SELECT date FROM {t} WHERE stock_id=%s AND date<>%s AND {cond} "
                            f"ORDER BY date DESC LIMIT 3", (sid, d, *row))
                dup = [r[0] for r in cur.fetchall()]
                print(f"    sample {sid}: {dict(zip(cols, row))} duplicates {dup or 'no other date'}")
    print(f"\nTOTAL rows to delete: stock_daily={totals['stock_daily']} chip_daily={totals['chip_daily']}")

    if conflicts:
        print("\nCONFLICTS (rows from per-day official sources; NOT deleted):")
        for d in sorted(conflicts):
            for t in sorted(conflicts[d]):
                print(f"  {t} {d}: {dict(sorted(per[t][d].items()))}")
    conn.rollback()
    conn.close()

    with open(LIST_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(final_dates) + "\n")
    print(f"\nwrote {len(final_dates)} dates to {LIST_FILE}")
    print("FINAL DATES:", " ".join(final_dates))

    if not args.apply:
        print("Dry run only. Re-run with --apply to delete.")
        return

    conn = connect()
    cur = conn.cursor()
    for t in TABLES:
        for d in final[t]:
            # SET LOCAL: session-level settings are not reliably kept under the transaction pooler
            cur.execute("SET LOCAL statement_timeout = '5min'")
            cur.execute(f"DELETE FROM {t} WHERE date = %s", (d,))
            print(f"  {t} {d}: {cur.rowcount} rows deleted", flush=True)
            conn.commit()
    conn.close()

    vconn = connect(session=True)
    vconn.autocommit = True
    vcur = vconn.cursor()
    for t in TABLES:
        vcur.execute(f"VACUUM (ANALYZE) {t}")
        print(f"  VACUUM (ANALYZE) {t} done", flush=True)
    vconn.close()


if __name__ == "__main__":
    main()
