"""repair_t86_dealer.py — fix TWSE dealer_buy (自營商買賣超股數) that the old T86 parser read from the wrong
column ("外資自營商買賣超股數", index 7, almost always 0) instead of "自營商買賣超股數" (index 11).

Targets: dates whose TWSE chip rows have dealer_buy all 0 / NULL (in the Parquet store, and in Supabase from
the retention cutoff on). One T86 request per date; only dealer_buy of TWSE rows is changed.

  python backend/repair_t86_dealer.py --store            # fix history/chips_*.parquet (local; push separately)
  python backend/repair_t86_dealer.py --db               # dry run against Supabase
  python backend/repair_t86_dealer.py --db --apply       # UPDATE Supabase chip_daily.dealer_buy
Fetched values are cached in history/cache/t86_dealer/<date>.json so --store and --db share one fetch.
"""
import argparse
import json
import os
import sys
import time

BACKEND = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BACKEND)

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

HISTORY = os.path.join(os.path.dirname(BACKEND), "history")
CACHE = os.path.join(HISTORY, "cache", "t86_dealer")


def log(m):
    print(m, flush=True)


def fetch_dealer(jobs, d, sleep):
    path = os.path.join(CACHE, f"{d}.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    time.sleep(sleep)
    p, err = jobs.fetch_json(jobs.TWSE_T86, params={"response": "json", "date": d, "selectType": "ALL"})
    rows = jobs._rows(p)
    if err or not rows or jobs._payload_date_mismatch(p, d):
        return None
    f = jobs._fields(p)
    ci, di = jobs._idx(f, "證券", "代號", default=0), jobs._idx_exact(f, "自營商買賣超股數", default=11)
    out = {str(jobs._row_value(r, ci)).strip(): jobs.safe_int(jobs._row_value(r, di)) for r in rows}
    os.makedirs(CACHE, exist_ok=True)
    with open(path + ".tmp", "w", encoding="utf-8") as fh:
        json.dump(out, fh)
    os.replace(path + ".tmp", path)
    return out


def bad_dates_store():
    g = os.path.join(HISTORY, "chips_*.parquet").replace("\\", "/")
    r = duckdb.sql(f"""SELECT date, count(dealer_buy) nn, count(*) FILTER (WHERE dealer_buy <> 0) nz
                       FROM read_parquet('{g}', union_by_name=true) WHERE market = 'TWSE' GROUP BY 1""").df()
    return sorted(r.loc[(r["nn"] > 100) & (r["nz"] <= 5), "date"])


def fix_store(jobs, sleep):
    import pyarrow as pa
    import pyarrow.parquet as pq
    sys.path.insert(0, BACKEND)
    import history_store as hs
    dates = bad_dates_store()
    log(f"[store] {len(dates)} dates with all-zero TWSE dealer")
    fixed = {}
    fails = 0
    for i, d in enumerate(dates, 1):
        v = fetch_dealer(jobs, d, sleep)
        if v is None:
            fails += 1
            log(f"  [{i}/{len(dates)}] {d} fetch failed ({fails})")
            if fails >= 5:
                break
            continue
        fails = 0
        fixed[d] = v
        if i % 50 == 0:
            log(f"  [{i}/{len(dates)}] fetched")
    for year in sorted({d[:4] for d in fixed}):
        df = hs.read_year("chips", year)
        m = df["market"].eq("TWSE") & df["date"].isin([d for d in fixed if d[:4] == year])
        new = [fixed[d].get(s) for s, d in zip(df.loc[m, "stock_id"], df.loc[m, "date"])]
        df.loc[m, "dealer_buy"] = pd.array(new, dtype="Int64")
        hs.write_year("chips", year, df)
        log(f"  [store] chips_{year}.parquet: dealer_buy rewritten on {int(m.sum()):,} rows")
    return 0


def fix_db(jobs, sleep, apply):
    from history_store import months_cutoff
    from maintenance.delete_nontrading import connect
    from psycopg2.extras import execute_values
    cutoff = months_cutoff(13)
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("""SELECT date FROM chip_daily WHERE market = 'TWSE' AND date >= %s GROUP BY date
                       HAVING count(dealer_buy) > 100 AND count(*) FILTER (WHERE dealer_buy <> 0) <= 5
                       ORDER BY date""", (cutoff,))
        dates = [r[0] for r in cur.fetchall()]
    conn.rollback()
    log(f"[db] {len(dates)} dates since {cutoff} with all-zero TWSE dealer")
    rows = []
    for d in dates:
        v = fetch_dealer(jobs, d, sleep)
        if v:
            rows += [(s, d, val) for s, val in v.items() if val is not None]
    log(f"[db] {len(rows):,} (stock, date) dealer values fetched")
    if not apply:
        log("Dry run only. Re-run with --apply to UPDATE chip_daily.dealer_buy (TWSE rows only).")
        return 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), 2000):
            execute_values(cur, """UPDATE chip_daily AS t SET dealer_buy = v.dealer::bigint, updated_at = NOW()
                                   FROM (VALUES %s) AS v(stock_id, date, dealer)
                                   WHERE t.stock_id = v.stock_id AND t.date = v.date AND t.market = 'TWSE'""",
                           rows[i:i + 2000])
            conn.commit()
            log(f"  updated {min(i + 2000, len(rows)):,}/{len(rows):,}")
    conn.close()
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", action="store_true")
    ap.add_argument("--db", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--sleep", type=float, default=2.0)
    a = ap.parse_args()
    db_url = os.environ.get("DATABASE_URL")
    os.environ.pop("DATABASE_URL", None)          # jobs must never write while fetching
    import jobs
    if db_url:
        os.environ["DATABASE_URL"] = db_url
    if a.store:
        fix_store(jobs, a.sleep)
    if a.db:
        fix_db(jobs, a.sleep, a.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
