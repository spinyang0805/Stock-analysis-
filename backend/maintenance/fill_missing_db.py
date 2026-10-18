"""fill_missing_db.py — insert official price rows that the Parquet history store has but Supabase lacks.

INSERT only (ON CONFLICT DO NOTHING): existing DB rows are never modified. Limited to dates >= --since
(default: the DB's own earliest date in the retained window) so the DB retention window is respected.

  python backend/maintenance/fill_missing_db.py              # dry run: counts per month
  python backend/maintenance/fill_missing_db.py --apply
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.dirname(HERE)
sys.path.insert(0, BACKEND)

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

from delete_nontrading import connect  # noqa: E402

HISTORY = os.path.join(os.path.dirname(BACKEND), "history")
COLS = ["stock_id", "date", "open", "high", "low", "close", "volume", "turnover", "change", "trades",
        "market", "product_type", "name", "source"]
BATCH = 2000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="20250601")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    glob = os.path.join(HISTORY, "prices_*.parquet").replace("\\", "/")
    st = duckdb.sql(f"""SELECT {', '.join(COLS)} FROM read_parquet('{glob}', union_by_name=true)
                        WHERE date >= '{a.since}' AND source NOT ILIKE 'yfinance%'""").df()
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("SELECT stock_id, date FROM stock_daily WHERE date >= %s", (a.since,))
        have = set(cur.fetchall())
    conn.rollback()
    miss = st[[(s, d) not in have for s, d in zip(st["stock_id"], st["date"])]]
    print(f"store rows since {a.since}: {len(st):,}; missing in DB: {len(miss):,}")
    print(miss.groupby(miss["date"].str[:6]).size().to_string())
    if not a.apply:
        print("\nDry run only. Re-run with --apply to insert.")
        return 0
    rows = [tuple(None if pd.isna(v) else v for v in r) for r in miss[COLS].itertuples(index=False)]
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH):
            execute_values(cur, f"INSERT INTO stock_daily ({', '.join(COLS)}) VALUES %s "
                                "ON CONFLICT (stock_id, date) DO NOTHING", rows[i:i + BATCH])
            conn.commit()
            print(f"  inserted {min(i + BATCH, len(rows)):,}/{len(rows):,}")
    conn.close()
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
