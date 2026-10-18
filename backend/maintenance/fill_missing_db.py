"""fill_missing_db.py — insert rows that the Parquet history store has but Supabase lacks.

INSERT only (ON CONFLICT DO NOTHING): existing DB rows are never modified. Limited to dates >= --since
(default: the DB retention cutoff, 13 months, same rule as history_store prune) so pruned history is not
re-inserted.

  python backend/maintenance/fill_missing_db.py [--kind prices|chips|both]            # dry run
  python backend/maintenance/fill_missing_db.py --kind both --apply
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
SPEC = {
    "prices": ("stock_daily", "prices", ["stock_id", "date", "open", "high", "low", "close", "volume", "turnover",
                                         "change", "trades", "market", "product_type", "name", "source"],
               "source NOT ILIKE 'yfinance%'"),
    "chips": ("chip_daily", "chips", ["stock_id", "date", "name", "market", "foreign_buy", "investment_trust_buy",
                                      "dealer_buy", "institution_total_buy", "margin_balance", "short_balance",
                                      "source"], "TRUE"),
}
BATCH = 2000
INT_COLS = {"foreign_buy", "investment_trust_buy", "dealer_buy", "institution_total_buy", "margin_balance",
            "short_balance"}


def default_since():
    sys.path.insert(0, BACKEND)
    from history_store import months_cutoff
    return months_cutoff(13)


def run_kind(kind, since, apply, conn):
    table, prefix, cols, where = SPEC[kind]
    glob = os.path.join(HISTORY, f"{prefix}_*.parquet").replace("\\", "/")
    have_cols = set(duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name=true)").df()["column_name"])
    use = [c for c in cols if c in have_cols]
    st = duckdb.sql(f"""SELECT {', '.join(use)} FROM read_parquet('{glob}', union_by_name=true)
                        WHERE date >= '{since}' AND {where}""").df()
    with conn.cursor() as cur:
        cur.execute(f"SELECT stock_id, date FROM {table} WHERE date >= %s", (since,))
        have = set(cur.fetchall())
    conn.rollback()
    miss = st[[(s, d) not in have for s, d in zip(st["stock_id"], st["date"])]]
    print(f"[{table}] store rows since {since}: {len(st):,}; missing in DB: {len(miss):,}")
    if len(miss):
        by = miss.groupby([miss["date"].str[:6], miss.get("market", pd.Series("?", index=miss.index))]).size()
        print(by.unstack(fill_value=0).to_string())
    if not apply or not len(miss):
        return len(miss)

    def val(c, v):
        if pd.isna(v):
            return None
        return int(v) if c in INT_COLS else v
    rows = [tuple(val(c, v) for c, v in zip(use, r)) for r in miss[use].itertuples(index=False)]
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH):
            execute_values(cur, f"INSERT INTO {table} ({', '.join(use)}) VALUES %s "
                                "ON CONFLICT (stock_id, date) DO NOTHING", rows[i:i + BATCH])
            conn.commit()
            print(f"  inserted {min(i + BATCH, len(rows)):,}/{len(rows):,}")
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=["prices", "chips", "both"], default="both")
    ap.add_argument("--since", default=None)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    since = a.since or default_since()
    conn = connect()
    for k in (["prices", "chips"] if a.kind == "both" else [a.kind]):
        run_kind(k, since, a.apply, conn)
    conn.close()
    if not a.apply:
        print("\nDry run only. Re-run with --apply to insert.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
