"""update_fundamentals_revenue.py — refresh fundamentals.revenue / revenue_mom / revenue_yoy / revenue_date from
the monthly revenue store (history/revenue.parquet, fetch_extra.py revenue). Only the revenue columns are
touched and only when the store has a NEWER month than the DB row (or the DB row has none); a missing
fundamentals row is inserted with just the revenue columns.

  python backend/maintenance/update_fundamentals_revenue.py          # dry run
  python backend/maintenance/update_fundamentals_revenue.py --apply
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.dirname(HERE)
sys.path.insert(0, BACKEND)

import pandas as pd  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

from delete_nontrading import connect  # noqa: E402

STORE = os.path.join(os.path.dirname(BACKEND), "history", "revenue.parquet")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if not os.path.exists(STORE):
        print("no revenue store; nothing to do")
        return 0
    rv = pd.read_parquet(STORE)
    rv = rv.sort_values(["stock_id", "ym"]).drop_duplicates("stock_id", keep="last")
    rv["revenue_date"] = rv["ym"].str[:4] + "-" + rv["ym"].str[4:6]
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("SELECT stock_id, revenue_date FROM fundamentals")
        have = dict(cur.fetchall())
    conn.rollback()
    todo = rv[[(have.get(s) or "") < d for s, d in zip(rv["stock_id"], rv["revenue_date"])]]
    print(f"store latest months: {rv['revenue_date'].value_counts().head(3).to_dict()}; "
          f"rows to update/insert: {len(todo):,} (new rows: {int((~todo['stock_id'].isin(have)).sum()):,})")
    if not a.apply or not len(todo):
        print("Dry run only." if not a.apply else "nothing to do")
        return 0

    def f(x):
        return None if pd.isna(x) else float(x)
    rows = [(r.stock_id, None if pd.isna(r.revenue) else int(r.revenue), f(r.mom), f(r.yoy), r.revenue_date)
            for r in todo.itertuples()]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fundamentals (stock_id, revenue, revenue_mom, revenue_yoy, revenue_date, updated_at)
            VALUES %s
            ON CONFLICT (stock_id) DO UPDATE SET revenue = EXCLUDED.revenue, revenue_mom = EXCLUDED.revenue_mom,
                revenue_yoy = EXCLUDED.revenue_yoy, revenue_date = EXCLUDED.revenue_date, updated_at = NOW()
            WHERE fundamentals.revenue_date IS NULL OR fundamentals.revenue_date < EXCLUDED.revenue_date""",
                       rows, template="(%s, %s, %s, %s, %s, NOW())")
    conn.commit()
    conn.close()
    print(f"updated {len(rows):,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
