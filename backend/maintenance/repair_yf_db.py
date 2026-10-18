"""repair_yf_db.py — replace yfinance-sourced stock_daily rows in Supabase with official exchange prices.

The Parquet history store has already been repaired (history_store.py backfill --replace-yf): every date
that had yfinance rows was re-fetched from TWSE MI_INDEX + TPEx otc. This script uses that store as the
reference:

  UPDATE  DB yfinance row whose (stock_id, date) has an official row in the store → official OHLCV values
  DELETE  DB yfinance row inside the stock's official listing period (first..last official date in the store)
          on a date the store covers, but with no official row that day (suspended / no trade; Yahoo
          carried a stale bar)
  KEEP    rows outside the official listing period (e.g. 興櫃 trading before a TPEx listing — real prices
          the main-board daily quotes do not include) and dates the store has no official data for

Default is a dry run that prints counts and samples. --apply writes in batches (commit per batch).
Requires: python backend/history_store.py pull (store up to date) — refuses if the store still has yfinance rows.

  python backend/maintenance/repair_yf_db.py            # dry run
  python backend/maintenance/repair_yf_db.py --apply
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.dirname(HERE)
sys.path.insert(0, BACKEND)

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
import psycopg2  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

from delete_nontrading import connect  # noqa: E402

HISTORY = os.path.join(os.path.dirname(BACKEND), "history")
COLS = ["open", "high", "low", "close", "volume", "turnover", "change", "trades"]
BATCH = 2000


def load_store():
    glob = os.path.join(HISTORY, "prices_*.parquet").replace("\\", "/")
    con = duckdb.connect()
    con.execute(f"CREATE VIEW p AS SELECT * FROM read_parquet('{glob}', union_by_name=true)")
    left = con.execute("SELECT count(*) FROM p WHERE source ILIKE 'yfinance%'").fetchone()[0]
    if left:
        raise SystemExit(f"store still has {left} yfinance rows — run history_store backfill --replace-yf first")
    off = con.execute(f"SELECT stock_id, date, market, {', '.join(COLS)}, source FROM p").df()
    dup = off.duplicated(["stock_id", "date"]).sum()
    if dup:
        raise SystemExit(f"store has {dup} duplicate (stock_id, date) rows — fix the store first")
    # coverage per MARKET: a date counts as officially covered for TWSE / TPEx only if that market's
    # full-day table is there (a failed TPEx fetch must not make every OTC stock look 'not traded')
    cov = con.execute("""SELECT date, market, count(*) n FROM p GROUP BY date, market""").df()
    med = cov.groupby("market")["n"].transform("median")
    ok = cov[cov["n"] >= 0.6 * med]
    covered = set(zip(ok["date"], ok["market"]))
    return off, covered


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="really write (default: dry run)")
    args = ap.parse_args()

    off, covered = load_store()
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("SELECT stock_id, date, close, volume FROM stock_daily WHERE source ILIKE 'yfinance%%'")
        db = pd.DataFrame(cur.fetchall(), columns=["stock_id", "date", "close_db", "volume_db"])
    conn.rollback()
    m = db.merge(off.assign(_hit=True), on=["stock_id", "date"], how="left")
    span = off.groupby("stock_id").agg(min=("date", "min"), max=("date", "max"), mk=("market", "last"))
    m = m.join(span, on="stock_id")
    listed = m["min"].notna() & (m["date"] >= m["min"]) & (m["date"] <= m["max"])
    covered_mk = pd.Series([(d, k) in covered for d, k in zip(m["date"], m["mk"])], index=m.index)
    hit = m["_hit"].fillna(False).astype(bool)
    m["action"] = "KEEP"
    m.loc[hit & m["close"].notna(), "action"] = "UPDATE"
    m.loc[~hit & covered_mk & listed, "action"] = "DELETE"
    upd, dele, keep = (m[m["action"] == a] for a in ("UPDATE", "DELETE", "KEEP"))
    print(f"DB yfinance rows: {len(db):,}")
    print(f"  UPDATE → official: {len(upd):,}")
    print(f"  DELETE (inside official listing period, no trade that day): {len(dele):,} "
          f"(volume 0: {int((dele['volume_db'].fillna(0) == 0).sum()):,})")
    print(f"  KEEP (outside official listing period, e.g. 興櫃, or date not covered): {len(keep):,}")
    by_month = m.assign(ym=m["date"].str[:6])
    print(by_month.pivot_table(index="ym", columns="action", values="stock_id", aggfunc="count", fill_value=0)
          .to_string())
    if len(upd):
        diff = (upd["close_db"] / upd["close"] - 1).abs()
        print(f"  close differs >1% in {int((diff > 0.01).sum()):,} of the UPDATE rows "
              f"(yfinance adjusted prices); sample:")
        print(upd.assign(diff_pct=(diff * 100).round(2)).sort_values("diff_pct", ascending=False)
              [["stock_id", "date", "close_db", "close", "diff_pct"]].head(8).to_string(index=False))
    if len(dele):
        print("  DELETE sample:")
        print(dele[["stock_id", "date", "close_db", "volume_db"]].head(8).to_string(index=False))
    if not args.apply:
        print("\nDry run only. Re-run with --apply to write.")
        return 0

    rows = [tuple([r.stock_id, r.date] + [None if pd.isna(getattr(r, c)) else float(getattr(r, c)) for c in COLS]
                  + [r.source]) for r in upd.itertuples()]
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH):
            execute_values(cur, f"""
                UPDATE stock_daily AS t SET {', '.join(f'{c} = v.{c}::float' for c in COLS)},
                       source = v.source, updated_at = NOW()
                FROM (VALUES %s) AS v(stock_id, date, {', '.join(COLS)}, source)
                WHERE t.stock_id = v.stock_id AND t.date = v.date AND t.source ILIKE 'yfinance%%'""",
                           rows[i:i + BATCH])
            conn.commit()
            print(f"  updated {min(i + BATCH, len(rows)):,}/{len(rows):,}")
        keys = list(dele[["stock_id", "date"]].itertuples(index=False, name=None))
        for i in range(0, len(keys), BATCH):
            execute_values(cur, """
                DELETE FROM stock_daily AS t USING (VALUES %s) AS v(stock_id, date)
                WHERE t.stock_id = v.stock_id AND t.date = v.date AND t.source ILIKE 'yfinance%%'""",
                           keys[i:i + BATCH])
            conn.commit()
            print(f"  deleted {min(i + BATCH, len(keys)):,}/{len(keys):,}")
    conn.close()
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
