"""Delete warrant (權證/牛熊證) rows listed in warrant_ids.txt.

Default = dry run (counts only, READ ONLY transaction).
--apply  = delete in batches of 200 ids/statement, commit per batch, then VACUUM (ANALYZE).
Never uses VACUUM FULL (needs extra disk; DB is near the 500 MB quota).
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import psycopg2  # noqa: E402
from export_static_json import load_env_file  # noqa: E402
from instrument_filter import is_warrant  # noqa: E402

IDS_FILE = os.path.join(HERE, "warrant_ids.txt")
BATCH = 200
# (table, id column). Order: big tables first.
TARGETS = [
    ("stock_daily", "stock_id"),
    ("chip_daily", "stock_id"),
    ("analysis_cache", "stock_id"),
    ("fundamentals", "stock_id"),
    ("financials_yearly", "stock_id"),
    ("heal_blacklist", "stock_id"),
    ("product_universe", "code"),
]
# User-owned tables: any id present here is never deleted.
USER_TABLES = [
    ("transactions", "stock_id"),
    ("watchlist", "stock_id"),
    ("track_requests", "code"),
    ("holding_settings", "stock_id"),
]


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
        cur.execute("SET statement_timeout = '30min'")
        cur.execute("SET lock_timeout = '60s'")
    conn.commit()
    return conn


def db_size(cur):
    cur.execute("SELECT pg_database_size(current_database())")
    return cur.fetchone()[0] / 1024 / 1024


def load_ids():
    with open(IDS_FILE, encoding="utf-8") as f:
        ids = sorted({ln.strip() for ln in f if ln.strip()})
    bad = [i for i in ids if not is_warrant(i)]
    if bad:
        raise SystemExit(f"{len(bad)} ids in file fail is_warrant() (e.g. {bad[:5]}); abort")
    return ids


def user_ids(cur, ids):
    found = set()
    for t, col in USER_TABLES:
        cur.execute(f"SELECT DISTINCT {col} FROM {t} WHERE {col} = ANY(%s)", (ids,))
        found.update(r[0] for r in cur.fetchall())
    return found


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="really delete (default: dry run)")
    args = ap.parse_args()

    ids = load_ids()
    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()
    protected = user_ids(cur, ids)
    conn.commit()
    if protected:
        print(f"Excluded (present in user tables): {sorted(protected)}")
    ids = [i for i in ids if i not in protected]
    print(f"ids to process: {len(ids)}  mode={'APPLY' if args.apply else 'DRY RUN'}")
    before = db_size(cur)
    print(f"pg_database_size before: {before:.1f} MB")
    conn.commit()

    totals = {}
    if not args.apply:
        conn.rollback()
        conn.set_session(readonly=True)
        cur = conn.cursor()
        for t, col in TARGETS:
            n = 0
            for b in chunks(ids, BATCH):
                cur.execute(f"SELECT count(*) FROM {t} WHERE {col} = ANY(%s)", (b,))
                n += cur.fetchone()[0]
            totals[t] = n
            print(f"  {t}: {n} rows would be deleted")
        conn.rollback()
        conn.close()
        print("Dry run only. Re-run with --apply to delete.")
        return

    for t, col in TARGETS:
        n = 0
        for b in chunks(ids, BATCH):
            # SET LOCAL：transaction pooler 下 session 級設定不一定保留
            cur.execute("SET LOCAL statement_timeout = '5min'")
            cur.execute(f"DELETE FROM {t} WHERE {col} = ANY(%s)", (b,))
            n += cur.rowcount
            conn.commit()
        totals[t] = n
        print(f"  {t}: {n} rows deleted", flush=True)
    conn.close()

    # VACUUM must run outside a transaction block, on a session-mode connection.
    vconn = connect(session=True)
    vconn.autocommit = True
    vcur = vconn.cursor()
    for t, _ in TARGETS:
        vcur.execute(f"VACUUM (ANALYZE) {t}")
        print(f"  VACUUM (ANALYZE) {t} done", flush=True)
    after = db_size(vcur)
    vconn.close()
    print(f"pg_database_size after: {after:.1f} MB (before {before:.1f} MB)")


if __name__ == "__main__":
    main()
