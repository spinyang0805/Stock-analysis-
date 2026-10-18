"""Repair TWSE margin_balance / short_balance in chip_daily (units: 張).

Usage: python repair_margin.py --start YYYYMMDD --end YYYYMMDD [--apply]
Dry-run by default. Only UPDATEs margin_balance/short_balance of EXISTING rows
(stock_id, date); stocks missing from chip_daily are counted, never inserted.
Dates iterated = distinct TWSE dates present in chip_daily within the range.
"""
import argparse
import os
import sys
import time

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
load_dotenv(os.path.join(HERE, ".env"))

import jobs  # noqa: E402

THROTTLE = 1.6
MAX_CONSEC_FAIL = 5
SAMPLES = ("2330", "2317", "2454")


def fetch_day(d):
    """Return ({stock_id: (margin, short)}, error). Retries with backoff."""
    last = None
    for attempt in range(4):
        payload, err = jobs.fetch_json(
            jobs.TWSE_MARGIN, params={"response": "json", "date": d, "selectType": "ALL"}, retries=0)
        if isinstance(payload, dict) and payload:
            rows = jobs.twse_margin_summary_rows(payload)
            out = {}
            for r in rows:
                if len(r) > 12 and str(r[0]).strip():
                    out[str(r[0]).strip()] = (jobs.safe_int(r[6]), jobs.safe_int(r[12]))
            return out, None
        last = err or "empty payload"
        time.sleep(3 * (2 ** attempt))
    return None, last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT date FROM chip_daily WHERE market='TWSE' AND date BETWEEN %s AND %s ORDER BY date",
                (a.start, a.end))
    dates = [r[0] for r in cur.fetchall()]
    print(f"mode={'APPLY' if a.apply else 'DRY-RUN'} dates={len(dates)} ({a.start}..{a.end})", flush=True)

    fails = 0
    tot = {"change": 0, "missing": 0, "skipped_days": 0}
    t0 = time.time()
    for i, d in enumerate(dates):
        if i:
            time.sleep(THROTTLE)
        data, err = fetch_day(d)
        if data is None:
            fails += 1
            print(f"{d} FAIL {str(err)[:80]}", flush=True)
            if fails >= MAX_CONSEC_FAIL:
                print("stop: 5 consecutive failures", flush=True)
                break
            continue
        fails = 0
        if not data:
            tot["skipped_days"] += 1
            print(f"{d} no margin data (skipped)", flush=True)
            continue
        cur.execute("SELECT stock_id, margin_balance, short_balance FROM chip_daily WHERE date=%s AND market='TWSE'", (d,))
        existing = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        upd = [(sid, d, v[0], v[1]) for sid, v in data.items()
               if sid in existing and (v[0], v[1]) != existing[sid] and (v[0] is not None or v[1] is not None)]
        missing = sum(1 for sid in data if sid not in existing)
        tot["change"] += len(upd)
        tot["missing"] += missing
        samples = " ".join(f"{s}:{existing.get(s)}->{data.get(s)}" for s in SAMPLES if s in data)
        print(f"{d} fetched={len(data)} existing={len(existing)} to_update={len(upd)} not_in_db={missing} | {samples}", flush=True)
        if a.apply and upd:
            execute_values(cur, """
                UPDATE chip_daily c SET margin_balance=COALESCE(v.m::bigint, c.margin_balance),
                                        short_balance=COALESCE(v.s::bigint, c.short_balance), updated_at=NOW()
                FROM (VALUES %s) AS v(sid, dt, m, s)
                WHERE c.stock_id=v.sid AND c.date=v.dt""", upd, page_size=500)
            conn.commit()
    print(f"TOTAL to_update={tot['change']} not_in_db={tot['missing']} empty_days={tot['skipped_days']} "
          f"elapsed={time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
