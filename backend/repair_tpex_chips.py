"""Repair TPEx chip rows in chip_daily (institutional net buys + margin/short balances).

Old writers sent YYYYMMDD to TPEx, which ignores it and returns the latest trading
day, so past dates were stamped with later data; margin also read 前資餘額 (previous
day) and institution_total_buy was always NULL. jobs.py now sends ROC dates.

Usage: python repair_tpex_chips.py --start YYYYMMDD --end YYYYMMDD [--apply]
Dry-run by default. Only UPDATEs EXISTING TPEx rows (stock_id, date) present in the payload,
overwriting all six chip columns; never inserts or deletes. Dates whose payload is for another day are skipped.
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

THROTTLE = 1.5
MAX_CONSEC_FAIL = 5
SAMPLES = ("6488", "8299", "5347")
FIELDS = ("foreign_buy", "investment_trust_buy", "dealer_buy", "institution_total_buy",
          "margin_balance", "short_balance")


def fetch_day(d):
    """Return ({stock_id: {field: value}}, status) where status is ok|mismatch|error."""
    captured = []
    real_save = jobs.save_chip_daily_bulk
    jobs.save_chip_daily_bulk = lambda rows: (captured.extend(rows), len(rows))[1]
    try:
        res = {"errors": []}
        n_insti = jobs.write_tpex_insti_chips(d, res)
        time.sleep(THROTTLE)
        n_margin = jobs.write_tpex_margin_chips(d, res)
    finally:
        jobs.save_chip_daily_bulk = real_save
    if any("server returned data for" in e for e in res["errors"]):
        return None, "mismatch"
    if not n_insti or not n_margin:
        return None, "error: " + "; ".join(res["errors"][:2])
    out = {}
    for sid, dt, doc in captured:
        if dt != d:
            continue
        cur = out.setdefault(sid, {})
        for k in FIELDS:
            if doc.get(k) is not None:
                cur[k] = doc[k]
    return out, "ok"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT date FROM chip_daily WHERE market='TPEx' AND date BETWEEN %s AND %s ORDER BY date DESC",
                (a.start, a.end))
    dates = [r[0] for r in cur.fetchall()]
    print(f"mode={'APPLY' if a.apply else 'DRY-RUN'} dates={len(dates)} ({a.start}..{a.end})", flush=True)

    fails = 0
    tot = {"change": 0, "missing": 0, "mismatch_days": 0}
    t0 = time.time()
    for i, d in enumerate(dates):
        if i:
            time.sleep(THROTTLE)
        data, status = fetch_day(d)
        if status == "mismatch":
            tot["mismatch_days"] += 1
            print(f"{d} payload for another day (non-trading?), skipped", flush=True)
            continue
        if data is None:
            fails += 1
            print(f"{d} FAIL {status[:100]}", flush=True)
            if fails >= MAX_CONSEC_FAIL:
                print("stop: 5 consecutive failures", flush=True)
                break
            continue
        fails = 0
        cur.execute(f"SELECT stock_id, {', '.join(FIELDS)} FROM chip_daily WHERE date=%s AND market='TPEx'", (d,))
        existing = {r[0]: r[1:] for r in cur.fetchall()}
        upd = []
        for sid, v in data.items():
            if sid not in existing:
                continue
            # 舊值來自別天，不可保留：payload 有此股就整組覆寫（payload 缺的欄位設 NULL）
            new = tuple(v.get(k) for k in FIELDS)
            if new != tuple(existing[sid]):
                upd.append((sid, d) + new)
        missing = sum(1 for sid in data if sid not in existing)
        tot["change"] += len(upd)
        tot["missing"] += missing
        samples = " ".join(f"{s}:{existing[s][0]}/{existing[s][4]}->{data[s].get('foreign_buy')}/{data[s].get('margin_balance')}"
                           for s in SAMPLES if s in data and s in existing)
        print(f"{d} fetched={len(data)} existing={len(existing)} to_update={len(upd)} not_in_db={missing} | {samples}",
              flush=True)
        if a.apply and upd:
            sets = ", ".join(f"{k}=v.{k}::bigint" for k in FIELDS)
            execute_values(cur, f"""
                UPDATE chip_daily c SET {sets}, updated_at=NOW()
                FROM (VALUES %s) AS v(sid, dt, {', '.join(FIELDS)})
                WHERE c.stock_id=v.sid AND c.date=v.dt AND c.market='TPEx'""", upd, page_size=500)
            conn.commit()
    print(f"TOTAL to_update={tot['change']} not_in_db={tot['missing']} mismatch_days={tot['mismatch_days']} "
          f"elapsed={time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
