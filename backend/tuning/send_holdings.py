"""send_holdings.py — e-mail / Teams the latest private holdings advice from Supabase private_store now.

  python backend/tuning/send_holdings.py [--date YYYYMMDD] [--channels email,teams]
Used by the "Holdings advice (send now)" workflow; the daily 08:50 report already includes it.
"""
import argparse
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
if __package__ in (None, ""):
    __package__ = "tuning"

import notify  # noqa: E402
from tuning import private_store  # noqa: E402


def latest_date():
    c = private_store._connect()
    try:
        with c.cursor() as cur:
            cur.execute("SELECT max(substring(key from 10)) FROM public.private_store WHERE key LIKE 'holdings:%'")
            r = cur.fetchone()
        c.rollback()
        return r[0] if r else None
    finally:
        c.close()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--channels", default="email,teams")
    a = ap.parse_args(argv)
    notify.load_env()
    d = a.date or latest_date()
    body = private_store.get(f"holdings:{d}") if d else None
    if not body:
        print("no holdings advice stored")
        return 1
    res = notify.notify(f"我的持股建議 {d[:4]}/{d[4:6]}/{d[6:]}", body, channels=a.channels.split(","))
    print(res)
    return 0 if any(v == "ok" for v in res.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
