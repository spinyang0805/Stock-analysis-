"""fetch_us_prices.py — daily closes for the US tickers the user has traded (public.us_transactions) + USDTWD,
from each ticker's first trade date to today, into public.us_prices (migration 011). Only held/traded tickers are
downloaded, never a market-wide list. Source: Yahoo Finance chart API (no key). Idempotent upsert.

  python backend/fetch_us_prices.py            # needs DATABASE_URL (env or backend/.env)
  python backend/fetch_us_prices.py --selftest
"""
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests

BACKEND = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BACKEND)
FX = "USDTWD"
YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
SYMBOL = {FX: "TWD=X"}                  # Yahoo: TWD=X = TWD per 1 USD


def parse_chart(j):
    """Yahoo chart JSON → [(date 'YYYY-MM-DD', close)] in the exchange's local date, closes that are null dropped."""
    res = (j.get("chart") or {}).get("result") or []
    if not res:
        return []
    r = res[0]
    off = int((r.get("meta") or {}).get("gmtoffset") or 0)
    ts = r.get("timestamp") or []
    closes = (((r.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or []
    out = {}
    for t, c in zip(ts, closes):
        if c is None:
            continue
        d = datetime.fromtimestamp(t + off, tz=timezone.utc).strftime("%Y-%m-%d")
        out[d] = round(float(c), 6)
    return sorted(out.items())


def fetch(ticker, start):
    p1 = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    p2 = int(time.time()) + 86400
    url = YAHOO.format(sym=SYMBOL.get(ticker, ticker))
    for attempt in range(3):
        try:
            r = requests.get(url, params={"period1": p1, "period2": p2, "interval": "1d"},
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            if r.status_code == 200:
                return parse_chart(r.json())
            print(f"[us_prices] {ticker}: HTTP {r.status_code}")
        except Exception as exc:
            print(f"[us_prices] {ticker}: {type(exc).__name__}: {str(exc)[:120]}")
        time.sleep(3 * (attempt + 1))
    return None


def main():
    from tuning.private_store import _connect
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("select upper(ticker), min(trade_date) from public.us_transactions group by 1")
            wanted = {t: d for t, d in cur.fetchall()}
        if not wanted:
            print("[us_prices] no US transactions — nothing to fetch")
            return 0
        wanted[FX] = min(wanted.values())
        start_from = {t: d - timedelta(days=7) for t, d in wanted.items()}
        failed = []
        for t, start in sorted(start_from.items()):
            rows = fetch(t, start)
            if rows is None:
                failed.append(t)
                continue
            with conn.cursor() as cur:
                cur.executemany(
                    "insert into public.us_prices (ticker, date, close) values (%s, %s, %s) "
                    "on conflict (ticker, date) do update set close = excluded.close, updated_at = now()",
                    [(t, d, c) for d, c in rows])
            conn.commit()
            print(f"[us_prices] {t}: {len(rows)} days from {start} (last {rows[-1][0] if rows else '-'})")
            time.sleep(1)
        if failed:
            print("[us_prices] FAILED:", ", ".join(failed))
            return 1
        return 0
    finally:
        conn.close()


def selftest():
    j = {"chart": {"result": [{"meta": {"gmtoffset": -14400},
                               "timestamp": [1792440001, 1792526401, 1792612801],
                               "indicators": {"quote": [{"close": [700.5, None, 701.25]}]}}]}}
    rows = parse_chart(j)
    assert len(rows) == 2 and rows[0][1] == 700.5 and rows[1][1] == 701.25, rows
    assert parse_chart({"chart": {"result": None}}) == []
    print("fetch_us_prices selftest OK")
    return 0


if __name__ == "__main__":
    sys.exit(selftest() if "--selftest" in sys.argv else main())
