"""fetch_us_prices.py — daily closes for the US tickers the user has traded (public.us_transactions) + USDTWD,
from each ticker's first trade date to today, into public.us_prices (migration 011). Only held/traded tickers are
downloaded, never a market-wide list. Stock closes: Yahoo Finance chart API (no key). USDTWD: Bank of Taiwan
posted rate (臺灣銀行牌告, 即期買入 = what the bank pays per USD, i.e. the TWD value of USD you hold), from the
official CSV rate.bot.com.tw/xrt/flcsv/0/L6M/USD (last 6 months). If that site refuses (it sometimes answers with a
bot-check page) the run falls back to Yahoo TWD=X and stores source='yahoo' so the site can label it. Idempotent upsert.

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
SYMBOL = {FX: "TWD=X"}                  # Yahoo: TWD=X = TWD per 1 USD (fallback only)
BOT_CSV = "https://rate.bot.com.tw/xrt/flcsv/0/L6M/USD"


def parse_bot_csv(text):
    """臺灣銀行牌告 CSV → [(date 'YYYY-MM-DD', 即期買入)]. Header: 資料日期,幣別,匯率,現金,即期,…,匯率,現金,即期,…;
    the first 即期 column is 本行買入. Rows with no spot rate (0 / blank) are dropped."""
    import csv
    import io
    rows = list(csv.reader(io.StringIO(text.lstrip("﻿"))))
    if not rows:
        return []
    head = [h.strip() for h in rows[0]]
    if "即期" not in head or "資料日期" not in head:
        return []
    di, si = head.index("資料日期"), head.index("即期")
    out = {}
    for r in rows[1:]:
        if len(r) <= max(di, si):
            continue
        d = r[di].strip().replace("/", "")
        try:
            v = float(r[si])
        except ValueError:
            continue
        if len(d) == 8 and d.isdigit() and v > 0:
            out[f"{d[:4]}-{d[4:6]}-{d[6:]}"] = v
    return sorted(out.items())


def fetch_bot_via_db(conn):
    """Same CSV fetched by the database server (public._market_get, migration 001) — a different IP than the runner,
    used when the runner gets the bot-check page."""
    try:
        with conn.cursor() as cur:
            cur.execute("select status, body from public._market_get(%s, 20000)", (BOT_CSV,))
            status, body = cur.fetchone()
        conn.commit()
        rows = parse_bot_csv(body or "") if status == 200 else []
        if rows:
            return rows
        print(f"[us_prices] Bank of Taiwan CSV via DB unusable (HTTP {status}, starts {(body or '')[:60]!r})")
    except Exception as exc:
        conn.rollback()
        print(f"[us_prices] Bank of Taiwan CSV via DB: {type(exc).__name__}: {str(exc)[:160]}")
    return None


def fetch_bot():
    try:
        r = requests.get(BOT_CSV, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        rows = parse_bot_csv(r.content.decode("utf-8-sig", "replace")) if r.status_code == 200 else []
        if rows:
            return rows
        print(f"[us_prices] Bank of Taiwan CSV unusable (HTTP {r.status_code}, {r.headers.get('content-type')})")
    except Exception as exc:
        print(f"[us_prices] Bank of Taiwan CSV: {type(exc).__name__}: {str(exc)[:120]}")
    return None


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
            src = "yahoo"
            if t == FX:
                rows = fetch_bot() or fetch_bot_via_db(conn)
                if rows:
                    src = "bot"
                else:
                    print("[us_prices] USDTWD: falling back to Yahoo TWD=X (labelled source=yahoo)")
                    rows = fetch(t, start)
            else:
                rows = fetch(t, start)
            if rows is None:
                failed.append(t)
                continue
            with conn.cursor() as cur:
                # a Bank of Taiwan rate is never overwritten by a Yahoo fallback value
                cur.executemany(
                    "insert into public.us_prices (ticker, date, close, source) values (%s, %s, %s, %s) "
                    "on conflict (ticker, date) do update set close = excluded.close, source = excluded.source, "
                    "updated_at = now() where not (us_prices.source = 'bot' and excluded.source <> 'bot')",
                    [(t, d, c, src) for d, c in rows])
            conn.commit()
            print(f"[us_prices] {t} ({src}): {len(rows)} days from {start} (last {rows[-1] if rows else '-'})")
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
    csv_text = ("﻿資料日期,幣別,匯率,現金,即期,遠期10天,匯率,現金,即期,遠期10天\n"
                "20261020,USD,本行買入,32.06,32.385,32.3,本行賣出,32.73,32.485,32.4\n"
                "20261017,USD,本行買入,31.98,32.31,32.2,本行賣出,32.65,32.41,32.3\n")
    assert parse_bot_csv(csv_text) == [("2026-10-17", 32.31), ("2026-10-20", 32.385)], parse_bot_csv(csv_text)
    assert parse_bot_csv("<!DOCTYPE html><title>Challenge Validation</title>") == []
    print("fetch_us_prices selftest OK")
    return 0


if __name__ == "__main__":
    sys.exit(selftest() if "--selftest" in sys.argv else main())
