#!/usr/bin/env python3
"""
local_chip_backfill_fast.py — 批次寫入版本，速度比原版快 100x
=================================================================
原版問題：每一行各自 commit（15000 支 × 528 天 = 792 萬次 transaction）
本版修法：每天一次批次 executemany，commit 1 次
=================================================================
使用方式:
  python local_chip_backfill_fast.py            # 補 2 年
  python local_chip_backfill_fast.py --months 12
  python local_chip_backfill_fast.py --start-date 20240101
=================================================================
"""
import argparse, os, sys, time, requests
from datetime import datetime, timedelta
from typing import Optional

def _load_dotenv():
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_file):
        return
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

_load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("[ERR] DATABASE_URL not set")
    sys.exit(1)

import psycopg2
from psycopg2.extras import execute_values

# ── API endpoints ──────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9",
    "Referer": "https://www.twse.com.tw/zh/trading/foreign/t86.html",
    "Origin": "https://www.twse.com.tw",
}
TPEX_HEADERS = {
    **HEADERS,
    "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/detail/day.html",
    "Origin": "https://www.tpex.org.tw",
}
TWSE_T86    = "https://www.twse.com.tw/rwd/zh/fund/T86"
TWSE_MARGIN = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TPEX_INSTI  = "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"
TPEX_MARGIN = "https://www.tpex.org.tw/www/zh-tw/margin/balance"
TIMEOUT = 20


def _get(url, params, headers=None, retries=2):
    h = headers or HEADERS
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            if attempt == retries:
                return None
            time.sleep(1.5 * (attempt + 1))
    return None


def _safe_int(v):
    if v in (None, "", "-", "--", "N/A"):
        return None
    try:
        return int(str(v).replace(",", "").strip())
    except Exception:
        return None


# ── Fetch functions — return dict {stock_id: row_dict} ────────
def fetch_twse_t86(date_str: str) -> Optional[dict]:
    """TWSE T86: 三大法人. Returns {code: {foreign, trust, dealer, total}} or None."""
    data = _get(TWSE_T86, {"response": "json", "date": date_str, "selectType": "ALL"})
    if not data:
        return None
    rows = data.get("data") or data.get("aaData", [])
    fields = data.get("fields") or data.get("head", [])
    if not rows:
        return None

    def _idx(*keys, default=0):
        for key in keys:
            for i, f in enumerate(fields):
                if key in str(f):
                    return i
        return default

    code_i    = _idx("代號", "Code", default=0)
    name_i    = _idx("名稱", "Name", default=1)
    foreign_i = _idx("外陸資", default=4)
    trust_i   = _idx("投信", default=10)
    dealer_i  = 11  # 自營商買賣超股數 (unique)
    total_i   = _idx("三大法人", default=18)

    result = {}
    for row in rows:
        code = str(row[code_i]).strip() if row else ""
        if not code or len(code) > 10:
            continue
        result[code] = {
            "name": str(row[name_i]).strip(),
            "market": "TWSE",
            "foreign_buy": _safe_int(row[foreign_i]),
            "investment_trust_buy": _safe_int(row[trust_i]),
            "dealer_buy": _safe_int(row[dealer_i]) if len(row) > dealer_i else None,
            "institution_total_buy": _safe_int(row[total_i]) if len(row) > total_i else None,
        }
    return result if result else None


def _tables_data(data: dict, table_idx: int = 0):
    """Extract rows and fields from TWSE/TPEx tables-style response."""
    tables = data.get("tables", [])
    if isinstance(tables, list) and len(tables) > table_idx:
        t = tables[table_idx]
        return t.get("data", []) or [], t.get("fields", []) or []
    rows = data.get("data") or data.get("aaData") or []
    fields = data.get("fields") or data.get("head") or []
    return rows, fields


def fetch_twse_margin(date_str: str) -> Optional[dict]:
    """TWSE margin: 融資融券. tables[1] has per-stock data; margin@6, short@12."""
    data = _get(TWSE_MARGIN, {"response": "json", "date": date_str, "selectType": "ALL"})
    if not data:
        return None
    rows, _ = _tables_data(data, table_idx=1)   # table[1] = per-stock data
    if not rows:
        return None
    result = {}
    for row in rows:
        try:
            code = str(row[0]).strip()
            if not code or len(code) > 10:
                continue
            result[code] = {
                "market": "TWSE",
                "margin_balance": _safe_int(row[6]) if len(row) > 6 else None,
                "short_balance":  _safe_int(row[12]) if len(row) > 12 else None,
            }
        except Exception:
            continue
    return result if result else None


def fetch_tpex_insti(date_str: str) -> Optional[dict]:
    """TPEx institutional. YYYYMMDD format. tables[0]. Uses _parse_tpex_insti_row logic."""
    data = _get(TPEX_INSTI, {"response": "json", "date": date_str, "sect": "AL", "type": "Daily"}, headers=TPEX_HEADERS)
    if not data:
        return None
    rows, _ = _tables_data(data, table_idx=0)
    if not rows:
        return None
    result = {}
    for row in rows:
        try:
            parts = [str(x) for x in row] if isinstance(row, list) else row.split()
            if len(parts) < 24:
                continue
            code = parts[0].strip()
            name = parts[1].strip()
            values = parts[2:]   # values[N] = row[N+2]
            result[code] = {
                "name": name,
                "market": "TPEx",
                "foreign_buy":            _safe_int(values[8])  if len(values) > 8  else None,
                "investment_trust_buy":   _safe_int(values[11]) if len(values) > 11 else None,
                "dealer_buy":             _safe_int(values[20]) if len(values) > 20 else None,
                "institution_total_buy":  _safe_int(values[23]) if len(values) > 23 else None,
            }
        except Exception:
            continue
    return result if result else None


def fetch_tpex_margin(date_str: str) -> Optional[dict]:
    """TPEx margin. YYYYMMDD format. tables[0]. margin@6, short@14."""
    data = _get(TPEX_MARGIN, {"response": "json", "date": date_str}, headers=TPEX_HEADERS)
    if not data:
        return None
    rows, _ = _tables_data(data, table_idx=0)
    if not rows:
        return None
    result = {}
    for row in rows:
        try:
            code = str(row[0]).strip()
            if not code or len(code) > 10:
                continue
            result[code] = {
                "market": "TPEx",
                "margin_balance": _safe_int(row[6])  if len(row) > 6  else None,
                "short_balance":  _safe_int(row[14]) if len(row) > 14 else None,
            }
        except Exception:
            continue
    return result if result else None


# ── Batch DB write ─────────────────────────────────────────────
INSERT_SQL = """
    INSERT INTO chip_daily
        (stock_id, date, name, market,
         foreign_buy, investment_trust_buy, dealer_buy, institution_total_buy,
         margin_balance, short_balance, source, chip_date)
    VALUES %s
    ON CONFLICT (stock_id, date) DO UPDATE SET
        name=COALESCE(EXCLUDED.name, chip_daily.name),
        market=COALESCE(EXCLUDED.market, chip_daily.market),
        foreign_buy=COALESCE(EXCLUDED.foreign_buy, chip_daily.foreign_buy),
        investment_trust_buy=COALESCE(EXCLUDED.investment_trust_buy, chip_daily.investment_trust_buy),
        dealer_buy=COALESCE(EXCLUDED.dealer_buy, chip_daily.dealer_buy),
        institution_total_buy=COALESCE(EXCLUDED.institution_total_buy, chip_daily.institution_total_buy),
        margin_balance=COALESCE(EXCLUDED.margin_balance, chip_daily.margin_balance),
        short_balance=COALESCE(EXCLUDED.short_balance, chip_daily.short_balance),
        source=EXCLUDED.source, updated_at=NOW()
"""

_ALLOWED_CODES: set = set()  # loaded once from DB

def load_allowed_codes(conn):
    """Load product_universe codes. Called once at startup."""
    global _ALLOWED_CODES
    with conn.cursor() as cur:
        cur.execute("SELECT code FROM product_universe")
        _ALLOWED_CODES = {r[0] for r in cur.fetchall()}
    print(f"  Loaded {len(_ALLOWED_CODES)} product codes from DB")


def batch_write(conn, date_str: str, t86: dict, margin: dict, tpex_t: dict, tpex_m: dict) -> int:
    all_codes = set()
    for d in (t86, margin, tpex_t, tpex_m):
        if d:
            all_codes.update(d.keys())
    # Filter: keep only product_universe codes or codes starting with "00" (ETFs).
    # This excludes warrants (6-digit not starting "00") and CBBCs.
    if _ALLOWED_CODES:
        all_codes = {c for c in all_codes if c in _ALLOWED_CODES or c.startswith("00")}

    rows = []
    for code in all_codes:
        r_t  = (t86 or {}).get(code, {})
        r_m  = (margin or {}).get(code, {})
        r_ti = (tpex_t or {}).get(code, {})
        r_tm = (tpex_m or {}).get(code, {})
        name   = r_t.get("name") or r_m.get("name") or r_ti.get("name") or code
        market = r_t.get("market") or r_ti.get("market") or "TWSE"
        rows.append((
            code, date_str, name, market,
            r_t.get("foreign_buy") or r_ti.get("foreign_buy"),
            r_t.get("investment_trust_buy") or r_ti.get("investment_trust_buy"),
            r_t.get("dealer_buy") or r_ti.get("dealer_buy"),
            r_t.get("institution_total_buy") or r_ti.get("institution_total_buy"),
            r_m.get("margin_balance") or r_tm.get("margin_balance"),
            r_m.get("short_balance") or r_tm.get("short_balance"),
            "local_backfill", date_str,
        ))

    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows, page_size=500)
    conn.commit()
    return len(rows)


# ── Trading date helpers ───────────────────────────────────────
def trading_dates_between(start: str, end: str) -> list:
    """Generate weekday dates from start to end (YYYYMMDD), newest first."""
    s = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    dates = []
    cur = e
    while cur >= s:
        if cur.weekday() < 5:
            dates.append(cur.strftime("%Y%m%d"))
        cur -= timedelta(days=1)
    return dates  # newest first


def find_valid_date(target: str, days_back: int = 7) -> Optional[str]:
    """Try target date and up to days_back earlier, return first with TWSE T86 data."""
    base = datetime.strptime(target, "%Y%m%d")
    for i in range(days_back + 1):
        d = (base - timedelta(days=i)).strftime("%Y%m%d")
        if datetime.strptime(d, "%Y%m%d").weekday() >= 5:
            continue
        data = _get(TWSE_T86, {"response": "json", "date": d, "selectType": "ALL"})
        if data and (data.get("data") or data.get("aaData")):
            return d
    return None


# ── Main ───────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fast chip backfill (batch insert)")
    parser.add_argument("--months",     type=int, default=24,  help="months to backfill (default 24)")
    parser.add_argument("--start-date", type=str, default="",  help="start date YYYYMMDD (overrides --months)")
    parser.add_argument("--end-date",   type=str, default="",  help="end date YYYYMMDD (default today)")
    parser.add_argument("--sleep",      type=float, default=0.5, help="sleep between dates (default 0.5s)")
    parser.add_argument("--skip-existing", action="store_true", help="skip dates already in chip_daily")
    args = parser.parse_args()

    end_str = args.end_date or datetime.now().strftime("%Y%m%d")
    if args.start_date:
        start_str = args.start_date
    else:
        start_dt = datetime.strptime(end_str, "%Y%m%d") - timedelta(days=args.months * 31)
        start_str = start_dt.strftime("%Y%m%d")

    dates = trading_dates_between(start_str, end_str)
    print(f"\n[chip fast] {start_str} ~ {end_str}, {len(dates)} trading days, sleep={args.sleep}s")
    print(f"  Estimated time: {len(dates) * (args.sleep + 4) / 60:.0f} min")

    conn = psycopg2.connect(DATABASE_URL)
    load_allowed_codes(conn)

    # Optionally skip dates already in DB
    existing = set()
    if args.skip_existing:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT date FROM chip_daily")
            existing = {r[0] for r in cur.fetchall()}
        print(f"  Skipping {len(existing)} existing dates")

    print(f"\n  {'Date':<12} {'T86':>7} {'Margin':>7} {'TPEx-T':>8} {'TPEx-M':>8} {'Written':>8}  Status")
    print("  " + "-" * 65)

    written_total = skipped_total = error_total = 0

    for i, date_str in enumerate(dates, 1):
        if date_str in existing:
            skipped_total += 1
            continue

        t86 = margin = tpex_t = tpex_m = None
        errs = []

        t86 = fetch_twse_t86(date_str)
        if t86 is None:
            errs.append("T86")
        margin = fetch_twse_margin(date_str)
        if margin is None:
            errs.append("Margin")
        tpex_t = fetch_tpex_insti(date_str)
        if tpex_t is None:
            errs.append("TPEx-T")
        tpex_m = fetch_tpex_margin(date_str)
        if tpex_m is None:
            errs.append("TPEx-M")

        if not t86 and not margin and not tpex_t and not tpex_m:
            # Holiday or no data — skip silently
            continue

        try:
            written = batch_write(conn, date_str, t86, margin, tpex_t, tpex_m)
            written_total += written
            err_str = ",".join(errs) if errs else "OK"
            print(f"  {date_str:<12} {len(t86 or {}):>7} {len(margin or {}):>7} {len(tpex_t or {}):>8} {len(tpex_m or {}):>8} {written:>8}  {err_str}", flush=True)
        except Exception as e:
            error_total += 1
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"  {date_str:<12} {'':>7} {'':>7} {'':>8} {'':>8} {'':>8}  [ERR] {str(e)[:50]}", flush=True)

        time.sleep(args.sleep)

    conn.close()
    print(f"\n{'='*65}")
    print(f"  Written: {written_total:,}  Skipped: {skipped_total}  Errors: {error_total}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
