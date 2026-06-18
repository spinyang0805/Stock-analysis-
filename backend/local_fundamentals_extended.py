#!/usr/bin/env python3
"""
local_fundamentals_extended.py — 補充基本面新欄位
=================================================================
Phase 1 (本腳本): TWSE OpenAPI 快速填入
  - book_value_per_share = close / pb_ratio
  - cash_dividend        = close × dividend_yield / 100
  - foreign_shareholding_pct (from chip_daily cumulative)
  - market_cap           = close × shares_outstanding (if available)

Phase 2 (TODO): ROE/ROA/margins — FinMind API 或 MOPS Playwright
=================================================================
"""
import os, sys, requests, time
from datetime import datetime

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

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _float(v):
    try:
        return float(str(v).replace(",", "").strip()) if v not in (None, "", "-", "--") else None
    except Exception:
        return None


# ── Phase 1A: TWSE BWIBBU_d → book_value_per_share, cash_dividend ─
def fetch_twse_bwibbu():
    print("[1A] Fetching TWSE BWIBBU_d (PE/PB/yield/close)...")
    r = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_d",
                     headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()
    result = {}
    for row in data:
        code = str(row.get("Code", "")).strip()
        if not code:
            continue
        close = _float(row.get("ClosePrice"))
        pb    = _float(row.get("PBratio"))
        yld   = _float(row.get("DividendYield"))
        bvps  = round(close / pb, 2) if close and pb and pb > 0 else None
        div   = round(close * yld / 100, 2) if close and yld else None
        result[code] = {"book_value_per_share": bvps, "cash_dividend": div, "close": close}
    print(f"  Got {len(result)} TWSE stocks")
    return result


def fetch_tpex_bwibbu():
    print("[1A] Fetching TPEx PE/PB/yield/close...")
    try:
        today = datetime.now()
        roc = f"{today.year-1911}/{today.month:02d}/{today.day:02d}"
        r = requests.get("https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate",
                         params={"response": "json", "date": roc},
                         headers={**HEADERS, "Referer": "https://www.tpex.org.tw/"},
                         timeout=20)
        data = r.json()
        tables = data.get("tables", [{}])
        rows = tables[0].get("data", []) if tables else []
        result = {}
        for row in rows:
            try:
                code  = str(row[0]).strip()
                close = _float(row[2])
                yld   = _float(row[3])
                pb    = _float(row[6])
                bvps  = round(close / pb, 2) if close and pb and pb > 0 else None
                div   = round(close * yld / 100, 2) if close and yld else None
                result[code] = {"book_value_per_share": bvps, "cash_dividend": div, "close": close}
            except Exception:
                continue
        print(f"  Got {len(result)} TPEx stocks")
        return result
    except Exception as e:
        print(f"  TPEx failed: {e}")
        return {}


# ── Phase 1B: foreign_shareholding_pct from chip_daily ────────────
def calc_foreign_pct(conn):
    print("[1B] Calculating foreign_shareholding_pct from chip_daily...")
    with conn.cursor() as cur:
        # Get cumulative net foreign buy per stock over last 60 days as proxy
        # True shareholding % needs total shares; this is a simplified version
        cur.execute("""
            SELECT stock_id,
                   SUM(COALESCE(foreign_buy, 0)) AS cum_foreign_buy,
                   COUNT(date) AS days
            FROM chip_daily
            WHERE date >= TO_CHAR(NOW() - INTERVAL '60 days', 'YYYYMMDD')
            GROUP BY stock_id
        """)
        rows = cur.fetchall()
    result = {}
    for code, cum_buy, days in rows:
        # Store cumulative foreign net buy (in shares) as proxy field
        # We'll divide by 1000 to convert lots to thousands of shares
        result[code] = cum_buy
    print(f"  Got {len(result)} stocks")
    return result


# ── Phase 1C: market_cap from stock_daily latest close ────────────
def calc_market_cap(conn, bwibbu: dict) -> dict:
    print("[1C] Getting latest close from stock_daily for market_cap...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (stock_id) stock_id, close
            FROM stock_daily
            WHERE close IS NOT NULL
            ORDER BY stock_id, date DESC
        """)
        rows = cur.fetchall()
    # For market_cap we need shares_outstanding which we don't have yet
    # So we calculate a "price" based on close × 1000 as placeholder
    # Real market_cap requires shares_outstanding from TWSE company data
    result = {}
    for code, close in rows:
        result[code] = float(close) if close else None
    print(f"  Got {len(result)} stocks with close price")
    return result


# ── Write to DB ───────────────────────────────────────────────────
def update_fundamentals(conn, bwibbu_twse: dict, bwibbu_tpex: dict):
    print("[WRITE] Updating fundamentals table...")
    merged = {**bwibbu_tpex, **bwibbu_twse}  # TWSE takes priority

    sql = """
        INSERT INTO fundamentals (stock_id, book_value_per_share, cash_dividend, updated_at)
        VALUES %s
        ON CONFLICT (stock_id) DO UPDATE SET
            book_value_per_share = EXCLUDED.book_value_per_share,
            cash_dividend        = EXCLUDED.cash_dividend,
            updated_at           = NOW()
    """
    rows = []
    for code, d in merged.items():
        if d.get("book_value_per_share") or d.get("cash_dividend"):
            rows.append((code, d.get("book_value_per_share"), d.get("cash_dividend"), datetime.now()))

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()
    print(f"  Upserted {len(rows)} rows (book_value_per_share, cash_dividend)")
    return len(rows)


def main():
    conn = psycopg2.connect(DATABASE_URL)

    # Phase 1A: TWSE + TPEx BWIBBU
    twse = fetch_twse_bwibbu()
    tpex = fetch_tpex_bwibbu()
    written = update_fundamentals(conn, twse, tpex)
    print(f"\n[Phase 1A] Done: {written} stocks updated with book_value + cash_dividend")

    # Phase 1B: foreign_shareholding_pct (cumulative chip data proxy)
    # Note: True % requires shares_outstanding; storing raw cumulative for now
    print("\n[Phase 1B] foreign_shareholding_pct: requires shares_outstanding data")
    print("  -> Skipping until shares_outstanding is available from TWSE company data")

    # Phase 1C: market_cap placeholder
    close_prices = calc_market_cap(conn, twse)
    print(f"\n[Phase 1C] market_cap: {len(close_prices)} close prices found")
    print("  -> Requires shares_outstanding to compute market_cap")
    print("  -> Will calculate once shares_outstanding is fetched")

    # Final coverage report
    print("\n[VERIFY] Coverage report:")
    with conn.cursor() as cur:
        for col in ["pe_ratio", "pb_ratio", "dividend_yield", "eps",
                    "revenue", "book_value_per_share", "cash_dividend",
                    "roe", "roa", "gross_margin"]:
            cur.execute(f"SELECT COUNT(*) FROM fundamentals WHERE {col} IS NOT NULL")
            n = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM product_universe")
            total = cur.fetchone()[0]
            pct = n / total * 100 if total else 0
            print(f"  {col:<25} {n:>5}/{total} ({pct:.0f}%)")

    conn.close()
    print("\nPhase 1 complete. ROE/ROA/margins pending (MOPS Playwright or FinMind API).")


if __name__ == "__main__":
    main()
