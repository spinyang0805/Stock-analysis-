#!/usr/bin/env python3
"""
local_yfinance_ratios.py — 用 yfinance 補充基本面財務比率
=================================================================
填入欄位: roe, roa, gross_margin, operating_margin, net_margin,
          debt_ratio (debtToEquity), current_ratio,
          shares_outstanding, market_cap, book_value_per_share
=================================================================
使用方式:
  python local_yfinance_ratios.py              # 全部股票
  python local_yfinance_ratios.py --limit 50   # 前 50 支測試
  python local_yfinance_ratios.py --start 2330 # 從指定代號開始
=================================================================
"""
import argparse, os, sys, time
import yfinance as yf

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


def _pct(v):
    """Convert decimal ratio to percentage, rounded to 2dp."""
    try:
        return round(float(v) * 100, 2) if v is not None else None
    except Exception:
        return None


def _f(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _i(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def fetch_stock(ticker_code: str) -> dict:
    """Fetch financial info from yfinance. Returns dict or {}."""
    try:
        t = yf.Ticker(ticker_code)
        info = t.info
        if not info or info.get("regularMarketPrice") is None and info.get("marketCap") is None:
            return {}
        return {
            "roe":                    _pct(info.get("returnOnEquity")),
            "roa":                    _pct(info.get("returnOnAssets")),
            "gross_margin":           _pct(info.get("grossMargins")),
            "operating_margin":       _pct(info.get("operatingMargins")),
            "net_margin":             _pct(info.get("profitMargins")),
            "debt_ratio":             _f(info.get("debtToEquity")),   # D/E × 100
            "current_ratio":          _f(info.get("currentRatio")),
            "shares_outstanding":     _i(info.get("sharesOutstanding")),
            "market_cap":             _i(info.get("marketCap")),
            "book_value_per_share":   _f(info.get("bookValue")),
        }
    except Exception:
        return {}


MAX_NUMERIC = 9_999_999_999.99  # safe cap for all NUMERIC columns

def update_db(conn, code: str, data: dict) -> bool:
    if not data:
        return False
    fields = ["roe", "roa", "gross_margin", "operating_margin", "net_margin",
              "debt_ratio", "current_ratio", "shares_outstanding",
              "market_cap", "book_value_per_share"]
    # Cap extreme values to avoid numeric overflow
    capped = {k: (v if abs(v) <= MAX_NUMERIC else None)
              for k, v in data.items()
              if v is not None and isinstance(v, (int, float))}
    data = {**data, **capped}
    updates = {f: data[f] for f in fields if data.get(f) is not None}
    if not updates:
        return False
    set_clause  = ", ".join(f"{k} = %s" for k in updates)
    col_clause  = ", ".join(updates.keys())
    val_holders = ", ".join(["%s"] * len(updates))
    sql = f"""
        INSERT INTO fundamentals (stock_id, updated_at, {col_clause})
        VALUES (%s, NOW(), {val_holders})
        ON CONFLICT (stock_id) DO UPDATE SET
            {set_clause},
            updated_at = NOW()
    """
    with conn.cursor() as cur:
        vals = [code] + list(updates.values()) + list(updates.values())
        cur.execute(sql, vals)
    conn.commit()
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",  type=int,   default=0)
    parser.add_argument("--start",  type=str,   default="")
    parser.add_argument("--sleep",  type=float, default=0.5)
    args = parser.parse_args()

    conn = psycopg2.connect(DATABASE_URL)

    # Get stock list with market type
    with conn.cursor() as cur:
        cur.execute("SELECT code, market FROM product_universe ORDER BY code")
        stocks = cur.fetchall()

    # Decode market name (might be garbled) — check bytes
    def is_twse(market_raw):
        if not market_raw:
            return True
        m = str(market_raw)
        # '上市' in various encodings, or check for TPEx markers
        return "櫃" not in m and "OTC" not in m and "TWO" not in m

    if args.start:
        codes_list = [(c, m) for c, m in stocks if c >= args.start]
    else:
        codes_list = list(stocks)

    if args.limit:
        codes_list = codes_list[:args.limit]

    print(f"\n[yfinance] {len(codes_list)} stocks, sleep={args.sleep}s")
    print(f"  {'#':<5} {'Code':<8} {'ROE':>6} {'ROA':>6} {'Gross':>6} {'Op':>6} {'Net':>6} {'D/E':>6}  Status")
    print("  " + "-" * 68)

    done = skipped = errors = 0

    for i, (code, market) in enumerate(codes_list, 1):
        suffix = ".TW" if is_twse(market) else ".TWO"
        ticker = code + suffix

        data = fetch_stock(ticker)

        # If .TW fails try .TWO and vice versa
        if not data:
            alt = code + (".TWO" if suffix == ".TW" else ".TW")
            data = fetch_stock(alt)

        if not data:
            skipped += 1
            print(f"  {i:<5} {code:<8} {'':>6} {'':>6} {'':>6} {'':>6} {'':>6} {'':>6}  [SKIP]", flush=True)
        elif update_db(conn, code, data):
            done += 1
            print(f"  {i:<5} {code:<8} "
                  f"{data.get('roe') or 0:>6.1f} {data.get('roa') or 0:>6.1f} "
                  f"{data.get('gross_margin') or 0:>6.1f} {data.get('operating_margin') or 0:>6.1f} "
                  f"{data.get('net_margin') or 0:>6.1f} {data.get('debt_ratio') or 0:>6.1f}  [OK]",
                  flush=True)
        else:
            skipped += 1
            print(f"  {i:<5} {code:<8} {'':>6} {'':>6} {'':>6} {'':>6} {'':>6} {'':>6}  [NO DATA]", flush=True)

        time.sleep(args.sleep)

    conn.close()
    print(f"\n{'='*68}")
    print(f"  Done:{done}  Skipped:{skipped}  Errors:{errors}")
    print(f"{'='*68}\n")


if __name__ == "__main__":
    main()
