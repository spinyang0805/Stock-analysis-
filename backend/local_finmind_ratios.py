#!/usr/bin/env python3
"""
local_finmind_ratios.py — 從 FinMind 免費 API 取得季度財務比率
=================================================================
計算: ROE, ROA, gross_margin, operating_margin, net_margin,
      debt_ratio, current_ratio, shares_outstanding, market_cap
免費版: 600 req/day。每支股票需 2 req，每天跑 ~290 支。
=================================================================
使用方式:
  python local_finmind_ratios.py              # 從上次中斷繼續
  python local_finmind_ratios.py --limit 100  # 只跑前 100 支
  python local_finmind_ratios.py --start 2330 # 從指定代號開始
=================================================================
"""
import argparse, os, sys, time, requests
from datetime import datetime, timedelta

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

FINMIND_BASE = "https://api.finmindtrade.com/api/v4/data"
START_DATE = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
SLEEP_BETWEEN = 0.4


def _get(dataset: str, stock_id: str) -> list:
    try:
        r = requests.get(FINMIND_BASE, params={
            "dataset": dataset, "data_id": stock_id, "start_date": START_DATE
        }, timeout=15)
        if r.status_code == 429:
            return None  # Rate limited
        if r.status_code != 200:
            return []
        d = r.json()
        if d.get("msg") != "success":
            return []
        return d.get("data", [])
    except Exception:
        return []


def _val(rows: list, type_key: str, latest_date: str):
    matches = [r["value"] for r in rows if r["type"] == type_key and r["date"] == latest_date]
    return matches[0] if matches else None


def _safe_pct(num, denom):
    try:
        if num is None or denom is None or denom == 0:
            return None
        return round(num / denom * 100, 2)
    except Exception:
        return None


def _safe_ratio(num, denom):
    try:
        if num is None or denom is None or denom == 0:
            return None
        return round(num / denom, 2)
    except Exception:
        return None


def process_stock(code: str, close_prices: dict, sleep_sec: float = 0.4) -> dict:
    """Fetch income + balance for one stock, return ratio dict."""
    # Income statement
    income = _get("TaiwanStockFinancialStatements", code)
    if income is None:
        return None  # Rate limited - stop
    time.sleep(sleep_sec)

    # Balance sheet
    balance = _get("TaiwanStockBalanceSheet", code)
    if balance is None:
        return None  # Rate limited - stop
    time.sleep(sleep_sec)

    if not income and not balance:
        return {}

    # Get latest quarter
    dates = sorted(set(r["date"] for r in income + balance), reverse=True)
    if not dates:
        return {}
    latest = dates[0]

    # Income statement values
    revenue     = _val(income, "Revenue", latest)
    gross       = _val(income, "GrossProfit", latest)
    op_income   = _val(income, "OperatingIncome", latest)
    net_income  = _val(income, "IncomeAfterTaxes", latest)

    # Balance sheet values
    total_assets   = _val(balance, "TotalAssets", latest)
    cur_assets     = _val(balance, "CurrentAssets", latest)
    cur_liab       = _val(balance, "CurrentLiabilities", latest)
    total_liab     = _val(balance, "Liabilities", latest)
    equity         = _val(balance, "Equity", latest)
    capital_stock  = _val(balance, "OrdinaryShare", latest)  # in NT dollars, par = NT$10

    # Calculate ratios
    gross_margin    = _safe_pct(gross, revenue)
    op_margin       = _safe_pct(op_income, revenue)
    net_margin      = _safe_pct(net_income, revenue)
    roe             = _safe_pct(net_income, equity)
    roa             = _safe_pct(net_income, total_assets)
    debt_ratio      = _safe_pct(total_liab, total_assets)
    current_ratio   = _safe_ratio(cur_assets, cur_liab)

    # Shares outstanding: capital stock (NT$) / par value (NT$10)
    shares_outstanding = int(capital_stock / 10) if capital_stock else None

    # Market cap: close × shares
    close = close_prices.get(code)
    market_cap = int(close * shares_outstanding) if close and shares_outstanding else None

    return {
        "roe": roe, "roa": roa,
        "gross_margin": gross_margin, "operating_margin": op_margin,
        "net_margin": net_margin, "debt_ratio": debt_ratio,
        "current_ratio": current_ratio,
        "shares_outstanding": shares_outstanding,
        "market_cap": market_cap,
        "latest_quarter": latest,
    }


def update_db(conn, code: str, data: dict):
    if not data:
        return False
    fields = ["roe", "roa", "gross_margin", "operating_margin", "net_margin",
              "debt_ratio", "current_ratio", "shares_outstanding", "market_cap"]
    updates = {f: data[f] for f in fields if data.get(f) is not None}
    if not updates:
        return False
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    sql = f"""
        INSERT INTO fundamentals (stock_id, updated_at, {', '.join(updates.keys())})
        VALUES (%s, NOW(), {', '.join(['%s']*len(updates))})
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
    parser.add_argument("--limit",  type=int, default=0)
    parser.add_argument("--start",  type=str, default="")
    parser.add_argument("--sleep",  type=float, default=SLEEP_BETWEEN)
    args = parser.parse_args()
    sleep_sec = args.sleep
    conn = psycopg2.connect(DATABASE_URL)

    # Get stock list
    with conn.cursor() as cur:
        cur.execute("SELECT code FROM product_universe ORDER BY code")
        codes = [r[0] for r in cur.fetchall()]

    # Get latest close prices
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (stock_id) stock_id, close
            FROM stock_daily WHERE close IS NOT NULL
            ORDER BY stock_id, date DESC
        """)
        close_prices = {r[0]: float(r[1]) for r in cur.fetchall()}

    if args.start:
        try:
            idx = codes.index(args.start)
            codes = codes[idx:]
        except ValueError:
            pass

    if args.limit:
        codes = codes[:args.limit]

    est_req = len(codes) * 2
    print(f"\n[FinMind] {len(codes)} stocks, ~{est_req} API requests (limit 600/day)")
    print(f"  Date range: {START_DATE} ~ today  sleep={sleep_sec}s")
    print(f"  {'#':<5} {'Code':<8} {'ROE':>6} {'ROA':>6} {'Gross':>7} {'Op':>7} {'Net':>7} {'Debt':>7}  Status")
    print("  " + "-" * 72)

    done = skipped = errors = rate_limited = 0
    last_code = ""

    for i, code in enumerate(codes, 1):
        data = process_stock(code, close_prices, sleep_sec)

        if data is None:
            rate_limited += 1
            print(f"\n  [RATE LIMIT] Hit at stock {code} ({i}/{len(codes)})")
            print(f"  Remaining stocks: {len(codes)-i+1} — re-run tomorrow with --start {code}")
            break

        if not data:
            skipped += 1
            print(f"  {i:<5} {code:<8} {'':>6} {'':>6} {'':>7} {'':>7} {'':>7} {'':>7}  [SKIP]", flush=True)
            continue

        if update_db(conn, code, data):
            done += 1
            last_code = code
            print(f"  {i:<5} {code:<8} "
                  f"{data.get('roe') or 0:>6.1f} {data.get('roa') or 0:>6.1f} "
                  f"{data.get('gross_margin') or 0:>7.1f} {data.get('operating_margin') or 0:>7.1f} "
                  f"{data.get('net_margin') or 0:>7.1f} {data.get('debt_ratio') or 0:>7.1f}  "
                  f"Q={data.get('latest_quarter','?')}", flush=True)
        else:
            skipped += 1
            print(f"  {i:<5} {code:<8} {'':>6} {'':>6} {'':>7} {'':>7} {'':>7} {'':>7}  [NO DATA]", flush=True)

    print(f"\n{'='*72}")
    print(f"  Done:{done}  Skipped:{skipped}  Errors:{errors}  RateLimited:{rate_limited}")
    if last_code:
        print(f"  Last processed: {last_code}")
    print(f"  To continue: python local_finmind_ratios.py --start <next_code>")
    print(f"{'='*72}\n")
    conn.close()


if __name__ == "__main__":
    main()
