"""fetch_financials_yearly.py — Fetch per-stock yearly financials via yfinance
and store them in the financials_yearly table.

Per stock (last ~5-6 years, as far as yfinance provides):
  dividend        現金股利合計（該年度發放）
  dividend_yield  股利 / 該年底收盤價 × 100
  revenue         年營收（Total Revenue, TWD）
  net_income      年淨利（Net Income, TWD）
  eps             年 EPS（Basic EPS，缺值時為 None）

yfinance annual statements通常只有最近 4 個完整年度＋TTM；股利與股價可回溯更久。
Designed to run weekly on GitHub Actions (see update-financials.yml).

Usage:
  python fetch_financials_yearly.py                # full universe (stocks.txt)
  python fetch_financials_yearly.py --only 2330
  python fetch_financials_yearly.py --limit 20 --sleep 0.2
"""
import argparse
import os
import sys
import time
from datetime import datetime

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BACKEND_DIR)
STOCKS_TXT = os.path.join(BACKEND_DIR, "stocks.txt")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS financials_yearly (
    stock_id TEXT NOT NULL,
    year INT NOT NULL,
    revenue BIGINT,
    net_income BIGINT,
    eps FLOAT,
    dividend FLOAT,
    dividend_yield FLOAT,
    year_end_close FLOAT,
    source TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, year)
)
"""

UPSERT_SQL = """
INSERT INTO financials_yearly
    (stock_id, year, revenue, net_income, eps, dividend, dividend_yield, year_end_close, source)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (stock_id, year) DO UPDATE SET
    revenue        = COALESCE(EXCLUDED.revenue,        financials_yearly.revenue),
    net_income     = COALESCE(EXCLUDED.net_income,     financials_yearly.net_income),
    eps            = COALESCE(EXCLUDED.eps,            financials_yearly.eps),
    dividend       = COALESCE(EXCLUDED.dividend,       financials_yearly.dividend),
    dividend_yield = COALESCE(EXCLUDED.dividend_yield, financials_yearly.dividend_yield),
    year_end_close = COALESCE(EXCLUDED.year_end_close, financials_yearly.year_end_close),
    source         = EXCLUDED.source,
    updated_at     = NOW()
"""


def load_env_file():
    env_path = os.path.join(BACKEND_DIR, ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def _f(value):
    try:
        import math
        v = float(value)
        return None if math.isnan(v) else v
    except Exception:
        return None


def fetch_stock_years(code: str, market: str, years_back: int) -> dict:
    """Return {year: {...}} for one stock. market: 'TWSE' or 'TPEx'."""
    import yfinance as yf

    suffixes = [".TW", ".TWO"] if market != "TPEx" else [".TWO", ".TW"]
    this_year = datetime.now().year
    first_year = this_year - years_back

    for suffix in suffixes:
        ticker = yf.Ticker(f"{code}{suffix}")

        # 月線 → 每年年底收盤（同時用來確認 ticker 存在）
        hist = ticker.history(period=f"{years_back + 1}y", interval="1mo", auto_adjust=False)
        if hist is None or hist.empty:
            continue
        year_close = {}
        for ts, row in hist.iterrows():
            close = _f(row.get("Close"))
            if close is not None and ts.year >= first_year:
                year_close[ts.year] = close  # 迭代到年底最後一個月即為年終價

        # 現金股利：按發放年度加總
        dividend_by_year = {}
        try:
            for ts, value in ticker.dividends.items():
                if ts.year >= first_year and _f(value) is not None:
                    dividend_by_year[ts.year] = round(dividend_by_year.get(ts.year, 0) + float(value), 4)
        except Exception:
            pass

        # 年度損益表：營收 / 淨利 / EPS
        revenue_by_year, income_by_year, eps_by_year = {}, {}, {}
        try:
            stmt = ticker.income_stmt
            if stmt is not None and not stmt.empty:
                for col in stmt.columns:
                    year = col.year
                    if year < first_year:
                        continue
                    def row_of(*names):
                        for n in names:
                            if n in stmt.index:
                                return _f(stmt.at[n, col])
                        return None
                    rev = row_of("Total Revenue", "Operating Revenue")
                    net = row_of("Net Income", "Net Income Common Stockholders")
                    eps = row_of("Basic EPS", "Diluted EPS")
                    if rev is not None:
                        revenue_by_year[year] = int(rev)
                    if net is not None:
                        income_by_year[year] = int(net)
                    if eps is not None:
                        eps_by_year[year] = eps
        except Exception:
            pass

        all_years = sorted(set(year_close) | set(dividend_by_year) | set(revenue_by_year))
        result = {}
        for year in all_years:
            dividend = dividend_by_year.get(year)
            close = year_close.get(year)
            yield_pct = round(dividend / close * 100, 2) if dividend and close else None
            result[year] = {
                "revenue": revenue_by_year.get(year),
                "net_income": income_by_year.get(year),
                "eps": eps_by_year.get(year),
                "dividend": dividend,
                "dividend_yield": yield_pct,
                "year_end_close": close,
            }
        if result:
            return result
    return {}


def main():
    parser = argparse.ArgumentParser(description="Fetch yearly financials via yfinance")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--only", default="")
    parser.add_argument("--years", type=int, default=6)
    parser.add_argument("--sleep", type=float, default=0.3)
    args = parser.parse_args()

    load_env_file()
    if not os.environ.get("DATABASE_URL"):
        print("DATABASE_URL not set")
        return 1

    from firebase_cache import _run

    _, err = _run(SCHEMA_SQL)
    if err:
        print(f"schema error: {err}")
        return 1

    markets = {}
    rows, err = _run("SELECT DISTINCT ON (stock_id) stock_id, market FROM stock_daily "
                     "ORDER BY stock_id, date DESC", fetch="all")
    if not err and rows:
        markets = {r[0]: r[1] for r in rows}

    with open(STOCKS_TXT, encoding="utf-8") as f:
        codes = [c.strip().upper() for c in f if c.strip()]
    if args.only:
        wanted = {c.strip().upper() for c in args.only.split(",") if c.strip()}
        codes = [c for c in codes if c in wanted]
    if args.limit:
        codes = codes[:args.limit]

    print(f"[financials] {len(codes)} stocks, {args.years} years back")
    written_rows, done, failed = 0, 0, 0
    started = time.time()
    for code in codes:
        try:
            years = fetch_stock_years(code, markets.get(code, "TWSE"), args.years)
            for year, d in years.items():
                _, err = _run(UPSERT_SQL, (code, year, d["revenue"], d["net_income"], d["eps"],
                                           d["dividend"], d["dividend_yield"], d["year_end_close"],
                                           "yfinance"))
                if not err:
                    written_rows += 1
            done += 1
        except Exception as exc:
            failed += 1
            print(f"[financials] {code} failed: {exc}")
        if done % 50 == 0:
            rate = done / max(1e-9, time.time() - started)
            print(f"[financials] {done}/{len(codes)} rows={written_rows} failed={failed} "
                  f"eta={(len(codes) - done) / max(rate, 1e-9) / 60:.0f}m")
        time.sleep(args.sleep)

    print(f"[financials] done: stocks={done} rows={written_rows} failed={failed} "
          f"({(time.time() - started) / 60:.1f} min)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
