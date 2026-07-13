"""daily_job.py — Daily data pipeline for GitHub Actions (or any host that can
reach both TWSE/TPEx and Supabase).

Steps:
  1. run_daily_update(lookback)   bulk TWSE + TPEx daily K + chips + valuation
  2. --heal                       per-stock backfill for stocks whose latest date
                                  lags the market (fixes gaps like TPEx 2026-06)
Run this before export_static_json.py.

Usage:
  python daily_job.py --lookback 5 --heal --heal-limit 1200
"""
import argparse
import os
import sys
import time
from datetime import datetime

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BACKEND_DIR)


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


def heal_stale_stocks(limit: int, months: int = 2, lag_days: int = 2):
    """Backfill stocks whose latest stock_daily date lags their market's max date."""
    from firebase_cache import _run
    from jobs import run_on_demand_backfill

    rows, err = _run(
        """
        WITH latest AS (
          SELECT stock_id, market, MAX(date) AS last_date
          FROM stock_daily
          GROUP BY stock_id, market
        ),
        market_max AS (
          SELECT market, MAX(last_date) AS market_last FROM latest GROUP BY market
        )
        SELECT l.stock_id, l.market, l.last_date, m.market_last
        FROM latest l JOIN market_max m ON m.market = l.market
        WHERE l.last_date < m.market_last
          AND l.stock_id IN (SELECT code FROM product_universe)
        ORDER BY l.last_date ASC
        """,
        fetch="all",
    )
    if err:
        print(f"[heal] scan failed: {err}")
        return
    # Only heal stocks lagging more than lag_days behind (suspensions are normal)
    stale = []
    for stock_id, market, last_date, market_last in rows or []:
        gap = (datetime.strptime(str(market_last), "%Y%m%d")
               - datetime.strptime(str(last_date), "%Y%m%d")).days
        if gap > lag_days:
            stale.append((stock_id, market, last_date, gap))
    stale = stale[:limit]
    print(f"[heal] {len(stale)} stale stocks (showing 10): {stale[:10]}")

    healed, failed = 0, 0
    for i, (stock_id, market, _, _) in enumerate(stale, 1):
        try:
            run_on_demand_backfill(stock_id, months, market or "TWSE")
            healed += 1
        except Exception as exc:
            failed += 1
            print(f"[heal] {stock_id} failed: {exc}")
        if i % 25 == 0:
            print(f"[heal] {i}/{len(stale)} healed={healed} failed={failed}")
        time.sleep(0.1)
    print(f"[heal] done: healed={healed} failed={failed}")


def main():
    parser = argparse.ArgumentParser(description="Daily stock data update")
    parser.add_argument("--lookback", type=int, default=5)
    parser.add_argument("--heal", action="store_true")
    parser.add_argument("--heal-limit", type=int, default=1200)
    parser.add_argument("--heal-months", type=int, default=2)
    args = parser.parse_args()

    load_env_file()
    if not os.environ.get("DATABASE_URL"):
        print("DATABASE_URL not set")
        return 1

    from jobs import run_daily_update, write_twse_valuation, write_tpex_valuation

    print(f"[daily] run_daily_update(lookback_days={args.lookback}) ...")
    result = run_daily_update(args.lookback)
    print(f"[daily] stocks={result.get('stocks')} chips={result.get('chips')} "
          f"tpex_chips={result.get('tpex_chips')} dates={result.get('dates_written')}")
    for e in (result.get("errors") or [])[:10]:
        print(f"[daily] error: {e}")

    try:
        val = {"errors": []}
        write_twse_valuation(val)
        write_tpex_valuation(val)
        print(f"[daily] valuation TWSE={val.get('twse_valuation_written', 0)} "
              f"TPEx={val.get('tpex_valuation_written', 0)}")
    except Exception as exc:
        print(f"[daily] valuation failed: {exc}")

    if args.heal:
        heal_stale_stocks(args.heal_limit, months=args.heal_months)
    return 0


if __name__ == "__main__":
    sys.exit(main())
