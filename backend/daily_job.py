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


def cleanup_mislabeled_dates():
    """Delete sparse mislabeled dates (weekends/holidays written by old TPEx fallback bug).

    真正的交易日整市場有 1000+ 列；近 40 天內列數 < 100 的日期必為誤標殘留。
    """
    from firebase_cache import _run
    for table in ("stock_daily", "chip_daily"):
        deleted, err = _run(
            f"""DELETE FROM {table} WHERE date IN (
                  SELECT date FROM {table}
                  WHERE date >= to_char(CURRENT_DATE - 40, 'YYYYMMDD')
                  GROUP BY date HAVING COUNT(*) < 100
                )""",
        )
        print(f"[cleanup] {table}: removed {deleted if not err else 0} mislabeled rows"
              + (f" (error: {err})" if err else ""))


def heal_stale_stocks(limit: int, months: int = 2, min_recent_rows: int = 25):
    """Backfill stocks with too few rows in the recent window.

    以「近 45 天列數」判斷而非最新日期 — 只看最新日期會漏掉中段缺口
    （例如 TPEx 2026-06 整月斷更後補上今日，最新日期正常但六月全空）。
    完整股票近 45 天約 28~30 個交易日；< min_recent_rows 視為有缺口。
    """
    from firebase_cache import _run
    from jobs import run_on_demand_backfill

    rows, err = _run(
        """
        WITH recent AS (
          SELECT stock_id, COUNT(*) AS cnt
          FROM stock_daily
          WHERE date >= to_char(CURRENT_DATE - 45, 'YYYYMMDD')
          GROUP BY stock_id
        ),
        mkt AS (
          SELECT DISTINCT ON (stock_id) stock_id, market
          FROM stock_daily ORDER BY stock_id, date DESC
        )
        SELECT p.code, COALESCE(r.cnt, 0) AS cnt, COALESCE(m.market, '上市') AS market
        FROM product_universe p
        LEFT JOIN recent r ON r.stock_id = p.code
        LEFT JOIN mkt m ON m.stock_id = p.code
        WHERE COALESCE(r.cnt, 0) < %s
        ORDER BY COALESCE(r.cnt, 0) ASC
        """,
        (min_recent_rows,), fetch="all",
    )
    if err:
        print(f"[heal] scan failed: {err}")
        return
    stale = [(r[0], r[2], r[1]) for r in rows or []][:limit]
    print(f"[heal] {len(stale)} gappy stocks (showing 10): {stale[:10]}")

    healed, failed = 0, 0
    for i, (stock_id, market, _) in enumerate(stale, 1):
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

    cleanup_mislabeled_dates()

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
