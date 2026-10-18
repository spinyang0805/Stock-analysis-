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


# 單日官方來源（逐檔逐月抓取，日期可信）；有這些列的日期絕不當成休市刪除
OFFICIAL_PER_DAY_SOURCES = ("TWSE STOCK_DAY", "TPEx tradingStock", "TPEx dailyCloseQuotes")


CLEANUP_MAX_DATES = 5  # 近 40 天內的休市日通常只有幾天；超過代表日曆異常，整批放棄


def cleanup_nontrading_dates(days: int = 40):
    """清除近 days 天內「依交易日曆為休市日」卻有資料的日期（舊版誤標寫入的殘留）。

    預設只列出不刪除；設 CLEANUP_NONTRADING=1 才實際刪除。安全條件：
    - 日曆來源必須是 TWSE API（fallback/cache 不動）
    - stock_daily 在該日有單日官方來源的列 → 兩張表都保留
    - 只處理今年的平日假日（日曆只涵蓋 API 回傳的年份）；週末不受此限
    - 候選日期超過 CLEANUP_MAX_DATES 則全部放棄
    """
    import datetime as _dt
    import trading_calendar
    from firebase_cache import _run
    src = trading_calendar.calendar_source()
    if src != "api":
        print(f"[cleanup] nontrading: skipped (calendar_source={src})")
        return
    apply = os.environ.get("CLEANUP_NONTRADING") == "1"
    official_rows, err = _run(
        """SELECT DISTINCT date FROM stock_daily
           WHERE date >= to_char(CURRENT_DATE - %s, 'YYYYMMDD') AND source = ANY(%s)""",
        (days, list(OFFICIAL_PER_DAY_SOURCES)), fetch="all",
    )
    if err:
        print(f"[cleanup] nontrading: official-date scan failed, skipped: {err}")
        return
    official = {r[0] for r in official_rows or []}
    this_year = str(_dt.date.today().year)

    plan = []  # (table, date, rows)
    for table in ("stock_daily", "chip_daily"):
        rows, err = _run(
            f"""SELECT date, COUNT(*) FROM {table}
                WHERE date >= to_char(CURRENT_DATE - %s, 'YYYYMMDD')
                GROUP BY date ORDER BY date""",
            (days,), fetch="all",
        )
        if err:
            print(f"[cleanup] {table}: nontrading scan failed, skipped: {err}")
            return
        for d, total in rows or []:
            if trading_calendar.is_trading_day(d) or not trading_calendar.covers(d):
                continue
            weekend = _dt.datetime.strptime(d, "%Y%m%d").weekday() >= 5
            if not weekend and not d.startswith(this_year):
                continue
            if d in official:
                print(f"[cleanup] {table} {d}: non-trading per calendar but stock_daily has official rows; kept")
                continue
            plan.append((table, d, total))

    dates = sorted({d for _, d, _ in plan})
    if not plan:
        return
    if len(dates) > CLEANUP_MAX_DATES:
        print(f"[cleanup] nontrading: {len(dates)} candidate dates > {CLEANUP_MAX_DATES}, "
              f"calendar looks wrong; nothing deleted: {dates}")
        return
    for table, d, total in plan:
        if not apply:
            print(f"[cleanup] {table} {d}: non-trading, would remove {total} rows (set CLEANUP_NONTRADING=1)")
            continue
        deleted, err = _run(f"DELETE FROM {table} WHERE date = %s", (d,))
        print(f"[cleanup] {table} {d}: non-trading, removed {deleted if not err else 0}/{total} rows"
              + (f" (error: {err})" if err else ""))


def _ensure_heal_blacklist_table():
    from firebase_cache import _run
    _, err = _run(
        """
        CREATE TABLE IF NOT EXISTS heal_blacklist (
          stock_id TEXT PRIMARY KEY,
          noop_count INT NOT NULL DEFAULT 0,
          last_attempt DATE NOT NULL
        )
        """
    )
    if err:
        print(f"[heal] heal_blacklist table setup failed: {err}")


def heal_stale_stocks(limit: int, months: int = 2, min_recent_rows: int = 25,
                       budget_min: float = 45):
    """Backfill stocks with too few rows in the recent window.

    以「近 45 天列數」判斷而非最新日期 — 只看最新日期會漏掉中段缺口
    （例如 TPEx 2026-06 整月斷更後補上今日，最新日期正常但六月全空）。
    完整股票近 45 天約 28~30 個交易日；< min_recent_rows 視為有缺口。

    budget_min: 累計耗時超過此分鐘數就停止，確保 export/commit 一定跑得到
    （見 .omc/plans/2026-07-14-pipeline-fix-spec.md T3）。

    下市股 3 天內連續補 0 筆會被寫進 heal_blacklist 並跳過（T4）。若某支股票
    其實已復活但被排除，需手動執行：
      DELETE FROM heal_blacklist WHERE stock_id='<code>';
    """
    from firebase_cache import _run
    from jobs import run_on_demand_backfill

    _ensure_heal_blacklist_table()

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
        LEFT JOIN heal_blacklist b ON b.stock_id = p.code
        WHERE COALESCE(r.cnt, 0) < %s
          AND COALESCE(b.noop_count, 0) < 3
        ORDER BY COALESCE(r.cnt, 0) ASC
        """,
        (min_recent_rows,), fetch="all",
    )
    if err:
        print(f"[heal] scan failed: {err}")
        return
    stale = [(r[0], r[2], r[1]) for r in rows or []][:limit]
    print(f"[heal] {len(stale)} gappy stocks (showing 10): {stale[:10]}")

    healed, noop, failed = 0, 0, 0
    start = time.monotonic()
    for i, (stock_id, market, _) in enumerate(stale, 1):
        elapsed_min = (time.monotonic() - start) / 60
        if elapsed_min >= budget_min:
            print(f"[heal] budget exhausted at {i - 1}/{len(stale)} "
                  f"(elapsed={elapsed_min:.1f}min, budget={budget_min}min)")
            break
        try:
            result = run_on_demand_backfill(stock_id, months, market or "TWSE")
            written = int(result.get("written_days") or 0)
            if written > 0:
                healed += 1
                _run("DELETE FROM heal_blacklist WHERE stock_id=%s", (stock_id,))
            else:
                noop += 1
                _run(
                    """
                    INSERT INTO heal_blacklist (stock_id, noop_count, last_attempt)
                    VALUES (%s, 1, CURRENT_DATE)
                    ON CONFLICT (stock_id) DO UPDATE SET
                        noop_count = heal_blacklist.noop_count + 1,
                        last_attempt = CURRENT_DATE
                    """,
                    (stock_id,),
                )
        except Exception as exc:
            failed += 1
            print(f"[heal] {stock_id} failed: {exc}")
        if i % 25 == 0:
            print(f"[heal] {i}/{len(stale)} healed={healed} noop={noop} failed={failed}")
        time.sleep(0.1)
    else:
        print(f"[heal] done: healed={healed} noop={noop} failed={failed}")
        return
    print(f"[heal] done (budget exhausted): healed={healed} noop={noop} failed={failed}")


def heal_chip_dates(days: int = 30, max_dates: int = 10, min_ratio: float = 0.7):
    """Re-fetch chips for recent trading days whose chip rows (per market) are far below that day's price rows.
    The daily writers only cover the latest session, so a failed T86 / MI_MARGN / TPEx fetch used to leave a
    permanent hole (e.g. TWSE chips 2026-04-01..04-22). Covers both markets and all four chip sources."""
    import trading_calendar as cal
    from datetime import date, timedelta as td
    from firebase_cache import _run
    from jobs import write_margin_chips, write_t86_chips, write_tpex_insti_chips, write_tpex_margin_chips
    end = date.today()
    tdays = cal.trading_days((end - td(days=days)).strftime("%Y%m%d"), (end - td(days=1)).strftime("%Y%m%d"))
    if not tdays:
        return
    rows, err = _run(
        """SELECT s.date, s.market, s.n, COALESCE(c.n, 0)
             FROM (SELECT date, market, count(*) n FROM stock_daily WHERE date = ANY(%s) GROUP BY 1, 2) s
             LEFT JOIN (SELECT date, market, count(*) n FROM chip_daily WHERE date = ANY(%s) GROUP BY 1, 2) c
               ON c.date = s.date AND c.market = s.market""",
        (tdays, tdays), fetch="all")
    if err:
        print(f"[heal-chips] scan failed: {err}")
        return
    bad = sorted({(r[0], r[1]) for r in rows or [] if r[1] in ("TWSE", "TPEx") and r[3] < min_ratio * r[2]},
                 reverse=True)[:max_dates]
    print(f"[heal-chips] {len(bad)} (date, market) with chips < {min_ratio:.0%} of prices: {bad}")
    for d, mk in bad:
        res = {"errors": [], "chips": 0, "margin_rows": 0}
        try:
            if mk == "TWSE":
                n1, n2 = write_t86_chips(d, res), write_margin_chips(d, res)
            else:
                n1, n2 = write_tpex_insti_chips(d, res), write_tpex_margin_chips(d, res)
            print(f"[heal-chips] {d} {mk}: insti={n1} margin={n2} errors={res['errors'][:2]}")
        except Exception as exc:
            print(f"[heal-chips] {d} {mk} failed: {exc}")
        time.sleep(1.5)


def main():
    parser = argparse.ArgumentParser(description="Daily stock data update")
    parser.add_argument("--lookback", type=int, default=5)
    parser.add_argument("--heal", action="store_true")
    parser.add_argument("--heal-limit", type=int, default=1200)
    parser.add_argument("--heal-months", type=int, default=2)
    parser.add_argument("--heal-budget-min", type=float, default=45)
    args = parser.parse_args()

    load_env_file()
    if not os.environ.get("DATABASE_URL"):
        print("DATABASE_URL not set")
        return 1

    from jobs import run_daily_update, write_twse_valuation, write_tpex_valuation

    cleanup_mislabeled_dates()
    cleanup_nontrading_dates()

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

    try:
        heal_chip_dates()
    except Exception as exc:
        print(f"[daily] heal-chips failed: {exc}")

    if args.heal:
        heal_stale_stocks(args.heal_limit, months=args.heal_months,
                           budget_min=args.heal_budget_min)
    return 0


if __name__ == "__main__":
    sys.exit(main())
