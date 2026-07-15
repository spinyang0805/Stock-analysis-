"""
batch_api.py — Manual maintenance tools not covered by the daily GitHub
Actions pipeline (.github/workflows/update-data.yml).
Endpoints cover:
  - DB stats + connectivity tests
  - Single-stock backfill (rescue a specific stock/date range)
  - Fundamentals: yfinance batch, MOPS monthly revenue, query
  - Job status tracking
"""
from datetime import datetime
import sys
import threading
import time

import pytz
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from firebase import db
from jobs import (
    TPEX_HEADERS,
    TPEX_INSTITUTIONAL,
    TWSE_T86,
    fetch_json,
    run_on_demand_backfill,
    today_str,
    write_mops_revenue_all,
    write_yfinance_fundamentals,
)

TW_TZ = pytz.timezone("Asia/Taipei")

# ── In-memory job registry ──────────────────────────────────────────────────
_JOBS: dict = {}
_JOBS_LOCK = threading.Lock()


def _json(payload):
    return JSONResponse(jsonable_encoder(payload), media_type="application/json; charset=utf-8")


def _main():
    return sys.modules.get("main") or sys.modules.get("__main__")


def _normalize(stock: str) -> str:
    m = _main()
    if m and hasattr(m, "normalize_stock"):
        return m.normalize_stock(stock)
    return str(stock).strip().upper()


def _universe(product_type="all", market="all"):
    m = _main()
    if m and hasattr(m, "product_universe"):
        return m.product_universe(product_type=product_type, market=market)
    return []


def _job_set(job_id: str, payload: dict):
    with _JOBS_LOCK:
        _JOBS[job_id] = payload


def _start_job(job_id: str, fn, *args, **kwargs):
    """Register + start a background thread, return immediately."""
    _job_set(job_id, {
        "status": "running",
        "job_id": job_id,
        "started_at": datetime.now(TW_TZ).isoformat(),
        "progress": None,
        "result": None,
    })

    def _worker():
        try:
            result = fn(*args, **kwargs)
            _job_set(job_id, {
                "status": "done",
                "job_id": job_id,
                "started_at": _JOBS[job_id].get("started_at"),
                "finished_at": datetime.now(TW_TZ).isoformat(),
                "result": result,
            })
        except Exception as exc:
            _job_set(job_id, {
                "status": "error",
                "job_id": job_id,
                "started_at": _JOBS.get(job_id, {}).get("started_at"),
                "finished_at": datetime.now(TW_TZ).isoformat(),
                "error": str(exc),
            })

    threading.Thread(target=_worker, daemon=True, name=f"batch-{job_id}").start()
    return {"status": "started", "job_id": job_id}


# ── Route installer ─────────────────────────────────────────────────────────
def _ensure_fundamentals_schema():
    """Create fundamentals table and ensure eps column exists (idempotent)."""
    try:
        from firebase_cache import _run as _db_run
        _db_run("""
            CREATE TABLE IF NOT EXISTS fundamentals (
                stock_id TEXT PRIMARY KEY, pe_ratio FLOAT, dividend_yield FLOAT,
                pb_ratio FLOAT, eps FLOAT, revenue BIGINT, revenue_mom FLOAT,
                revenue_yoy FLOAT, revenue_date TEXT, valuation_date TEXT,
                source TEXT, updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        _db_run("ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS eps FLOAT")
        # Clear obviously wrong dividend yields (>99% = data error from ETF tickers)
        _db_run("UPDATE fundamentals SET dividend_yield = NULL WHERE dividend_yield > 99")
    except Exception as exc:
        print(f"[batch_api] fundamentals schema setup: {exc}")


def install(app):
    _ensure_fundamentals_schema()

    # ── 0. DB stats overview ────────────────────────────────────────────────
    @app.get("/api/batch/stats")
    def batch_stats():
        """One-shot overview of all tables: counts, date ranges, missing data."""
        from firebase_cache import _run as _db_run
        stats = {}

        queries = {
            "stock_daily": [
                ("SELECT COUNT(*) FROM stock_daily", "total_rows"),
                ("SELECT COUNT(DISTINCT stock_id) FROM stock_daily", "stocks"),
                ("SELECT COUNT(DISTINCT stock_id) FROM stock_daily WHERE market='TWSE'", "twse_stocks"),
                ("SELECT COUNT(DISTINCT stock_id) FROM stock_daily WHERE market='TPEx'", "tpex_stocks"),
                ("SELECT MIN(date), MAX(date) FROM stock_daily", "date_range"),
            ],
            "chip_daily": [
                ("SELECT COUNT(*) FROM chip_daily", "total_rows"),
                ("SELECT COUNT(DISTINCT stock_id) FROM chip_daily", "stocks"),
                ("SELECT MIN(date), MAX(date) FROM chip_daily", "date_range"),
            ],
            "fundamentals": [
                ("SELECT COUNT(*) FROM fundamentals", "total_rows"),
                ("SELECT COUNT(*) FROM fundamentals WHERE pe_ratio IS NOT NULL", "with_pe"),
                ("SELECT COUNT(*) FROM fundamentals WHERE eps IS NOT NULL", "with_eps"),
                ("SELECT COUNT(*) FROM fundamentals WHERE revenue IS NOT NULL", "with_revenue"),
                ("SELECT COUNT(*) FROM fundamentals WHERE source LIKE %s", "from_yfinance", ("yfinance%",)),
                ("SELECT COUNT(*) FROM fundamentals WHERE source='tpex_pebook'", "from_tpex"),
            ],
        }

        for table, qs in queries.items():
            stats[table] = {}
            for entry in qs:
                sql, label = entry[0], entry[1]
                params = entry[2] if len(entry) > 2 else None
                row, err = _db_run(sql, params, fetch="one") if params else _db_run(sql, fetch="one")
                if err:
                    stats[table][label] = f"error: {err}"
                elif row:
                    stats[table][label] = list(row) if len(row) > 1 else row[0]

        return _json({"stats": stats, "checked_at": datetime.now(TW_TZ).isoformat()})

    # ── 1. Connectivity test ────────────────────────────────────────────────
    @app.get("/api/batch/test")
    def batch_test():
        """Test TWSE T86, TPEx institutional API, and Firebase write."""
        date = today_str()
        results = {}

        # TWSE T86 — try today, fall back up to 5 trading days
        try:
            from jobs import recent_trading_dates as _rtd
            t86_ok, t86_rows, t86_date, t86_err = False, 0, date, None
            for d in list(_rtd(5)):
                payload, err = fetch_json(TWSE_T86, params={"response": "json", "date": d, "selectType": "ALL"})
                rows = payload.get("data") or []
                if rows:
                    t86_ok, t86_rows, t86_date, t86_err = True, len(rows), d, err
                    break
                t86_err = err
            results["twse_t86"] = {
                "ok": t86_ok, "rows": t86_rows, "date": t86_date,
                "error": t86_err,
                "note": "T86 posts after 4pm TW time; showing most recent available" if not t86_ok else None,
            }
        except Exception as exc:
            results["twse_t86"] = {"ok": False, "rows": 0, "error": str(exc)}

        # TPEx institutional
        try:
            payload, err = fetch_json(
                TPEX_INSTITUTIONAL,
                params={"response": "json", "date": date, "sect": "AL", "type": "Daily"},
                headers=TPEX_HEADERS,
            )
            tables = payload.get("tables") or []
            rows = tables[0].get("data", []) if tables else []
            results["tpex_insti"] = {"ok": bool(rows), "rows": len(rows), "date": date, "error": err}
        except Exception as exc:
            results["tpex_insti"] = {"ok": False, "rows": 0, "error": str(exc)}

        # PostgreSQL write test
        try:
            from firebase_cache import _run
            _, err = _run(
                "INSERT INTO _batch_connectivity_test(ts) VALUES(NOW()) ON CONFLICT DO NOTHING",
                fetch=None,
            )
            if err and "does not exist" in str(err):
                # table absent — try a read-only ping instead
                row, err2 = _run("SELECT 1", fetch="one")
                results["postgresql"] = {"ok": row == (1,) or row == [1], "error": err2}
            else:
                results["postgresql"] = {"ok": err is None, "error": err}
        except Exception as exc:
            results["postgresql"] = {"ok": False, "error": str(exc)}

        all_ok = all(v.get("ok") for v in results.values())
        return _json({
            "status": "ok" if all_ok else "partial",
            "all_ok": all_ok,
            "tested_at": datetime.now(TW_TZ).isoformat(),
            "results": results,
        })

    # ── 2. Single-stock backfill (background) ───────────────────────────────
    @app.get("/api/batch/stock/backfill")
    def batch_stock_backfill(stock: str, months: int = 12, market: str = "TWSE"):
        """Backfill K-line for one stock (TWSE or TPEx)."""
        code = _normalize(stock)
        job_id = f"backfill-{code}-{months}m-{int(time.time())}"
        result = _start_job(job_id, run_on_demand_backfill, code, months, market)
        return _json({**result, "stock": code, "months": months, "market": market})

    # ── 3. Fundamentals: yfinance batch (background, works from any IP) ────────
    @app.get("/api/batch/fundamentals/yfinance")
    def batch_fundamentals_yfinance(market: str = "上市", offset: int = 0, limit: int = 100):
        """Fetch PE/PB/EPS/殖利率 via yfinance for universe stocks. Works from any IP."""
        from firebase_cache import _run as _db_run

        # Get codes from stock_daily (authoritative, has all stocks) filtered by market
        market_filter = "TPEx" if market in ("上櫃", "TPEx") else "TWSE"
        rows, _ = _db_run(
            "SELECT DISTINCT stock_id FROM stock_daily WHERE market=%s ORDER BY stock_id",
            (market_filter,), fetch="all",
        )
        all_codes = [r[0] for r in (rows or [])]

        # Fallback to product_universe if stock_daily is empty
        if not all_codes:
            universe = _universe(market=market)
            all_codes = [str(item.get("code") or "").strip() for item in universe if item.get("code")]

        cap = min(max(1, limit), 200)
        batch = all_codes[offset:offset + cap]
        next_offset = offset + len(batch) if offset + len(batch) < len(all_codes) else None
        job_id = f"yf-fund-{market_filter}-{offset}-{int(time.time())}"

        def _run_yf():
            r = {"errors": []}
            write_yfinance_fundamentals(batch, market, r, sleep_sec=0.25)
            r.update({"total_universe": len(all_codes), "batch_size": len(batch),
                       "offset": offset, "next_offset": next_offset})
            return r

        result = _start_job(job_id, _run_yf)
        return _json({**result, "market": market, "batch_size": len(batch),
                      "total_universe": len(all_codes), "next_offset": next_offset})

    # ── 4. Fundamentals: monthly revenue (background) ──────────────────────────
    @app.get("/api/batch/fundamentals/revenue")
    def batch_fundamentals_revenue(months_back: int = 0):
        """Write MOPS monthly revenue for all 上市+上櫃 stocks (background job)."""
        job_id = f"revenue-{months_back}m-{int(time.time())}"

        def _run_revenue():
            r = {"errors": []}
            write_mops_revenue_all(r, months_back)
            return r

        result = _start_job(job_id, _run_revenue)
        return _json({**result, "months_back": months_back})

    # ── 5. Fundamentals DB query (verify data) ────────────────────────────
    @app.get("/api/batch/fundamentals/query")
    def batch_fundamentals_query(stock: str = None, limit: int = 10):
        """Read from fundamentals table to verify data was written."""
        from firebase_cache import _run as _db_run
        if stock:
            rows, err = _db_run(
                "SELECT * FROM fundamentals WHERE stock_id=%s", (stock,), fetch="all"
            )
        else:
            rows, err = _db_run(
                "SELECT stock_id, pe_ratio, pb_ratio, eps, dividend_yield, revenue, revenue_yoy, revenue_mom, source, updated_at FROM fundamentals ORDER BY updated_at DESC LIMIT %s",
                (limit,), fetch="all"
            )
        if err:
            return _json({"error": str(err)})
        count_row, _ = _db_run("SELECT COUNT(*) FROM fundamentals", fetch="one")
        total = count_row[0] if count_row else 0
        return _json({"total_in_db": total, "rows": rows or [], "error": err})

    # ── 6. Job status ───────────────────────────────────────────────────────
    @app.get("/api/batch/job/{job_id}")
    def batch_job_status(job_id: str):
        with _JOBS_LOCK:
            return _json(_JOBS.get(job_id, {"status": "not_found", "job_id": job_id}))

    @app.get("/api/batch/jobs")
    def batch_jobs_list():
        with _JOBS_LOCK:
            jobs = sorted(_JOBS.values(), key=lambda j: j.get("started_at", ""), reverse=True)
        return _json({"count": len(jobs), "jobs": jobs[:20]})
