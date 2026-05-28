"""
batch_api.py — Real-data batch operations for admin use.
Endpoints cover:
  - Connectivity tests  (TWSE API, TPEx API, Firebase write)
  - Chip data updates   (today's real data + history backfill)
  - Stock daily updates (today + per-stock + universe batch)
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
from firebase_cache import save_job_log
from jobs import (
    HEADERS,
    TPEX_HEADERS,
    TPEX_INSTITUTIONAL,
    TWSE_T86,
    fetch_json,
    recent_trading_dates,
    run_chip_history_backfill,
    run_daily_update,
    run_on_demand_backfill,
    today_str,
    write_margin_chips,
    write_t86_chips,
    write_tpex_insti_chips,
    write_tpex_margin_chips,
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
def install(app):

    # ── 1. Connectivity test ────────────────────────────────────────────────
    @app.get("/api/batch/test")
    def batch_test():
        """Test TWSE T86, TPEx institutional API, and Firebase write."""
        date = today_str()
        results = {}

        # TWSE T86
        try:
            payload, err = fetch_json(TWSE_T86, params={"response": "json", "date": date, "selectType": "ALL"})
            rows = payload.get("data") or []
            results["twse_t86"] = {"ok": bool(rows), "rows": len(rows), "date": date, "error": err}
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

    # ── 2. Today's real chip data ───────────────────────────────────────────
    @app.get("/api/batch/chip/today")
    def batch_chip_today(date: str = None):
        """Write real chip data for a given date (default: today).
        Calls TWSE T86 + margin + TPEx institutional + TPEx margin."""
        target = date or today_str()
        r = {"chips": 0, "margin_rows": 0, "tpex_chips": 0, "tpex_margin_rows": 0, "errors": []}
        for fn, label in [
            (write_t86_chips, "TWSE T86"),
            (write_margin_chips, "TWSE margin"),
            (write_tpex_insti_chips, "TPEx insti"),
            (write_tpex_margin_chips, "TPEx margin"),
        ]:
            try:
                fn(target, r)
            except Exception as exc:
                r["errors"].append(f"{label}: {exc}")

        return _json({
            "status": "ok" if not r["errors"] else "partial",
            "date": target,
            "twse_chips": r.get("chips", 0),
            "twse_margin": r.get("margin_rows", 0),
            "tpex_chips": r.get("tpex_chips", 0),
            "tpex_margin": r.get("tpex_margin_rows", 0),
            "errors": r.get("errors", [])[:10],
        })

    # ── 3. Chip history backfill (background) ───────────────────────────────
    @app.get("/api/batch/chip/history")
    def batch_chip_history(months: int = 3, max_days: int = None):
        """Start background chip history backfill (TWSE + TPEx, all stocks, N months)."""
        days = int(max_days or max(20, months * 22))
        job_id = f"chip-history-{days}d-{int(time.time())}"
        result = _start_job(job_id, run_chip_history_backfill, months, days)
        return _json({**result, "months": months, "target_days": days})

    # ── 4. Stock daily today (background) ──────────────────────────────────
    @app.get("/api/batch/stock/today")
    def batch_stock_today(lookback_days: int = 5):
        """Update stock daily for the most recent N trading days (TWSE + TPEx)."""
        job_id = f"stock-daily-{today_str()}-{int(time.time())}"
        result = _start_job(job_id, run_daily_update, lookback_days)
        return _json({**result, "lookback_days": lookback_days})

    # ── 5. Single-stock backfill (background) ───────────────────────────────
    @app.get("/api/batch/stock/backfill")
    def batch_stock_backfill(stock: str, months: int = 12, market: str = "TWSE"):
        """Backfill K-line for one stock (TWSE or TPEx)."""
        code = _normalize(stock)
        job_id = f"backfill-{code}-{months}m-{int(time.time())}"
        result = _start_job(job_id, run_on_demand_backfill, code, months, market)
        return _json({**result, "stock": code, "months": months, "market": market})

    # ── 6. Universe batch backfill (background) ─────────────────────────────
    @app.get("/api/batch/stock/universe")
    def batch_stock_universe(
        product_type: str = "股票",
        market: str = "上市",
        offset: int = 0,
        limit: int = 50,
        months: int = 12,
    ):
        """Backfill K-line for a slice of the universe (capped at 100 per call)."""
        universe = _universe(product_type=product_type, market=market)
        cap = min(max(1, limit), 100)
        batch = universe[offset:offset + cap]
        next_offset = offset + len(batch) if offset + len(batch) < len(universe) else None
        job_id = f"universe-{market}-{offset}-{int(time.time())}"

        def _run_batch():
            res = {"written_days": 0, "stocks_done": 0, "errors": []}
            mkt = "TPEx" if market in ("上櫃", "TPEx") else "TWSE"
            for item in batch:
                code = str(item.get("code") or "").strip()
                if not code:
                    continue
                try:
                    r = run_on_demand_backfill(code, months, mkt)
                    res["written_days"] += r.get("written_days", 0)
                    res["stocks_done"] += 1
                except Exception as exc:
                    res["errors"].append(f"{code}: {exc}")
                time.sleep(0.15)
            res.update({
                "total_universe": len(universe),
                "batch_size": len(batch),
                "offset": offset,
                "next_offset": next_offset,
            })
            save_job_log(job_id, res)
            return res

        result = _start_job(job_id, _run_batch)
        return _json({
            **result,
            "total_universe": len(universe),
            "batch_size": len(batch),
            "offset": offset,
            "next_offset": next_offset,
        })

    # ── 7. Job status ───────────────────────────────────────────────────────
    @app.get("/api/batch/job/{job_id}")
    def batch_job_status(job_id: str):
        with _JOBS_LOCK:
            return _json(_JOBS.get(job_id, {"status": "not_found", "job_id": job_id}))

    @app.get("/api/batch/jobs")
    def batch_jobs_list():
        with _JOBS_LOCK:
            jobs = sorted(_JOBS.values(), key=lambda j: j.get("started_at", ""), reverse=True)
        return _json({"count": len(jobs), "jobs": jobs[:20]})
