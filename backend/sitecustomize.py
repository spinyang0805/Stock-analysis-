"""Runtime route patch for Render FastAPI startup.

This file is auto-imported by Python when the backend directory is on sys.path.
It adds queue endpoints without replacing the large main.py file.
"""
from datetime import datetime
import sys
import threading

try:
    import fastapi
except Exception:  # pragma: no cover
    fastapi = None


def _main():
    return sys.modules.get("main") or sys.modules.get("__main__")


def _run_backfill_job(job_id, products, months):
    m = _main()
    db = getattr(m, "db", None)
    run_on_demand_backfill = getattr(m, "run_on_demand_backfill")
    ref = db.collection("job_queue").document(job_id) if db is not None else None
    written_days = 0
    errors = []
    if ref:
        ref.set({"status": "running", "started_at": datetime.now().isoformat(), "updated_at": datetime.now().isoformat()}, merge=True)
    for idx, item in enumerate(products, start=1):
        code = item.get("code")
        try:
            result = run_on_demand_backfill(code, months, item.get("market"), item.get("type"))
            written_days += int(result.get("written_days", 0))
            if result.get("errors"):
                errors.append({"stock_id": code, "errors": result.get("errors", [])[:3]})
        except Exception as exc:
            errors.append({"stock_id": code, "error": str(exc)})
        if ref:
            ref.set({
                "status": "running",
                "progress": idx,
                "current_stock": code,
                "current_name": item.get("name"),
                "written_days": written_days,
                "error_count": len(errors),
                "recent_errors": errors[-20:],
                "updated_at": datetime.now().isoformat(),
            }, merge=True)
    if ref:
        ref.set({
            "status": "done",
            "progress": len(products),
            "current_stock": None,
            "written_days": written_days,
            "error_count": len(errors),
            "recent_errors": errors[-50:],
            "finished_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }, merge=True)


def _install_routes(app):
    @app.get("/api/job/backfill_all_auto")
    def backfill_all_auto(product_type: str = "股票", market: str = "上市", months: int = 12):
        m = _main()
        db = getattr(m, "db", None)
        if db is None:
            return {"status": "failed", "firebase_enabled": False, "message": "Firebase not initialized"}
        products = m.product_universe(product_type=product_type, market=market)
        job_id = f"backfill_all_{product_type}_{market}_{int(datetime.now().timestamp())}".replace("/", "_").replace(" ", "_")
        db.collection("job_queue").document(job_id).set({
            "job_id": job_id,
            "type": "backfill_all_auto",
            "product_type": product_type,
            "market": market,
            "months": months,
            "status": "pending",
            "total": len(products),
            "progress": 0,
            "current_stock": None,
            "current_name": None,
            "written_days": 0,
            "error_count": 0,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }, merge=True)
        thread = threading.Thread(target=_run_backfill_job, args=(job_id, products, months), daemon=True, name=f"queue-{job_id}")
        thread.start()
        return {"status": "started", "job_id": job_id, "total": len(products), "product_type": product_type, "market": market, "months": months}

    @app.get("/api/job/status/{job_id}")
    def job_status(job_id: str):
        m = _main()
        db = getattr(m, "db", None)
        if db is None:
            return {"status": "failed", "firebase_enabled": False, "message": "Firebase not initialized"}
        doc = db.collection("job_queue").document(job_id).get()
        if not doc.exists:
            return {"status": "not_found", "job_id": job_id}
        return doc.to_dict() or {}


def _patch_fastapi():
    if fastapi is None or getattr(fastapi.FastAPI, "_stock_queue_patch", False):
        return
    original_init = fastapi.FastAPI.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        try:
            _install_routes(self)
        except Exception as exc:
            print("queue route patch error:", exc)

    fastapi.FastAPI.__init__ = patched_init
    fastapi.FastAPI._stock_queue_patch = True


_patch_fastapi()
