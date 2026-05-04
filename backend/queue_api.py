from datetime import datetime
import threading


def install_queue_routes(app, db, product_universe, run_on_demand_backfill):
    def run_job(job_id, products, months):
        ref = db.collection("job_queue").document(job_id)
        written_days = 0
        errors = []
        ref.set({"status": "running", "started_at": datetime.now().isoformat()}, merge=True)
        for idx, item in enumerate(products, start=1):
            code = item.get("code")
            try:
                result = run_on_demand_backfill(code, months, item.get("market"), item.get("type"))
                written_days += int(result.get("written_days", 0))
                if result.get("errors"):
                    errors.append({"stock_id": code, "errors": result.get("errors", [])[:3]})
            except Exception as exc:
                errors.append({"stock_id": code, "error": str(exc)})
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

    @app.get("/api/job/backfill_all_auto")
    def backfill_all_auto(product_type: str = "股票", market: str = "上市", months: int = 12):
        if db is None:
            return {"status": "failed", "message": "Firebase not initialized"}
        products = product_universe(product_type=product_type, market=market)
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
        threading.Thread(target=run_job, args=(job_id, products, months), daemon=True).start()
        return {"status": "started", "job_id": job_id, "total": len(products), "product_type": product_type, "market": market, "months": months}

    @app.get("/api/job/status/{job_id}")
    def queue_job_status(job_id: str):
        if db is None:
            return {"status": "failed", "message": "Firebase not initialized"}
        doc = db.collection("job_queue").document(job_id).get()
        if not doc.exists:
            return {"status": "not_found", "job_id": job_id}
        return doc.to_dict() or {}
