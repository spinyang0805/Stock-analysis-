from datetime import datetime
import sys
import threading
import time

_INSTALLED = False


def _find_main():
    return sys.modules.get("main") or sys.modules.get("__main__")


def _run_backfill_job(job_id, products, months):
    m = _find_main()
    db = m.db
    ref = db.collection("job_queue").document(job_id)
    written_days = 0
    errors = []
    ref.set({"status": "running", "started_at": datetime.now().isoformat(), "updated_at": datetime.now().isoformat()}, merge=True)
    for idx, item in enumerate(products, start=1):
        state_doc = ref.get().to_dict() or {}
        if state_doc.get("control") == "pause":
            ref.set({"status": "paused", "updated_at": datetime.now().isoformat()}, merge=True)
            while True:
                time.sleep(2)
                state_doc = ref.get().to_dict() or {}
                if state_doc.get("control") == "resume":
                    ref.set({"status": "running", "updated_at": datetime.now().isoformat()}, merge=True)
                    break
                if state_doc.get("control") == "stop":
                    ref.set({"status": "stopped", "updated_at": datetime.now().isoformat()}, merge=True)
                    return
        if state_doc.get("control") == "stop":
            ref.set({"status": "stopped", "updated_at": datetime.now().isoformat()}, merge=True)
            return
        code = item.get("code")
        try:
            result = m.run_on_demand_backfill(code, months, item.get("market"), item.get("type"))
            written_days += int(result.get("written_days", 0))
            if result.get("errors"):
                errors.append({"stock_id": code, "name": item.get("name"), "errors": result.get("errors", [])[:3]})
        except Exception as exc:
            errors.append({"stock_id": code, "name": item.get("name"), "error": str(exc)})
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
        "current_name": None,
        "written_days": written_days,
        "error_count": len(errors),
        "recent_errors": errors[-50:],
        "finished_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }, merge=True)


def _install(m):
    global _INSTALLED
    if _INSTALLED:
        return
    app, db = m.app, m.db

    @app.get("/api/job/backfill_all_auto")
    def backfill_all_auto(product_type: str = "股票", market: str = "上市", months: int = 12):
        if db is None:
            return {"status": "failed", "message": "Firebase not initialized"}
        products = m.product_universe(product_type=product_type, market=market)
        job_id = f"backfill_all_{product_type}_{market}_{int(datetime.now().timestamp())}".replace("/", "_").replace(" ", "_")
        db.collection("job_queue").document(job_id).set({
            "job_id": job_id,
            "type": "backfill_all_auto",
            "product_type": product_type,
            "market": market,
            "months": months,
            "status": "pending",
            "control": "run",
            "total": len(products),
            "progress": 0,
            "current_stock": None,
            "current_name": None,
            "written_days": 0,
            "error_count": 0,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }, merge=True)
        threading.Thread(target=_run_backfill_job, args=(job_id, products, months), daemon=True).start()
        return {"status": "started", "job_id": job_id, "total": len(products), "product_type": product_type, "market": market, "months": months}

    @app.get("/api/job/status/{job_id}")
    def job_status(job_id: str):
        if db is None:
            return {"status": "failed", "message": "Firebase not initialized"}
        doc = db.collection("job_queue").document(job_id).get()
        if not doc.exists:
            return {"status": "not_found", "job_id": job_id}
        return doc.to_dict() or {}

    @app.get("/api/job/pause/{job_id}")
    def job_pause(job_id: str):
        db.collection("job_queue").document(job_id).set({"control": "pause", "updated_at": datetime.now().isoformat()}, merge=True)
        return {"status": "ok", "job_id": job_id, "control": "pause"}

    @app.get("/api/job/resume/{job_id}")
    def job_resume(job_id: str):
        db.collection("job_queue").document(job_id).set({"control": "resume", "updated_at": datetime.now().isoformat()}, merge=True)
        return {"status": "ok", "job_id": job_id, "control": "resume"}

    @app.get("/api/job/stop/{job_id}")
    def job_stop(job_id: str):
        db.collection("job_queue").document(job_id).set({"control": "stop", "updated_at": datetime.now().isoformat()}, merge=True)
        return {"status": "ok", "job_id": job_id, "control": "stop"}

    @app.get("/api/screener/strong")
    def screener_strong(product_type: str = "股票", market: str = "上市", limit: int = 50):
        picks = []
        for item in m.product_universe(product_type=product_type, market=market)[:limit]:
            code = item.get("code")
            try:
                df, _ = m.get_firebase_history(code)
                data = m.to_kline_payload(df)
                if len(data) < 60:
                    continue
                last = data[-1]
                ma5, ma10, ma20, ma60 = last.get("ma5"), last.get("ma10"), last.get("ma20"), last.get("ma60")
                score = 0
                tags = []
                if ma5 and ma10 and ma20 and ma60 and ma5 > ma10 > ma20 > ma60:
                    score += 50
                    tags.append("四線多排")
                if last.get("close") and last.get("bb_upper") and last["close"] > last["bb_upper"]:
                    score += 25
                    tags.append("開布林")
                if last.get("volume") and last.get("volume_ma5") and last["volume"] > last["volume_ma5"]:
                    score += 15
                    tags.append("量能放大")
                if score > 0:
                    picks.append({"code": code, "name": item.get("name"), "market": item.get("market"), "score": score, "tags": tags, "close": last.get("close")})
            except Exception:
                continue
        picks.sort(key=lambda x: x["score"], reverse=True)
        return {"count": len(picks), "items": picks[:50]}

    _INSTALLED = True


def boot():
    def waiter():
        for _ in range(100):
            m = _find_main()
            if m and all(hasattr(m, name) for name in ["app", "db", "product_universe", "run_on_demand_backfill", "get_firebase_history", "to_kline_payload"]):
                try:
                    _install(m)
                    return
                except Exception as exc:
                    print("auto route install error:", exc)
                    return
            time.sleep(0.1)
    threading.Thread(target=waiter, daemon=True).start()


boot()
