from datetime import datetime
import sys
import threading
import time

_INSTALLED = False


def _main():
    return sys.modules.get("main") or sys.modules.get("__main__")


def _infer_type(code, name):
    code = str(code).upper()
    text = f"{code} {name}"
    if code.endswith("B") or "債" in text:
        return "債券ETF"
    if code.startswith("00"):
        return "ETF"
    return "股票"


def _norm_market(v):
    s = str(v or "").upper()
    if s in ("TPEX", "上櫃"):
        return "上櫃"
    if s in ("TWSE", "上市"):
        return "上市"
    return "上市"


def _snapshot_products(db, limit=5000):
    items = []
    for collection in ["product_universe", "stock_daily"]:
        try:
            for doc in db.collection(collection).limit(limit).stream():
                d = doc.to_dict() or {}
                latest = d.get("latest") or {}
                code = str(d.get("code") or d.get("stock_id") or doc.id).strip()
                if not code:
                    continue
                name = str(d.get("name") or latest.get("name") or code).strip()
                market = _norm_market(d.get("market") or latest.get("market"))
                typ = d.get("type") or latest.get("product_type") or _infer_type(code, name)
                industry = d.get("industry") or typ
                items.append({"code": code, "name": name, "market": market, "type": typ, "industry": industry})
        except Exception:
            pass
        if items:
            break
    seed = [
        {"code":"2330","name":"台積電","market":"上市","type":"股票","industry":"半導體"},
        {"code":"2408","name":"南亞科","market":"上市","type":"股票","industry":"半導體"},
        {"code":"0050","name":"元大台灣50","market":"上市","type":"ETF","industry":"ETF"},
        {"code":"0056","name":"元大高股息","market":"上市","type":"ETF","industry":"ETF"},
    ]
    items.extend(seed)
    seen = set()
    out = []
    for x in items:
        if x["code"] in seen:
            continue
        seen.add(x["code"])
        out.append(x)
    return out


def _delete_doc_data(doc_ref):
    count = 0
    try:
        for sub in doc_ref.collection("data").stream():
            sub.reference.delete()
            count += 1
    except Exception:
        pass
    try:
        doc_ref.delete()
    except Exception:
        pass
    return count


def _run_rebuild(job_id, months, batch_delay, limit):
    m = _main()
    db = m.db
    ref = db.collection("job_queue").document(job_id)
    products = _snapshot_products(db, limit=limit)
    total = len(products)
    ref.set({"status":"running","phase":"snapshot","total":total,"progress":0,"updated_at":datetime.now().isoformat()}, merge=True)

    for p in products:
        db.collection("product_universe").document(p["code"]).set({**p, "updated_at": datetime.now().isoformat()}, merge=True)

    deleted_docs = 0
    collections = ["stock_daily", "chip_data", "analysis_cache", "indicators"]
    for ci, col in enumerate(collections, start=1):
        for idx, p in enumerate(products, start=1):
            deleted_docs += _delete_doc_data(db.collection(col).document(p["code"]))
            if idx % 10 == 0:
                ref.set({"phase":"reset","collection":col,"progress":idx,"total":total,"deleted_data_docs":deleted_docs,"updated_at":datetime.now().isoformat()}, merge=True)
        ref.set({"phase":"reset","collection":col,"collection_index":ci,"deleted_data_docs":deleted_docs,"updated_at":datetime.now().isoformat()}, merge=True)
        time.sleep(1)

    written = 0
    errors = []
    for idx, p in enumerate(products, start=1):
        try:
            r = m.run_on_demand_backfill(p["code"], months, p.get("market"), p.get("type"))
            written += int(r.get("written_days", 0))
            if r.get("errors"):
                errors.append({"code":p["code"],"name":p.get("name"),"errors":r.get("errors", [])[:3]})
        except Exception as exc:
            errors.append({"code":p["code"],"name":p.get("name"),"error":str(exc)})
        ref.set({"phase":"backfill","status":"running","progress":idx,"total":total,"current_stock":p["code"],"current_name":p.get("name"),"written_days":written,"error_count":len(errors),"recent_errors":errors[-20:],"updated_at":datetime.now().isoformat()}, merge=True)
        time.sleep(batch_delay)

    ref.set({"phase":"done","status":"done","progress":total,"total":total,"written_days":written,"error_count":len(errors),"recent_errors":errors[-50:],"finished_at":datetime.now().isoformat()}, merge=True)


def _install(app, db):
    global _INSTALLED
    if _INSTALLED:
        return

    @app.get("/api/products_fast")
    def products_fast(limit: int = 5000):
        items = _snapshot_products(db, limit=limit)
        return {"count": len(items), "items": items[:200], "source": "product_universe_or_stock_daily_snapshot"}

    @app.get("/api/job/rebuild_safe")
    def rebuild_safe(months: int = 12, batch_delay: int = 2, limit: int = 5000):
        if db is None:
            return {"status":"failed","message":"Firebase not initialized"}
        job_id = f"rebuild_safe_{int(datetime.now().timestamp())}"
        db.collection("job_queue").document(job_id).set({"job_id":job_id,"status":"pending","phase":"created","months":months,"created_at":datetime.now().isoformat()}, merge=True)
        threading.Thread(target=_run_rebuild, args=(job_id, months, batch_delay, limit), daemon=True).start()
        return {"status":"started","job_id":job_id,"months":months,"batch_delay":batch_delay}

    _INSTALLED = True


def boot():
    def wait():
        for _ in range(120):
            m = _main()
            if m and hasattr(m, "app") and hasattr(m, "db") and hasattr(m, "run_on_demand_backfill"):
                _install(m.app, m.db)
                return
            time.sleep(0.1)
    threading.Thread(target=wait, daemon=True).start()


boot()
