from datetime import datetime
import re
import sys
import threading
import time

import requests

_INSTALLED = False
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
TWSE_LISTED = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_LISTED = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TWSE_ETF = "https://openapi.twse.com.tw/v1/opendata/t187ap03_ETF"


def _main():
    return sys.modules.get("main") or sys.modules.get("__main__")


def _clean_text(value):
    text = str(value or "").strip()
    if "�" in text or "銝" in text:
        return ""
    return text


def _valid_code(code):
    code = str(code or "").strip().upper()
    if code.startswith("004"):
        return False
    if re.fullmatch(r"[1-9][0-9]{3}", code):
        return True
    if re.fullmatch(r"00[5-9][0-9]{1,3}", code):
        return True
    if re.fullmatch(r"00[5-9][0-9]{2,3}[AB]", code):
        return True
    return False


def _infer_type(code, name):
    code = str(code).upper()
    text = f"{code} {name}"
    if code.endswith("B") or "債" in text:
        return "債券ETF"
    if code.endswith("A") or "主動" in text:
        return "ETF"
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


def _pick(row, keys, default=""):
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return _clean_text(value)
    return default


def _get_json(url):
    try:
        res = requests.get(url, headers=HEADERS, timeout=12)
        res.encoding = "utf-8-sig"
        res.raise_for_status()
        return res.json()
    except Exception:
        return []


def _external_products():
    items = []
    for row in _get_json(TWSE_LISTED):
        code = _pick(row, ["公司代號", "Code", "SecuritiesCompanyCode"])
        name = _pick(row, ["公司名稱", "CompanyName", "名稱"])
        industry = _pick(row, ["產業別", "Industry", "產業類別"], "股票")
        if _valid_code(code):
            items.append({"code": code, "name": name or code, "market": "上市", "type": _infer_type(code, name), "industry": industry or "股票"})
    for row in _get_json(TPEX_LISTED):
        code = _pick(row, ["SecuritiesCompanyCode", "公司代號", "Code"])
        name = _pick(row, ["CompanyName", "公司名稱", "名稱"])
        industry = _pick(row, ["Industry", "產業別", "產業類別"], "股票")
        if _valid_code(code):
            items.append({"code": code, "name": name or code, "market": "上櫃", "type": _infer_type(code, name), "industry": industry or "股票"})
    for row in _get_json(TWSE_ETF):
        code = _pick(row, ["證券代號", "基金代號", "Code", "代號"])
        name = _pick(row, ["證券名稱", "基金名稱", "Name", "名稱"])
        if _valid_code(code):
            typ = _infer_type(code, name)
            items.append({"code": code, "name": name or code, "market": "上市", "type": typ, "industry": typ})
    return _dedupe(items)


def _dedupe(items):
    seen = set()
    out = []
    for x in items:
        code = str(x.get("code", "")).strip().upper()
        if not _valid_code(code) or code in seen:
            continue
        seen.add(code)
        name = _clean_text(x.get("name")) or code
        market = _norm_market(x.get("market"))
        typ = x.get("type") or _infer_type(code, name)
        out.append({"code": code, "name": name, "market": market, "type": typ, "industry": x.get("industry") or typ})
    return out


def _snapshot_products(db, limit=5000):
    items = _external_products()
    if len(items) >= 100:
        return items[:limit]

    for collection in ["product_universe", "stock_daily"]:
        items = []
        try:
            for doc in db.collection(collection).limit(limit).stream():
                d = doc.to_dict() or {}
                latest = d.get("latest") or {}
                code = str(d.get("code") or d.get("stock_id") or doc.id).strip().upper()
                if not _valid_code(code):
                    continue
                name = _clean_text(d.get("name") or latest.get("name")) or code
                market = _norm_market(d.get("market") or latest.get("market"))
                typ = d.get("type") or latest.get("product_type") or _infer_type(code, name)
                industry = d.get("industry") or typ
                items.append({"code": code, "name": name, "market": market, "type": typ, "industry": industry})
        except Exception:
            pass
        items = _dedupe(items)
        if items:
            return items[:limit]

    seed = [
        {"code":"2330","name":"台積電","market":"上市","type":"股票","industry":"半導體"},
        {"code":"2408","name":"南亞科","market":"上市","type":"股票","industry":"半導體"},
        {"code":"0050","name":"元大台灣50","market":"上市","type":"ETF","industry":"ETF"},
        {"code":"0056","name":"元大高股息","market":"上市","type":"ETF","industry":"ETF"},
        {"code":"00679B","name":"元大美債20年","market":"上市","type":"債券ETF","industry":"債券ETF"},
        {"code":"00981A","name":"主動式ETF示例","market":"上市","type":"ETF","industry":"ETF"},
    ]
    return _dedupe(seed)


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

    @app.get("/api/init_universe")
    def init_universe(limit: int = 5000):
        if db is None:
            return {"status":"failed","message":"Firebase not initialized"}
        products = _external_products()
        if not products:
            products = _snapshot_products(db, limit=limit)
        written = 0
        for p in products[:limit]:
            db.collection("product_universe").document(p["code"]).set({**p, "updated_at": datetime.now().isoformat()}, merge=True)
            written += 1
        return {"status":"ok","count":len(products),"written":written,"items":products[:50],"source":"external_products_or_snapshot"}

    @app.get("/api/products_fast")
    def products_fast(limit: int = 5000):
        items = _snapshot_products(db, limit=limit)
        return {"count": len(items), "items": items[:200], "source": "external_clean_universe_or_snapshot"}

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
