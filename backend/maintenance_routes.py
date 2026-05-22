from datetime import datetime
import re
import sys
import threading
import time

import requests

from firebase_cache import (
    save_product, get_all_products_from_db,
    save_job_queue, update_job_queue, delete_stock_data,
)

_INSTALLED = False
_PRODUCTS_CACHE = None

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
TWSE_LISTED = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_LISTED = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TWSE_ETF = "https://openapi.twse.com.tw/v1/opendata/t187ap03_ETF"

NAME_FIX = {
    "0050": "元大台灣50", "0056": "元大高股息", "00679B": "元大美債20年",
    "00878": "國泰永續高股息", "00919": "群益台灣精選高息",
    "00981A": "主動統一台股增長", "2330": "台積電", "2317": "鴻海",
    "2408": "南亞科", "2454": "聯發科", "2308": "台達電", "2382": "廣達",
}

SEED_PRODUCTS = [
    {"code": "2330", "name": "台積電", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "2317", "name": "鴻海", "market": "上市", "type": "股票", "industry": "電子"},
    {"code": "2454", "name": "聯發科", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "2308", "name": "台達電", "market": "上市", "type": "股票", "industry": "電子零組件"},
    {"code": "2382", "name": "廣達", "market": "上市", "type": "股票", "industry": "電腦及週邊"},
    {"code": "0050", "name": "元大台灣50", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "0056", "name": "元大高股息", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00878", "name": "國泰永續高股息", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00919", "name": "群益台灣精選高息", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00679B", "name": "元大美債20年", "market": "上市", "type": "高股息ETF", "industry": "ETF"},
    {"code": "00981A", "name": "主動統一台股增長", "market": "上市", "type": "ETF", "industry": "ETF"},
]


def _main():
    return sys.modules.get("main") or sys.modules.get("__main__")


def _clean_text(value):
    text = str(value or "").strip()
    if not text or "嚙" in text or "▯" in text:
        return ""
    return text


def _safe_name(code, name):
    code = str(code or "").strip().upper()
    return NAME_FIX.get(code) or _clean_text(name) or code


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
    code = str(code or "").upper()
    text = f"{code} {name}"
    if code.endswith("B") or "債" in text:
        return "高股息ETF"
    if code.startswith("00") or code.endswith("A") or "ETF" in text.upper():
        return "ETF"
    return "股票"


def _norm_market(value):
    text = str(value or "").strip().upper()
    if text in ("TPEX", "上櫃"):
        return "上櫃"
    if text in ("TWSE", "上市"):
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
        res = requests.get(url, headers=HEADERS, timeout=6)
        res.encoding = "utf-8-sig"
        res.raise_for_status()
        return res.json()
    except Exception:
        return []


def _dedupe(items):
    seen = set()
    out = []
    for item in items:
        code = str(item.get("code", "")).strip().upper()
        if not _valid_code(code) or code in seen:
            continue
        seen.add(code)
        name = _safe_name(code, item.get("name"))
        product_type = item.get("type") or _infer_type(code, name)
        out.append({
            "code": code, "name": name,
            "market": _norm_market(item.get("market")),
            "type": product_type, "industry": item.get("industry") or product_type,
        })
    return out


def _external_products():
    items = []
    for row in _get_json(TWSE_LISTED):
        code = _pick(row, ["公司代號", "Code", "SecuritiesCompanyCode"])
        name = _safe_name(code, _pick(row, ["公司名稱", "CompanyName", "簡稱"]))
        industry = _pick(row, ["產業別", "Industry"], "股票")
        if _valid_code(code):
            items.append({"code": code, "name": name, "market": "上市", "type": _infer_type(code, name), "industry": industry or "股票"})
    for row in _get_json(TPEX_LISTED):
        code = _pick(row, ["SecuritiesCompanyCode", "公司代號", "Code"])
        name = _safe_name(code, _pick(row, ["CompanyName", "公司名稱", "簡稱"]))
        industry = _pick(row, ["Industry", "產業別"], "股票")
        if _valid_code(code):
            items.append({"code": code, "name": name, "market": "上櫃", "type": _infer_type(code, name), "industry": industry or "股票"})
    for row in _get_json(TWSE_ETF):
        code = _pick(row, ["基金代號", "有價證券代號", "Code", "代號"])
        name = _safe_name(code, _pick(row, ["基金名稱", "有價證券名稱", "Name", "名稱"]))
        if _valid_code(code):
            product_type = _infer_type(code, name)
            items.append({"code": code, "name": name, "market": "上市", "type": product_type, "industry": product_type})
    return _dedupe(items)


def _seed_products():
    return _dedupe(SEED_PRODUCTS)


def _snapshot_products(limit=5000, use_cache=True):
    global _PRODUCTS_CACHE
    if use_cache and _PRODUCTS_CACHE:
        return _PRODUCTS_CACHE[:limit]
    items = _external_products()
    if len(items) >= 100:
        _PRODUCTS_CACHE = items
        return items[:limit]
    items = get_all_products_from_db(limit=limit)
    if items:
        _PRODUCTS_CACHE = items
        return items[:limit]
    _PRODUCTS_CACHE = _seed_products()
    return _PRODUCTS_CACHE[:limit]


def _run_rebuild(job_id, months, batch_delay, limit):
    m = _main()
    products = _snapshot_products(limit=limit)
    total = len(products)
    update_job_queue(job_id, {"status": "running", "phase": "snapshot", "total": total, "progress": 0})

    for product in products:
        save_product(product["code"], product)

    deleted_docs = 0
    for idx, product in enumerate(products, start=1):
        deleted_docs += delete_stock_data(product["code"])
        if idx % 10 == 0:
            update_job_queue(job_id, {"phase": "reset", "progress": idx, "total": total,
                                      "deleted_data_docs": deleted_docs})
    time.sleep(1)

    written = 0
    errors = []
    for idx, product in enumerate(products, start=1):
        try:
            result = m.run_on_demand_backfill(product["code"], months, product.get("market"), product.get("type"))
            written += int(result.get("written_days", 0))
            if result.get("errors"):
                errors.append({"code": product["code"], "name": product.get("name"), "errors": result.get("errors", [])[:3]})
        except Exception as exc:
            errors.append({"code": product["code"], "name": product.get("name"), "error": str(exc)})
        update_job_queue(job_id, {
            "phase": "backfill", "status": "running", "progress": idx, "total": total,
            "current_stock": product["code"], "current_name": product.get("name"),
            "written_days": written, "error_count": len(errors), "recent_errors": errors[-20:],
        })
        time.sleep(batch_delay)

    update_job_queue(job_id, {
        "phase": "done", "status": "done", "progress": total, "total": total,
        "written_days": written, "error_count": len(errors),
        "recent_errors": errors[-50:], "finished_at": datetime.now().isoformat(),
    })


def _install(app, db):
    global _INSTALLED
    if _INSTALLED:
        return

    @app.get("/api/init_universe")
    def init_universe(limit: int = 5000):
        items = _snapshot_products(limit=limit, use_cache=False)
        return {"status": "ready_for_batch", "count": len(items),
                "message": "Use /api/init_universe_batch?offset=0&limit=10 and repeat until next_offset is null"}

    @app.get("/api/init_universe_batch")
    def init_universe_batch(offset: int = 0, limit: int = 10):
        if db is None:
            return {"status": "failed", "message": "Database not initialized"}
        items = _snapshot_products(limit=5000, use_cache=True)
        batch = items[offset:offset + limit]
        written = 0
        errors = []
        for product in batch:
            try:
                save_product(str(product["code"]), product)
                written += 1
            except Exception as exc:
                errors.append({"code": product.get("code"), "error": str(exc)})
        next_offset = offset + len(batch) if offset + len(batch) < len(items) else None
        return {"status": "ok", "count": len(items), "offset": offset, "limit": limit,
                "processed": len(batch), "written": written, "error_count": len(errors),
                "errors": errors[:10], "next_offset": next_offset}

    @app.get("/api/products_fast")
    def products_fast(limit: int = 5000):
        try:
            items = _snapshot_products(limit=limit)
            return {"count": len(items), "items": items[:200], "source": "cached_external_or_db"}
        except Exception as exc:
            return {"count": 0, "items": [], "error": str(exc)}

    @app.get("/api/job/rebuild_safe")
    def rebuild_safe(months: int = 12, batch_delay: int = 2, limit: int = 5000):
        if db is None:
            return {"status": "failed", "message": "Database not initialized"}
        job_id = f"rebuild_safe_{int(datetime.now().timestamp())}"
        save_job_queue(job_id, {"job_id": job_id, "status": "pending", "phase": "created",
                                "months": months, "created_at": datetime.now().isoformat()})
        threading.Thread(target=_run_rebuild, args=(job_id, months, batch_delay, limit), daemon=True).start()
        return {"status": "queued", "job_id": job_id}

    _INSTALLED = True


def boot():
    def wait():
        for _ in range(50):
            main_module = _main()
            if main_module and hasattr(main_module, "app") and hasattr(main_module, "db"):
                _install(main_module.app, main_module.db)
                return
            time.sleep(0.1)
    threading.Thread(target=wait, daemon=True, name="maintenance_boot").start()


boot()
