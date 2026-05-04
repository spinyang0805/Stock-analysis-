from datetime import datetime
from typing import Any, Dict, List
import pytz

from firebase import db

TW_TZ = pytz.timezone("Asia/Taipei")


def now_tw():
    return datetime.now(TW_TZ)


def _is_number(value) -> bool:
    try:
        return value is not None and float(value) == float(value)
    except Exception:
        return False


def _float(value):
    try:
        return float(value)
    except Exception:
        return None


def explain_stock_payload_issue(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return "payload_not_dict"
    if payload.get("preload") is True:
        return "preload_placeholder"
    if not all(_is_number(payload.get(k)) for k in ["open", "high", "low", "close"]):
        return "missing_or_non_numeric_ohlc"

    open_price = _float(payload.get("open"))
    high = _float(payload.get("high"))
    low = _float(payload.get("low"))
    close = _float(payload.get("close"))
    volume = _float(payload.get("volume"))

    if min(open_price, high, low, close) <= 0:
        return "non_positive_price"
    if high < max(open_price, close, low):
        return "invalid_ohlc_high"
    if low > min(open_price, close, high):
        return "invalid_ohlc_low"
    if close > 10000 or open_price > 10000 or high > 10000 or low > 10000:
        return "price_too_large_probably_amount_field"
    if volume is not None and volume < 0:
        return "negative_volume"
    return "valid"


def is_valid_stock_payload(payload: Dict[str, Any]) -> bool:
    return explain_stock_payload_issue(payload) == "valid"


def save_stock_daily(stock_id: str, date: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        print("Firebase not initialized")
        return False
    if not is_valid_stock_payload(payload):
        print(f"skip invalid stock_daily: {stock_id} {date} {payload}")
        return False
    try:
        parent_ref = db.collection("stock_daily").document(stock_id)
        parent_ref.set({
            "stock_id": stock_id,
            "latest_date": date,
            "latest": payload,
            "updated_at": now_tw()
        }, merge=True)
        parent_ref.collection("data").document(date).set({
            "stock_id": stock_id,
            "date": date,
            "data": payload,
            "updated_at": now_tw()
        }, merge=True)
        print(f"stock_daily write: {stock_id} {date}")
        return True
    except Exception as e:
        print("stock_daily error:", e)
        return False


def save_chip_daily(stock_id: str, date: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        print("Firebase not initialized")
        return False
    try:
        parent_ref = db.collection("chip_data").document(stock_id)
        parent_ref.set({
            "stock_id": stock_id,
            "latest_date": date,
            "latest": payload,
            "updated_at": now_tw()
        }, merge=True)
        parent_ref.collection("data").document(date).set({
            "stock_id": stock_id,
            "date": date,
            "data": payload,
            "updated_at": now_tw()
        }, merge=True)
        print(f"chip_data merge write: {stock_id} {date}")
        return True
    except Exception as e:
        print("chip_data error:", e)
        return False


def save_job_log(job_id: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        print("Firebase not initialized")
        return False
    try:
        db.collection("job_logs").document(job_id).set({**payload, "job_id": job_id, "updated_at": now_tw()}, merge=True)
        print(f"job_log write: {job_id}")
        return True
    except Exception as e:
        print("job_log error:", e)
        return False


def _to_dicts(docs) -> List[Dict[str, Any]]:
    result = []
    for doc in docs:
        item = doc.to_dict()
        item["_doc_id"] = doc.id
        result.append(item)
    return result


def _valid_daily_docs(stock_id: str, limit: int = 260) -> List[Dict[str, Any]]:
    if db is None:
        return []
    docs = db.collection("stock_daily").document(stock_id).collection("data").order_by("date", direction="DESCENDING").limit(limit).stream()
    rows = []
    for doc in docs:
        item = doc.to_dict()
        item["_doc_id"] = doc.id
        if is_valid_stock_payload(item.get("data", {})):
            rows.append(item)
    return rows


def get_latest_valid_stock_daily(stock_id: str, limit: int = 60):
    rows = _valid_daily_docs(stock_id, limit=limit)
    return rows[0] if rows else None


def get_valid_stock_daily_series(stock_id: str, limit: int = 260) -> List[Dict[str, Any]]:
    rows = _valid_daily_docs(stock_id, limit=limit)
    rows = sorted(rows, key=lambda x: x.get("date", ""))
    result = []
    for item in rows:
        payload = item.get("data", {})
        result.append({
            "date": item.get("date"),
            "open": payload.get("open"),
            "high": payload.get("high"),
            "low": payload.get("low"),
            "close": payload.get("close"),
            "volume": payload.get("volume") or 0,
            "market": payload.get("market"),
            "name": payload.get("name"),
            "source": payload.get("source", "Firebase stock_daily"),
        })
    return result


def get_latest_chip_daily(stock_id: str, limit: int = 30) -> Dict[str, Any]:
    if db is None:
        return {}
    try:
        docs = db.collection("chip_data").document(stock_id).collection("data").order_by("date", direction="DESCENDING").limit(limit).stream()
        for doc in docs:
            item = doc.to_dict()
            payload = item.get("data", {}) or {}
            if isinstance(payload, dict) and payload:
                payload["date"] = item.get("date") or doc.id
                payload["_doc_id"] = doc.id
                return payload
    except Exception as e:
        print("latest chip read error:", e)
    return {}


def cleanup_invalid_stock_daily(stock_id: str, limit: int = 500) -> Dict[str, Any]:
    if db is None:
        return {"firebase_enabled": False, "message": "Firebase not initialized"}
    docs = list(db.collection("stock_daily").document(stock_id).collection("data").order_by("date", direction="DESCENDING").limit(limit).stream())
    deleted = 0
    kept = 0
    deleted_docs = []
    invalid_reasons = {}
    for doc in docs:
        item = doc.to_dict()
        reason = explain_stock_payload_issue(item.get("data", {}))
        if reason == "valid":
            kept += 1
            continue
        doc.reference.delete()
        deleted += 1
        deleted_docs.append(doc.id)
        invalid_reasons[reason] = invalid_reasons.get(reason, 0) + 1
    return {"firebase_enabled": True, "stock_id": stock_id, "checked": len(docs), "kept": kept, "deleted": deleted, "invalid_reasons": invalid_reasons, "deleted_docs": deleted_docs[:50]}


def audit_stock_daily_market(limit_stocks: int = 3000, limit_per_stock: int = 30, delete_invalid: bool = False) -> Dict[str, Any]:
    if db is None:
        return {"firebase_enabled": False, "message": "Firebase not initialized"}

    checked_stocks = 0
    checked_docs = 0
    valid_docs = 0
    invalid_docs = 0
    deleted_docs = 0
    invalid_stocks = []
    reason_counts = {}

    for stock_doc in db.collection("stock_daily").limit(limit_stocks).stream():
        stock_id = stock_doc.id
        checked_stocks += 1
        stock_invalid = []
        docs = stock_doc.reference.collection("data").order_by("date", direction="DESCENDING").limit(limit_per_stock).stream()
        for daily_doc in docs:
            item = daily_doc.to_dict() or {}
            payload = item.get("data", {})
            reason = explain_stock_payload_issue(payload)
            checked_docs += 1
            if reason == "valid":
                valid_docs += 1
                continue
            invalid_docs += 1
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            sample = {
                "date": item.get("date") or daily_doc.id,
                "reason": reason,
                "open": payload.get("open"),
                "high": payload.get("high"),
                "low": payload.get("low"),
                "close": payload.get("close"),
                "volume": payload.get("volume"),
                "source": payload.get("source"),
            }
            stock_invalid.append(sample)
            if delete_invalid:
                daily_doc.reference.delete()
                deleted_docs += 1
        if stock_invalid:
            invalid_stocks.append({"stock_id": stock_id, "invalid_count": len(stock_invalid), "samples": stock_invalid[:3]})

    return {
        "firebase_enabled": True,
        "mode": "cleanup" if delete_invalid else "audit_only",
        "checked_stocks": checked_stocks,
        "checked_docs": checked_docs,
        "valid_docs": valid_docs,
        "invalid_docs": invalid_docs,
        "deleted_docs": deleted_docs,
        "invalid_stock_count": len(invalid_stocks),
        "reason_counts": reason_counts,
        "invalid_stocks": invalid_stocks[:100],
    }


def get_cache_status(stock_id: str):
    if db is None:
        return {"firebase_enabled": False, "message": "Firebase not initialized"}
    try:
        daily_docs_raw = list(db.collection("stock_daily").document(stock_id).collection("data").order_by("date", direction="DESCENDING").limit(10).stream())
        chip_docs_raw = list(db.collection("chip_data").document(stock_id).collection("data").order_by("date", direction="DESCENDING").limit(10).stream())
        job_docs = list(db.collection("job_logs").limit(3).stream())
        daily_samples = _to_dicts(daily_docs_raw)
        chip_samples = _to_dicts(chip_docs_raw)
        valid_daily_samples = [d for d in daily_samples if is_valid_stock_payload(d.get("data", {}))]
        invalid_daily_samples = [d for d in daily_samples if not is_valid_stock_payload(d.get("data", {}))]
        latest_valid = valid_daily_samples[0] if valid_daily_samples else None
        return {
            "firebase_enabled": True,
            "stock_id": stock_id,
            "stock_daily_count": len(valid_daily_samples),
            "stock_daily_raw_sample_count": len(daily_samples),
            "stock_daily_invalid_sample_count": len(invalid_daily_samples),
            "chip_data_count": len(chip_samples),
            "job_log_count": len(job_docs),
            "latest_valid_stock_daily": latest_valid,
            "latest_chip_daily": get_latest_chip_daily(stock_id),
            "stock_daily_samples": valid_daily_samples[:3],
            "invalid_stock_daily_samples": invalid_daily_samples[:3],
            "chip_data_samples": chip_samples[:3]
        }
    except Exception as e:
        return {"firebase_enabled": False, "error": str(e)}
