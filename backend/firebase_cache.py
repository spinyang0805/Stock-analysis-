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


def is_valid_stock_payload(payload: Dict[str, Any]) -> bool:
    """Only real market data should be treated as cache data.

    Old preload docs like {"preload": true} or holiday empty docs should not
    count as valid stock_daily records because they do not contain price data.
    """
    if not isinstance(payload, dict):
        return False
    if payload.get("preload") is True:
        return False
    return any(_is_number(payload.get(k)) for k in ["open", "high", "low", "close"])


def save_stock_daily(stock_id: str, date: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        print("Firebase not initialized")
        return False

    if not is_valid_stock_payload(payload):
        print(f"skip invalid stock_daily: {stock_id} {date} {payload}")
        return False

    try:
        db.collection("stock_daily") \
          .document(stock_id) \
          .collection("data") \
          .document(date) \
          .set({
              "stock_id": stock_id,
              "date": date,
              "data": payload,
              "updated_at": now_tw()
          })

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
        db.collection("chip_data") \
          .document(stock_id) \
          .collection("data") \
          .document(date) \
          .set({
              "stock_id": stock_id,
              "date": date,
              "data": payload,
              "updated_at": now_tw()
          })

        print(f"chip_data write: {stock_id} {date}")
        return True

    except Exception as e:
        print("chip_data error:", e)
        return False


def save_job_log(job_id: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        print("Firebase not initialized")
        return False

    try:
        db.collection("job_logs").document(job_id).set({
            **payload,
            "job_id": job_id,
            "updated_at": now_tw()
        })
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


def get_latest_valid_stock_daily(stock_id: str, limit: int = 30):
    if db is None:
        return None
    docs = db.collection("stock_daily").document(stock_id).collection("data").order_by("date", direction="DESCENDING").limit(limit).stream()
    for doc in docs:
        item = doc.to_dict()
        if is_valid_stock_payload(item.get("data", {})):
            item["_doc_id"] = doc.id
            return item
    return None


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
            "stock_daily_samples": valid_daily_samples[:3],
            "invalid_stock_daily_samples": invalid_daily_samples[:3],
            "chip_data_samples": chip_samples[:3]
        }

    except Exception as e:
        return {"firebase_enabled": False, "error": str(e)}
