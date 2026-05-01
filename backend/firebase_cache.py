from datetime import datetime
from typing import Any, Dict, List, Optional
import pytz

from firebase import db

TW_TZ = pytz.timezone("Asia/Taipei")


def firebase_enabled() -> bool:
    return db is not None


def now_tw() -> datetime:
    return datetime.now(TW_TZ)


def save_stock_daily(stock_id: str, date: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        return False
    ref = db.collection("stocks").document(stock_id).collection("daily").document(date)
    ref.set({**payload, "stock_id": stock_id, "date": date, "updated_at": now_tw()})
    return True


def save_chip_daily(stock_id: str, date: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        return False
    ref = db.collection("chip_data").document(f"{stock_id}_{date}")
    ref.set({**payload, "stock_id": stock_id, "date": date, "updated_at": now_tw()})
    return True


def save_job_log(job_id: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        return False
    db.collection("job_logs").document(job_id).set({**payload, "updated_at": now_tw()})
    return True


def get_cache_status(stock_id: str) -> Dict[str, Any]:
    if db is None:
        return {"firebase_enabled": False, "message": "FIREBASE_KEY is not configured"}
    daily_docs = list(db.collection("stocks").document(stock_id).collection("daily").limit(3).stream())
    chip_docs = list(db.collection("chip_data").where("stock_id", "==", stock_id).limit(3).stream())
    return {
        "firebase_enabled": True,
        "stock_id": stock_id,
        "daily_sample_count": len(daily_docs),
        "chip_sample_count": len(chip_docs),
        "daily_samples": [d.to_dict() for d in daily_docs],
        "chip_samples": [d.to_dict() for d in chip_docs],
    }
