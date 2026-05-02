from datetime import datetime
from typing import Any, Dict
import pytz

from firebase import db

TW_TZ = pytz.timezone("Asia/Taipei")


def now_tw():
    return datetime.now(TW_TZ)


def save_stock_daily(stock_id: str, date: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        print("❌ Firebase not initialized")
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

        print(f"✅ stock_daily write: {stock_id} {date}")
        return True

    except Exception as e:
        print("🔥 stock_daily error:", e)
        return False


def save_chip_daily(stock_id: str, date: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        print("❌ Firebase not initialized")
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

        print(f"✅ chip_data write: {stock_id} {date}")
        return True

    except Exception as e:
        print("🔥 chip_data error:", e)
        return False


def get_cache_status(stock_id: str):
    if db is None:
        return {"firebase_enabled": False}

    try:
        daily_docs = list(db.collection("stock_daily").document(stock_id).collection("data").limit(3).stream())
        chip_docs = list(db.collection("chip_data").document(stock_id).collection("data").limit(3).stream())

        return {
            "firebase_enabled": True,
            "stock_daily_count": len(daily_docs),
            "chip_data_count": len(chip_docs)
        }

    except Exception as e:
        return {"firebase_enabled": False, "error": str(e)}
