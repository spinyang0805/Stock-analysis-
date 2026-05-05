from datetime import datetime
from typing import Any, Dict, List
import pytz

try:
    import auto_routes  # noqa: F401
except Exception:
    pass

# NEW
try:
    import maintenance_routes  # noqa: F401
except Exception:
    pass

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
    # stricter
    if close > high * 5 or open_price > high * 5:
        return "price_outlier"
    if close > 10000 or open_price > 10000 or high > 10000 or low > 10000:
        return "price_too_large_probably_amount_field"
    if volume is not None and volume < 0:
        return "negative_volume"
    return "valid"


def is_valid_stock_payload(payload: Dict[str, Any]) -> bool:
    return explain_stock_payload_issue(payload) == "valid"

# rest unchanged ...