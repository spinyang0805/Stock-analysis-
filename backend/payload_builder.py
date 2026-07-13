"""payload_builder.py — Pure payload-building logic shared by the API (main.py)
and the static JSON exporter (export_static_json.py).

No FastAPI / network dependencies: rows in, JSON-ready dicts out.
"""
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pandas as pd

from analysis_engine import build_rule_based_analysis, enrich_indicators
from chip_routes import analyze_chip_rows
from perspective_engine import generate_perspective_cards
from signal_engine import generate_signals, generate_trade_plan

MIN_ANALYSIS_ROWS = 90

FUNDAMENTALS_COLS = [
    "pe_ratio", "dividend_yield", "pb_ratio", "eps", "roe", "roa",
    "gross_margin", "operating_margin", "net_margin",
    "debt_ratio", "current_ratio", "shares_outstanding", "market_cap",
    "book_value_per_share", "cash_dividend",
    "revenue", "revenue_yoy", "revenue_mom", "revenue_date",
    "valuation_date", "updated_at",
]


def safe_float(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def rows_to_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """stock_daily rows -> OHLCV DataFrame indexed by Date (same as main.firebase_rows_to_df)."""
    data = []
    for r in rows:
        try:
            close = float(r["close"])
            data.append({
                "Date": pd.to_datetime(str(r["date"]), format="%Y%m%d"),
                "Open": float(r["open"]) if r.get("open") is not None else close,
                "High": float(r["high"]) if r.get("high") is not None else close,
                "Low": float(r["low"]) if r.get("low") is not None else close,
                "Close": close,
                "Volume": float(r.get("volume") or 0),
            })
        except Exception:
            continue
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data).drop_duplicates(subset=["Date"]).sort_values("Date").set_index("Date")


def to_kline_payload(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df = enrich_indicators(df).reset_index()
    date_col = "Date" if "Date" in df.columns else df.columns[0]
    data = []
    for _, row in df.iterrows():
        date_value = pd.to_datetime(row[date_col])
        if date_value.weekday() >= 5:  # skip Sat/Sun — TWSE doesn't trade
            continue
        item = {
            "time": date_value.strftime("%Y-%m-%d"),
            "date": date_value.strftime("%Y%m%d"),
            "open": safe_float(row.get("Open")),
            "high": safe_float(row.get("High")),
            "low": safe_float(row.get("Low")),
            "close": safe_float(row.get("Close")),
            "volume": safe_float(row.get("Volume", 0)),
            "volume_ma5": safe_float(row.get("V_MA5")),
            "change_pct": safe_float(row.get("CHANGE_PCT")),
            "bb_width": safe_float(row.get("BB_WIDTH")),
        }
        for key in ["MA5", "MA10", "MA20", "MA60", "BB_UPPER", "BB_MID", "BB_LOWER",
                    "RSI14", "MACD", "MACD_SIGNAL", "MACD_HIST", "KD_K", "KD_D"]:
            item[key.lower()] = safe_float(row.get(key))
        if all(item[k] is not None for k in ["open", "high", "low", "close"]):
            data.append(item)
    return data


def build_meta(code: str, data: List[Dict[str, Any]], source: str,
               info: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    info = info or {}
    latest = data[-1] if data else {}
    previous = data[-2] if len(data) >= 2 else {}
    close = latest.get("close")
    prev_close = previous.get("close")
    change = None if close is None or prev_close in (None, 0) else round(close - prev_close, 2)
    change_pct = None if change is None or prev_close in (None, 0) else round(change / prev_close * 100, 2)
    return {
        "code": code,
        "name": info.get("name") or code,
        "market": info.get("market") or "--",
        "industry": info.get("industry") or "--",
        "source": source,
        "price": close,
        "open": latest.get("open"),
        "high": latest.get("high"),
        "low": latest.get("low"),
        "close": close,
        "previous_close": prev_close,
        "change": change,
        "change_pct": change_pct,
        "volume": latest.get("volume"),
        "data_date": latest.get("date"),
    }


def _has_institutional_values(row) -> bool:
    if not isinstance(row, dict):
        return False
    return any(row.get(key) is not None for key in
               ["foreign_buy", "investment_trust_buy", "dealer_buy",
                "foreign", "investment_trust", "dealer"])


def is_real_chip_row(row) -> bool:
    return _has_institutional_values(row) and row.get("source") != "generated_seed_v1"


def build_kline_payload(code: str, daily_rows: List[Dict[str, Any]],
                        info: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Mirror of GET /api/kline/{stock} (without realtime merge)."""
    source = "Firebase stock_daily"
    df = rows_to_df(daily_rows)
    data = to_kline_payload(df)
    meta = build_meta(code, data, source, info)
    return {
        "status": "ok" if data else "loading",
        "message": "ok" if data else "尚無有效K線資料",
        "stock": code,
        "normalized_stock": code,
        "meta": meta,
        "source": source,
        "realtime": None,
        "last_close": meta.get("close"),
        "last_date": data[-1]["time"] if data else None,
        "data": data,
        "cache_rows": len(daily_rows),
        "data_requirement": {"minimum_rows": MIN_ANALYSIS_ROWS,
                             "has_enough_rows": len(data) >= MIN_ANALYSIS_ROWS},
    }


def build_chip_payload(code: str, chip_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Mirror of GET /api/chip/{stock} (read-only, no live refresh)."""
    rows = sorted(chip_rows or [], key=lambda x: str(x.get("date", "")))
    real_rows = [row for row in rows if is_real_chip_row(row)]
    analysis_rows = real_rows or rows
    analysis = analyze_chip_rows(analysis_rows)
    latest = analysis_rows[-1] if analysis_rows else {}
    return {
        "status": "ok",
        "route": "/api/chip/{stock}",
        "stock": code,
        "normalized_stock": code,
        "source": "Firebase chip_daily",
        "latest_chip": latest,
        "rows": analysis_rows[-20:],
        "row_count": len(analysis_rows),
        "raw_row_count": len(rows),
        "has_institutional_data": bool(real_rows),
        "live_refresh": None,
        "analysis": analysis,
        "updated_at": datetime.now().isoformat(),
    }


def build_analysis_payload(code: str, daily_rows: List[Dict[str, Any]],
                           chip_rows: List[Dict[str, Any]],
                           info: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Mirror of GET /api/analysis/{stock} (no fallback demo data, no cache write)."""
    source = "Firebase stock_daily"
    df = rows_to_df(daily_rows)
    result = build_rule_based_analysis(df, code)

    rows = sorted(chip_rows or [], key=lambda x: str(x.get("date", "")))
    chip_analysis = analyze_chip_rows(rows)
    latest_chip = rows[-1] if rows else {}
    metrics = chip_analysis.get("metrics") if isinstance(chip_analysis, dict) else {}
    chip_for_rules = {**(latest_chip or {}), **(metrics or {})}

    data = to_kline_payload(df)
    perspective_cards = generate_perspective_cards(data, chip_for_rules or {})
    signals = generate_signals(data, chip_for_rules or {}) if data else {"signals": [], "risks": [], "action": "HOLD"}
    trade_plan = generate_trade_plan(data) if data else {}
    result.update({
        "source": source,
        "normalized_stock": code,
        "meta": build_meta(code, data, source, info),
        "perspective_cards": perspective_cards,
        "signals": signals,
        "trade_plan": trade_plan,
        "backfill_started": False,
        "data_rows": len(data),
        "data_requirement": {"minimum_rows": MIN_ANALYSIS_ROWS,
                             "has_enough_rows": len(data) >= MIN_ANALYSIS_ROWS},
    })
    return result


def build_fundamentals_payload(code: str, row) -> Dict[str, Any]:
    """Mirror of GET /api/fundamentals/{stock} for a DB row tuple (or None)."""
    if not row:
        return {"error": "查無基本面資料", "stock": code}

    def _json_safe(v):
        if isinstance(v, Decimal):
            return float(v)
        return v

    data = {k: _json_safe(v) for k, v in zip(FUNDAMENTALS_COLS, row)}
    data["stock"] = code
    data["eps_est"] = data.get("eps")
    data["data_date"] = data.get("valuation_date") or datetime.now().strftime("%Y-%m-%d")
    data["updated_at"] = str(data["updated_at"]) if data.get("updated_at") else None
    return data
