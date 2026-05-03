from datetime import datetime, timedelta
import math
import random
import threading

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from analysis_engine import build_rule_based_analysis, enrich_indicators
from firebase import db
from firebase_cache import (
    cleanup_invalid_stock_daily,
    get_cache_status,
    get_valid_stock_daily_series,
    get_latest_chip_daily,
)
from jobs import run_daily_update, run_on_demand_backfill, preload_hot_stocks
from stock_list import search_products

try:
    from dashboard_service import fetch_realtime_board, fetch_institutional, fetch_margin, analyze_dashboard
except Exception:
    fetch_realtime_board = fetch_institutional = fetch_margin = analyze_dashboard = None

app = FastAPI(title="TW Stock Decision API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STOCK_NAME_MAP = {
    "台積電": "2330", "鴻海": "2317", "聯發科": "2454", "聯發": "2454", "大聯大": "3702",
    "廣達": "2382", "緯創": "3231", "仁寶": "2324", "台達電": "2308", "華碩": "2357",
}
STOCK_INFO_MAP = {
    "2330": {"name": "台積電", "market": "上市", "industry": "半導體"},
    "3702": {"name": "大聯大", "market": "上市", "industry": "電子通路"},
    "2317": {"name": "鴻海", "market": "上市", "industry": "其他電子"},
    "2454": {"name": "聯發科", "market": "上市", "industry": "半導體"},
    "2382": {"name": "廣達", "market": "上市", "industry": "電腦及週邊"},
    "3231": {"name": "緯創", "market": "上市", "industry": "電腦及週邊"},
    "2324": {"name": "仁寶", "market": "上市", "industry": "電腦及週邊"},
    "2308": {"name": "台達電", "market": "上市", "industry": "電子零組件"},
    "2357": {"name": "華碩", "market": "上市", "industry": "電腦及週邊"},
}


def normalize_stock(stock: str) -> str:
    stock = str(stock).strip()
    mapped = STOCK_NAME_MAP.get(stock, stock)
    return str(mapped).upper().replace(".TW", "").replace(".TWO", "").split()[0]


def safe_float(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def firebase_rows_to_df(rows):
    data = []
    for r in rows:
        try:
            data.append({
                "Date": pd.to_datetime(str(r["date"]), format="%Y%m%d"),
                "Open": float(r["open"]),
                "High": float(r["high"]),
                "Low": float(r["low"]),
                "Close": float(r["close"]),
                "Volume": float(r.get("volume") or 0),
            })
        except Exception:
            continue
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data).drop_duplicates(subset=["Date"]).sort_values("Date").set_index("Date")


def get_firebase_history(code: str, limit: int = 260):
    rows = get_valid_stock_daily_series(code, limit=limit)
    return firebase_rows_to_df(rows), rows


def fallback_history(stock: str, days: int = 120) -> pd.DataFrame:
    random.seed(normalize_stock(stock))
    code = normalize_stock(stock)
    base_map = {"2330": 600, "3702": 100, "2317": 150, "2454": 900}
    base = float(base_map.get(code, 100 + (sum(ord(c) for c in code) % 200)))
    rows, price = [], base
    current = datetime.now() - timedelta(days=days * 1.45)
    while len(rows) < days:
        current += timedelta(days=1)
        if current.weekday() >= 5:
            continue
        change = random.uniform(-2.2, 2.2) + math.sin(len(rows) / 18) * 0.8
        open_price = max(1, price + random.uniform(-1.5, 1.5))
        close = max(1, open_price + change)
        high = max(open_price, close) + random.uniform(.4, 2.2)
        low = min(open_price, close) - random.uniform(.4, 2.2)
        rows.append({"Date": current, "Open": open_price, "High": high, "Low": low, "Close": close, "Volume": random.randint(2000, 90000)})
        price = close
    return pd.DataFrame(rows).set_index("Date")


def to_kline_payload(df: pd.DataFrame):
    if df is None or df.empty:
        return []
    df = enrich_indicators(df).reset_index()
    date_col = "Date" if "Date" in df.columns else df.columns[0]
    data = []
    for _, row in df.iterrows():
        date_value = pd.to_datetime(row[date_col])
        item = {
            "time": int(date_value.timestamp()),
            "date": date_value.strftime("%Y%m%d"),
            "open": safe_float(row.get("Open")),
            "high": safe_float(row.get("High")),
            "low": safe_float(row.get("Low")),
            "close": safe_float(row.get("Close")),
            "volume": safe_float(row.get("Volume", 0)),
        }
        for key in ["MA5", "MA10", "MA20", "MA60", "BB_UPPER", "BB_MID", "BB_LOWER", "RSI14", "MACD", "MACD_SIGNAL", "MACD_HIST"]:
            item[key.lower()] = safe_float(row.get(key))
        if all(item[k] is not None for k in ["open", "high", "low", "close"]):
            data.append(item)
    return data


def build_meta(code: str, data, source: str):
    info = STOCK_INFO_MAP.get(code, {})
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


def start_thread(name: str, target, *args, **kwargs):
    def runner():
        try:
            target(*args, **kwargs)
        except Exception as exc:
            print(f"background job {name} error:", exc)
    thread = threading.Thread(target=runner, daemon=True, name=name)
    thread.start()
    return {"status": "started", "job": name, "message": f"{name} running in background"}


def start_backfill_if_needed(code: str):
    cache = get_cache_status(code)
    if cache.get("firebase_enabled") and cache.get("stock_daily_count", 0) == 0:
        start_thread(f"backfill-{code}", run_on_demand_backfill, code, 12)
        return True
    return False


@app.get("/")
def root():
    return JSONResponse({"status": "ok", "service": "TW Stock Decision API"}, media_type="application/json; charset=utf-8")


@app.get("/api/search")
def search(q: str):
    return search_products(q)


@app.get("/api/firebase/test")
def firebase_test():
    if db is None:
        return JSONResponse({"status": "failed", "firebase_enabled": False, "message": "Firebase not initialized. Check FIREBASE_KEY in Render Environment."}, media_type="application/json; charset=utf-8")
    try:
        payload = {"status": "ok", "message": "Firebase write test succeeded", "created_at": datetime.now().isoformat()}
        db.collection("system_health").document("test").set(payload)
        return JSONResponse({"status": "ok", "firebase_enabled": True, "write": "system_health/test", "data": payload}, media_type="application/json; charset=utf-8")
    except Exception as exc:
        return JSONResponse({"status": "failed", "firebase_enabled": False, "error": str(exc)}, media_type="application/json; charset=utf-8")


@app.get("/api/firebase/cleanup/{stock}")
def cleanup_stock_cache(stock: str, limit: int = 500):
    return cleanup_invalid_stock_daily(normalize_stock(stock), limit=limit)


@app.get("/api/job/daily")
def trigger_daily():
    return start_thread("daily-update", run_daily_update)


@app.get("/api/job/preload")
def trigger_preload():
    return start_thread("preload-hot-stocks", preload_hot_stocks)


@app.get("/api/job/backfill/{stock}")
def trigger_backfill(stock: str, months: int = 12):
    code = normalize_stock(stock)
    return start_thread(f"backfill-{code}", run_on_demand_backfill, code, months)


@app.get("/api/cache/status/{stock}")
def cache_status(stock: str):
    return get_cache_status(normalize_stock(stock))


@app.get("/api/kline/{stock}")
def kline(stock: str):
    code = normalize_stock(stock)
    df, rows = get_firebase_history(code)
    if df.empty:
        started = start_backfill_if_needed(code)
        return JSONResponse({
            "status": "loading",
            "message": "Firebase 尚無有效K線資料，已啟動背景 backfill。",
            "stock": stock,
            "normalized_stock": code,
            "meta": build_meta(code, [], "Firebase stock_daily"),
            "source": "Firebase stock_daily",
            "data": [],
            "backfill_started": started,
        }, media_type="application/json; charset=utf-8")
    data = to_kline_payload(df)
    meta = build_meta(code, data, "Firebase stock_daily")
    return JSONResponse({"status": "ok", "message": "ok", "stock": stock, "normalized_stock": code, "meta": meta, "source": "Firebase stock_daily", "last_close": meta.get("close"), "last_date": data[-1]["time"] if data else None, "data": data, "cache_rows": len(rows)}, media_type="application/json; charset=utf-8")


@app.get("/api/analysis/{stock}")
def analysis(stock: str):
    code = normalize_stock(stock)
    df, _ = get_firebase_history(code)
    source = "Firebase stock_daily"
    if df.empty:
        df = fallback_history(code)
        source = "fallback-demo-data"
    result = build_rule_based_analysis(df, code)
    data = to_kline_payload(df)
    result.update({"source": source, "normalized_stock": code, "meta": build_meta(code, data, source)})
    return JSONResponse(result, media_type="application/json; charset=utf-8")


@app.get("/api/dashboard/{stock}")
def dashboard(stock: str):
    code = normalize_stock(stock)
    df, _ = get_firebase_history(code)
    source = "Firebase stock_daily" if not df.empty else "fallback-demo-data"
    if df.empty:
        df = fallback_history(code)
    kline_data = to_kline_payload(df)
    analysis_result = build_rule_based_analysis(df, code)
    basic = build_meta(code, kline_data, source)
    chip = get_latest_chip_daily(code)
    realtime = fetch_realtime_board(code) if fetch_realtime_board else basic
    inst = fetch_institutional(code) if fetch_institutional else {}
    margin = fetch_margin(code) if fetch_margin else {}
    board = analyze_dashboard(code, kline_data, analysis_result, realtime, inst, margin) if analyze_dashboard else {}
    return JSONResponse({"basic": {**basic, **(realtime or {})}, "kline": kline_data, "analysis": analysis_result, "dashboard": board, "chip": chip, "source": source}, media_type="application/json; charset=utf-8")
