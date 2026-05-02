from datetime import datetime, timedelta
from typing import Tuple
import math
import random
import time
import threading

import pandas as pd
import requests
import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from analysis_engine import build_rule_based_analysis, enrich_indicators
from firebase import db
from firebase_cache import get_cache_status
from jobs import run_daily_update, run_on_demand_backfill, preload_hot_stocks

try:
    from dashboard_service import fetch_realtime_board, fetch_institutional, fetch_margin, analyze_dashboard
except Exception:
    fetch_realtime_board = fetch_institutional = fetch_margin = analyze_dashboard = None

REQUEST_TIMEOUT = 4
TWSE_MONTH_LIMIT = 4

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
    return STOCK_NAME_MAP.get(stock, stock).upper().replace(".TW", "").replace(".TWO", "")


def parse_number(value):
    try:
        if value in (None, "", "--", "---", "X0.00"):
            return None
        return float(str(value).replace(",", "").replace("+", ""))
    except Exception:
        return None


def roc_to_datetime(roc_date: str) -> datetime:
    y, m, d = roc_date.split("/")
    return datetime(int(y) + 1911, int(m), int(d))


def twse_month_history(stock: str, year: int, month: int) -> pd.DataFrame:
    code = normalize_stock(stock)
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    params = {"response": "json", "date": f"{year}{month:02d}01", "stockNo": code}
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
    res = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()
    payload = res.json()
    if payload.get("stat") != "OK" or not payload.get("data"):
        raise ValueError(f"TWSE no data for {code} {year}-{month:02d}")
    rows = []
    for row in payload["data"]:
        try:
            rows.append({
                "Date": roc_to_datetime(row[0]),
                "Open": parse_number(row[3]),
                "High": parse_number(row[4]),
                "Low": parse_number(row[5]),
                "Close": parse_number(row[6]),
                "Volume": parse_number(row[1]),
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


def twse_history(stock: str, months: int = TWSE_MONTH_LIMIT) -> pd.DataFrame:
    today = datetime.now()
    frames = []
    for offset in range(months):
        m = today.month - offset
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        try:
            frames.append(twse_month_history(stock, y, m))
        except Exception:
            continue
    if not frames:
        raise ValueError("TWSE monthly history returned no frames")
    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    df = df.drop_duplicates(subset=["Date"]).sort_values("Date")
    return df.set_index("Date")


def yahoo_chart_history(symbol: str) -> pd.DataFrame:
    now = int(time.time())
    start = now - 370 * 24 * 60 * 60
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"period1": start, "period2": now, "interval": "1d", "events": "history", "includeAdjustedClose": "true"}
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
    res = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()
    payload = res.json()
    result = payload.get("chart", {}).get("result", [])
    if not result:
        raise ValueError("Yahoo chart API returned no result")
    result = result[0]
    timestamps = result.get("timestamp") or []
    quote = (result.get("indicators", {}).get("quote") or [{}])[0]
    rows = []
    for idx, ts in enumerate(timestamps):
        o = quote.get("open", [None] * len(timestamps))[idx]
        h = quote.get("high", [None] * len(timestamps))[idx]
        l = quote.get("low", [None] * len(timestamps))[idx]
        c = quote.get("close", [None] * len(timestamps))[idx]
        v = quote.get("volume", [0] * len(timestamps))[idx]
        if None in (o, h, l, c):
            continue
        rows.append({"Date": datetime.fromtimestamp(ts), "Open": float(o), "High": float(h), "Low": float(l), "Close": float(c), "Volume": float(v or 0)})
    if not rows:
        raise ValueError("Yahoo chart API returned empty OHLCV rows")
    return pd.DataFrame(rows).set_index("Date")


def fallback_history(stock: str, days: int = 180) -> pd.DataFrame:
    random.seed(normalize_stock(stock))
    code = normalize_stock(stock)
    base_map = {"2330": 600, "3702": 100, "2317": 150, "2454": 900}
    base = base_map.get(code, 100 + (sum(ord(c) for c in code) % 200))
    rows, price = [], float(base)
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


def get_history(stock: str) -> Tuple[pd.DataFrame, str, str]:
    code = normalize_stock(stock)
    errors = []
    for getter, label in [
        (lambda: yahoo_chart_history(f"{code}.TW"), "Yahoo Finance chart API"),
        (lambda: yahoo_chart_history(f"{code}.TWO"), "Yahoo Finance chart API TPEx"),
        (lambda: twse_history(code), "TWSE official STOCK_DAY"),
    ]:
        try:
            df = getter()
            if df is not None and not df.empty:
                return df, label, code
        except Exception as exc:
            errors.append(str(exc))
    try:
        df = yf.download(f"{code}.TW", period="6mo", interval="1d", progress=False, auto_adjust=False, threads=False, timeout=REQUEST_TIMEOUT)
        if df is not None and not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]
            return df, "Yahoo Finance / yfinance", code
    except Exception as exc:
        errors.append(str(exc))
    df = fallback_history(code)
    df.attrs["last_error"] = " | ".join(errors[-3:])
    return df, "fallback-demo-data", code


def safe_float(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def to_kline_payload(df: pd.DataFrame):
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


def build_meta(stock: str, data, source: str, resolved_symbol: str):
    code = normalize_stock(stock)
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
        "resolved_symbol": resolved_symbol,
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


def start_backfill_if_needed(code: str):
    cache = get_cache_status(code)
    if cache.get("firebase_enabled") and cache.get("stock_daily_count", 0) == 0:
        threading.Thread(target=lambda: run_on_demand_backfill(code, months=12), daemon=True).start()
        return True
    return False


@app.get("/")
def root():
    return JSONResponse({"status": "ok", "service": "TW Stock Decision API"}, media_type="application/json; charset=utf-8")


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


@app.get("/api/job/daily")
def trigger_daily():
    return run_daily_update()


@app.get("/api/job/preload")
def trigger_preload():
    return preload_hot_stocks()


@app.get("/api/job/backfill/{stock}")
def trigger_backfill(stock: str, months: int = 12):
    return run_on_demand_backfill(normalize_stock(stock), months=months)


@app.get("/api/cache/status/{stock}")
def cache_status(stock: str):
    return get_cache_status(normalize_stock(stock))


@app.get("/api/kline/{stock}")
def kline(stock: str):
    code = normalize_stock(stock)
    backfill_started = start_backfill_if_needed(code)
    try:
        df, source, resolved_symbol = get_history(code)
        data = to_kline_payload(df)
        meta = build_meta(code, data, source, resolved_symbol)
        return JSONResponse({"status": "loading" if backfill_started else "ok", "message": "資料導入中，已先顯示可用資料" if backfill_started else "ok", "stock": stock, "normalized_stock": code, "meta": meta, "resolved_symbol": resolved_symbol, "source": source, "last_close": meta.get("close"), "last_date": data[-1]["time"] if data else None, "data": data}, media_type="application/json; charset=utf-8")
    except Exception as exc:
        df = fallback_history(code)
        data = to_kline_payload(df)
        meta = build_meta(code, data, "emergency-fallback", code)
        return JSONResponse({"status": "loading" if backfill_started else "fallback", "stock": stock, "normalized_stock": code, "meta": meta, "source": "emergency-fallback", "error": str(exc), "data": data}, media_type="application/json; charset=utf-8")


@app.get("/api/analysis/{stock}")
def analysis(stock: str):
    code = normalize_stock(stock)
    try:
        df, source, resolved_symbol = get_history(code)
        result = build_rule_based_analysis(df, code)
        data = to_kline_payload(df)
        result.update({"source": source, "resolved_symbol": resolved_symbol, "normalized_stock": code, "meta": build_meta(code, data, source, resolved_symbol)})
        if source == "fallback-demo-data" and df.attrs.get("last_error"):
            result.setdefault("missing_data", []).append(f"資料源錯誤：{df.attrs.get('last_error')}")
        return JSONResponse(result, media_type="application/json; charset=utf-8")
    except Exception as exc:
        df = fallback_history(code)
        result = build_rule_based_analysis(df, code)
        data = to_kline_payload(df)
        result.update({"source": "emergency-fallback", "resolved_symbol": code, "normalized_stock": code, "meta": build_meta(code, data, "emergency-fallback", code)})
        result.setdefault("missing_data", []).append(f"後端分析已快速回退，不阻塞前端：{exc}")
        return JSONResponse(result, media_type="application/json; charset=utf-8")


@app.get("/api/dashboard/{stock}")
def dashboard(stock: str):
    code = normalize_stock(stock)
    df, source, resolved_symbol = get_history(code)
    kline_data = to_kline_payload(df)
    analysis_result = build_rule_based_analysis(df, code)
    realtime = fetch_realtime_board(code) if fetch_realtime_board else build_meta(code, kline_data, source, resolved_symbol)
    inst = fetch_institutional(code) if fetch_institutional else {}
    margin = fetch_margin(code) if fetch_margin else {}
    board = analyze_dashboard(code, kline_data, analysis_result, realtime, inst, margin) if analyze_dashboard else {}
    return JSONResponse({"basic": realtime, "kline": kline_data, "analysis": analysis_result, "dashboard": board, "source": source, "resolved_symbol": resolved_symbol}, media_type="application/json; charset=utf-8")
