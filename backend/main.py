from datetime import datetime, timedelta
from typing import Tuple
import math
import random
import time

import pandas as pd
import requests
import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from analysis_engine import build_rule_based_analysis, enrich_indicators

app = FastAPI(title="TW Stock Decision API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STOCK_NAME_MAP = {
    "台積電": "2330",
    "鴻海": "2317",
    "聯發科": "2454",
    "聯發": "2454",
    "大聯大": "3702",
    "廣達": "2382",
    "緯創": "3231",
    "仁寶": "2324",
    "台達電": "2308",
    "華碩": "2357",
}


def normalize_stock(stock: str) -> str:
    stock = str(stock).strip()
    return STOCK_NAME_MAP.get(stock, stock).upper().replace(".TW", "").replace(".TWO", "")


def candidate_symbols(stock: str):
    code = normalize_stock(stock)
    # Listed stocks first, then OTC.
    return [f"{code}.TW", f"{code}.TWO"]


def yahoo_chart_history(symbol: str) -> pd.DataFrame:
    """Fetch OHLCV directly from Yahoo Finance chart API.

    This avoids some Render/yfinance issues and keeps Taiwan prices accurate, e.g. 大聯大 -> 3702.TW.
    """
    now = int(time.time())
    start = now - 370 * 24 * 60 * 60
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "period1": start,
        "period2": now,
        "interval": "1d",
        "events": "history",
        "includeAdjustedClose": "true",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }
    res = requests.get(url, params=params, headers=headers, timeout=15)
    res.raise_for_status()
    payload = res.json()
    result = payload.get("chart", {}).get("result", [])
    if not result:
        raise ValueError("Yahoo chart API returned no result")

    result = result[0]
    timestamps = result.get("timestamp") or []
    quote = (result.get("indicators", {}).get("quote") or [{}])[0]
    adjclose = (result.get("indicators", {}).get("adjclose") or [{}])[0].get("adjclose")

    rows = []
    for idx, ts in enumerate(timestamps):
        open_price = quote.get("open", [None] * len(timestamps))[idx]
        high = quote.get("high", [None] * len(timestamps))[idx]
        low = quote.get("low", [None] * len(timestamps))[idx]
        close = quote.get("close", [None] * len(timestamps))[idx]
        volume = quote.get("volume", [0] * len(timestamps))[idx]
        if None in (open_price, high, low, close):
            continue
        rows.append({
            "Date": datetime.fromtimestamp(ts),
            "Open": float(open_price),
            "High": float(high),
            "Low": float(low),
            "Close": float(close),
            "Adj Close": float(adjclose[idx]) if adjclose and idx < len(adjclose) and adjclose[idx] is not None else float(close),
            "Volume": float(volume or 0),
        })
    if not rows:
        raise ValueError("Yahoo chart API returned empty OHLCV rows")
    return pd.DataFrame(rows).set_index("Date")


def fallback_history(stock: str, days: int = 260) -> pd.DataFrame:
    random.seed(stock)
    code = normalize_stock(stock)
    base_map = {"2330": 600, "3702": 80, "2317": 150, "2454": 900}
    base = base_map.get(code, 100 + (sum(ord(c) for c in code) % 200))
    rows = []
    price = float(base)
    start = datetime.now() - timedelta(days=days * 1.45)
    current = start
    while len(rows) < days:
        current += timedelta(days=1)
        if current.weekday() >= 5:
            continue
        drift = math.sin(len(rows) / 18) * 0.8
        change = random.uniform(-2.2, 2.2) + drift
        open_price = max(1, price + random.uniform(-1.5, 1.5))
        close = max(1, open_price + change)
        high = max(open_price, close) + random.uniform(0.4, 2.2)
        low = min(open_price, close) - random.uniform(0.4, 2.2)
        volume = random.randint(2000, 90000)
        rows.append({"Date": current, "Open": open_price, "High": high, "Low": low, "Close": close, "Volume": volume})
        price = close
    return pd.DataFrame(rows).set_index("Date")


def get_history(stock: str) -> Tuple[pd.DataFrame, str, str]:
    last_error = None
    for symbol in candidate_symbols(stock):
        try:
            df = yahoo_chart_history(symbol)
            if df is not None and not df.empty:
                return df, "Yahoo Finance chart API", symbol
        except Exception as exc:
            last_error = str(exc)

    for symbol in candidate_symbols(stock):
        try:
            df = yf.download(symbol, period="1y", interval="1d", progress=False, auto_adjust=False, threads=False)
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [c[0] for c in df.columns]
                required = {"Open", "High", "Low", "Close"}
                if required.issubset(set(df.columns)):
                    return df, "Yahoo Finance / yfinance", symbol
        except Exception as exc:
            last_error = str(exc)

    df = fallback_history(stock)
    df.attrs["last_error"] = last_error
    return df, "fallback-demo-data", normalize_stock(stock)


def safe_float(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def to_kline_payload(df: pd.DataFrame):
    df = enrich_indicators(df)
    df = df.reset_index()
    date_col = "Date" if "Date" in df.columns else df.columns[0]
    data = []
    for _, row in df.iterrows():
        date_value = pd.to_datetime(row[date_col])
        item = {
            "time": int(date_value.timestamp()),
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


@app.get("/")
def root():
    return JSONResponse({"status": "ok", "service": "TW Stock Decision API"}, media_type="application/json; charset=utf-8")


@app.get("/api/kline/{stock}")
def kline(stock: str):
    df, source, resolved_symbol = get_history(stock)
    data = to_kline_payload(df)
    return JSONResponse({
        "stock": stock,
        "normalized_stock": normalize_stock(stock),
        "resolved_symbol": resolved_symbol,
        "source": source,
        "last_close": data[-1]["close"] if data else None,
        "last_date": data[-1]["time"] if data else None,
        "data": data,
    }, media_type="application/json; charset=utf-8")


@app.get("/api/analysis/{stock}")
def analysis(stock: str):
    df, source, resolved_symbol = get_history(stock)
    result = build_rule_based_analysis(df, normalize_stock(stock))
    result["source"] = source
    result["resolved_symbol"] = resolved_symbol
    result["normalized_stock"] = normalize_stock(stock)
    if source == "fallback-demo-data":
        result.setdefault("missing_data", []).append("Yahoo chart API / yfinance 暫時無法取得資料，已使用後端備援資料維持系統運作")
        if df.attrs.get("last_error"):
            result.setdefault("missing_data", []).append(f"資料源錯誤：{df.attrs.get('last_error')}")
    return JSONResponse(result, media_type="application/json; charset=utf-8")
