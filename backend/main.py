from datetime import datetime, timedelta
from typing import Tuple
import math
import random

import pandas as pd
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
    return [f"{code}.TW", f"{code}.TWO"]


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
    for symbol in candidate_symbols(stock):
        try:
            df = yf.download(symbol, period="1y", interval="1d", progress=False, auto_adjust=False, threads=False)
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [c[0] for c in df.columns]
                required = {"Open", "High", "Low", "Close"}
                if required.issubset(set(df.columns)):
                    return df, "Yahoo Finance / yfinance", symbol
        except Exception:
            continue
    return fallback_history(stock), "fallback-demo-data", normalize_stock(stock)


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
    return JSONResponse({"stock": stock, "resolved_symbol": resolved_symbol, "source": source, "data": data}, media_type="application/json; charset=utf-8")


@app.get("/api/analysis/{stock}")
def analysis(stock: str):
    df, source, resolved_symbol = get_history(stock)
    result = build_rule_based_analysis(df, normalize_stock(stock))
    result["source"] = source
    result["resolved_symbol"] = resolved_symbol
    if source == "fallback-demo-data":
        result.setdefault("missing_data", []).append("Yahoo/yfinance 暫時無法取得資料，已使用後端備援資料維持系統運作")
    return JSONResponse(result, media_type="application/json; charset=utf-8")
