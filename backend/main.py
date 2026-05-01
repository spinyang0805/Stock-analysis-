from datetime import datetime, timedelta
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


def yahoo_symbol(stock: str):
    stock = stock.upper().replace(".TW", "").replace(".TWO", "")
    return f"{stock}.TW"


def fallback_history(stock: str, days: int = 260) -> pd.DataFrame:
    """Fallback OHLCV data so the service and UI still work when free data sources throttle."""
    random.seed(stock)
    base = 600 if stock == "2330" else 100 + (sum(ord(c) for c in stock) % 200)
    rows = []
    price = float(base)
    start = datetime.now() - timedelta(days=days * 1.45)
    current = start
    while len(rows) < days:
        current += timedelta(days=1)
        if current.weekday() >= 5:
            continue
        drift = math.sin(len(rows) / 18) * 2.5
        change = random.uniform(-10, 10) + drift
        open_price = max(1, price + random.uniform(-5, 5))
        close = max(1, open_price + change)
        high = max(open_price, close) + random.uniform(1, 8)
        low = min(open_price, close) - random.uniform(1, 8)
        volume = random.randint(12000, 90000)
        rows.append({
            "Date": current,
            "Open": open_price,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
        })
        price = close
    return pd.DataFrame(rows).set_index("Date")


def get_history(stock: str) -> tuple[pd.DataFrame, str]:
    symbol = yahoo_symbol(stock)
    try:
        df = yf.download(symbol, period="1y", interval="1d", progress=False, auto_adjust=False)
        if df is not None and not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]
            return df, "Yahoo Finance / yfinance"
    except Exception:
        pass
    return fallback_history(stock), "fallback-demo-data"


def to_kline_payload(df: pd.DataFrame):
    df = enrich_indicators(df)
    df = df.reset_index()
    date_col = "Date" if "Date" in df.columns else df.columns[0]
    data = []
    for _, row in df.iterrows():
        date_value = pd.to_datetime(row[date_col])
        item = {
            "time": int(date_value.timestamp()),
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": float(row.get("Volume", 0)),
        }
        for key in ["MA5", "MA10", "MA20", "MA60", "BB_UPPER", "BB_MID", "BB_LOWER", "RSI14", "MACD", "MACD_SIGNAL", "MACD_HIST"]:
            val = row.get(key)
            item[key.lower()] = None if pd.isna(val) else float(val)
        data.append(item)
    return data


@app.get("/")
def root():
    return JSONResponse({"status": "ok", "service": "TW Stock Decision API"}, media_type="application/json; charset=utf-8")


@app.get("/api/kline/{stock}")
def kline(stock: str):
    df, source = get_history(stock)
    data = to_kline_payload(df)
    return JSONResponse(data, media_type="application/json; charset=utf-8")


@app.get("/api/analysis/{stock}")
def analysis(stock: str):
    df, source = get_history(stock)
    result = build_rule_based_analysis(df, stock)
    result["source"] = source
    if source == "fallback-demo-data":
        result.setdefault("missing_data", []).append("yfinance 暫時無法取得資料，已使用後端備援資料維持系統運作")
    return JSONResponse(result, media_type="application/json; charset=utf-8")
