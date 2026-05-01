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


def candidate_symbols(stock: str):
    code = normalize_stock(stock)
    return [f"{code}.TW", f"{code}.TWO"]


def roc_to_datetime(roc_date: str) -> datetime:
    parts = roc_date.split("/")
    year = int(parts[0]) + 1911
    return datetime(year, int(parts[1]), int(parts[2]))


def parse_twse_number(value):
    if value in (None, "", "--", "X0.00"):
        return None
    return float(str(value).replace(",", ""))


def twse_month_history(stock: str, year: int, month: int) -> pd.DataFrame:
    code = normalize_stock(stock)
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    params = {"response": "json", "date": f"{year}{month:02d}01", "stockNo": code}
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
    res = requests.get(url, params=params, headers=headers, timeout=15)
    res.raise_for_status()
    payload = res.json()
    if payload.get("stat") != "OK" or not payload.get("data"):
        raise ValueError(f"TWSE no data for {code} {year}-{month:02d}")
    rows = []
    for row in payload["data"]:
        try:
            rows.append({
                "Date": roc_to_datetime(row[0]),
                "Open": parse_twse_number(row[3]),
                "High": parse_twse_number(row[4]),
                "Low": parse_twse_number(row[5]),
                "Close": parse_twse_number(row[6]),
                "Volume": parse_twse_number(row[1]),
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


def twse_history(stock: str, months: int = 14) -> pd.DataFrame:
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


def twse_realtime_quote(stock: str):
    code = normalize_stock(stock)
    ex_ch_candidates = [f"tse_{code}.tw", f"otc_{code}.tw"]
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?stock=" + code}
    for ex_ch in ex_ch_candidates:
        try:
            url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
            params = {"ex_ch": ex_ch, "json": "1", "delay": "0", "_": int(time.time() * 1000)}
            res = requests.get(url, params=params, headers=headers, timeout=10)
            res.raise_for_status()
            payload = res.json()
            rows = payload.get("msgArray") or []
            if not rows:
                continue
            q = rows[0]
            def n(v): return parse_twse_number(v)
            z = n(q.get("z"))
            y = n(q.get("y"))
            o = n(q.get("o"))
            h = n(q.get("h"))
            l = n(q.get("l"))
            v = n(q.get("v"))
            return {
                "code": code,
                "name": q.get("n") or STOCK_INFO_MAP.get(code, {}).get("name") or code,
                "market": "上市" if ex_ch.startswith("tse_") else "上櫃",
                "industry": STOCK_INFO_MAP.get(code, {}).get("industry", ""),
                "price": z,
                "previous_close": y,
                "open": o,
                "high": h,
                "low": l,
                "volume_lot": v,
                "change": None if z is None or y is None else round(z - y, 2),
                "change_pct": None if z is None or y in (None, 0) else round((z - y) / y * 100, 2),
                "time": q.get("t"),
                "source": "TWSE MIS realtime",
            }
        except Exception:
            continue
    return None


def yahoo_chart_history(symbol: str) -> pd.DataFrame:
    now = int(time.time())
    start = now - 370 * 24 * 60 * 60
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"period1": start, "period2": now, "interval": "1d", "events": "history", "includeAdjustedClose": "true"}
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
    res = requests.get(url, params=params, headers=headers, timeout=15)
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


def fallback_history(stock: str, days: int = 260) -> pd.DataFrame:
    random.seed(stock)
    code = normalize_stock(stock)
    base_map = {"2330": 600, "3702": 100, "2317": 150, "2454": 900}
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
    code = normalize_stock(stock)
    try:
        df = twse_history(code)
        if df is not None and not df.empty:
            return df, "TWSE official STOCK_DAY", f"{code}.TW"
    except Exception as exc:
        last_error = str(exc)

    for symbol in candidate_symbols(stock):
        try:
            df = yahoo_chart_history(symbol)
            if df is not None and not df.empty:
                return df, "Yahoo Finance chart API", symbol
        except Exception as exc:
            last_error = str(exc)

    df = fallback_history(stock)
    df.attrs["last_error"] = last_error
    return df, "fallback-demo-data", code


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
        item = {"time": int(date_value.timestamp()), "open": safe_float(row.get("Open")), "high": safe_float(row.get("High")), "low": safe_float(row.get("Low")), "close": safe_float(row.get("Close")), "volume": safe_float(row.get("Volume", 0))}
        for key in ["MA5", "MA10", "MA20", "MA60", "BB_UPPER", "BB_MID", "BB_LOWER", "RSI14", "MACD", "MACD_SIGNAL", "MACD_HIST"]:
            item[key.lower()] = safe_float(row.get(key))
        if all(item[k] is not None for k in ["open", "high", "low", "close"]):
            data.append(item)
    return data


def build_meta(stock: str, data, source: str, resolved_symbol: str):
    code = normalize_stock(stock)
    info = STOCK_INFO_MAP.get(code, {})
    realtime = twse_realtime_quote(code)
    latest = data[-1] if data else {}
    previous = data[-2] if len(data) >= 2 else {}
    price = realtime.get("price") if realtime else None
    close = price if price is not None else latest.get("close")
    prev_close = (realtime.get("previous_close") if realtime else None) or previous.get("close")
    change = None if close is None or prev_close in (None, 0) else round(close - prev_close, 2)
    change_pct = None if change is None or prev_close in (None, 0) else round(change / prev_close * 100, 2)
    return {
        "code": code,
        "name": (realtime.get("name") if realtime else None) or info.get("name") or code,
        "market": (realtime.get("market") if realtime else None) or info.get("market") or "",
        "industry": (realtime.get("industry") if realtime else None) or info.get("industry") or "",
        "resolved_symbol": resolved_symbol,
        "source": source,
        "price": close,
        "open": (realtime.get("open") if realtime else None) or latest.get("open"),
        "high": (realtime.get("high") if realtime else None) or latest.get("high"),
        "low": (realtime.get("low") if realtime else None) or latest.get("low"),
        "close": close,
        "previous_close": prev_close,
        "change": change,
        "change_pct": change_pct,
        "volume": (realtime.get("volume_lot") if realtime else None) or latest.get("volume"),
        "quote_time": realtime.get("time") if realtime else None,
    }


@app.get("/")
def root():
    return JSONResponse({"status": "ok", "service": "TW Stock Decision API"}, media_type="application/json; charset=utf-8")


@app.get("/api/kline/{stock}")
def kline(stock: str):
    df, source, resolved_symbol = get_history(stock)
    data = to_kline_payload(df)
    meta = build_meta(stock, data, source, resolved_symbol)
    return JSONResponse({"stock": stock, "normalized_stock": normalize_stock(stock), "meta": meta, "resolved_symbol": resolved_symbol, "source": source, "last_close": meta.get("close"), "last_date": data[-1]["time"] if data else None, "data": data}, media_type="application/json; charset=utf-8")


@app.get("/api/analysis/{stock}")
def analysis(stock: str):
    df, source, resolved_symbol = get_history(stock)
    result = build_rule_based_analysis(df, normalize_stock(stock))
    data = to_kline_payload(df)
    result["source"] = source
    result["resolved_symbol"] = resolved_symbol
    result["normalized_stock"] = normalize_stock(stock)
    result["meta"] = build_meta(stock, data, source, resolved_symbol)
    if source == "fallback-demo-data":
        result.setdefault("missing_data", []).append("TWSE / Yahoo 暫時無法取得資料，已使用後端備援資料維持系統運作")
        if df.attrs.get("last_error"):
            result.setdefault("missing_data", []).append(f"資料源錯誤：{df.attrs.get('last_error')}")
    return JSONResponse(result, media_type="application/json; charset=utf-8")
