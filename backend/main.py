from datetime import datetime, timedelta
import math
import random
import threading
import time

import pandas as pd
import pytz
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from analysis_engine import build_rule_based_analysis, enrich_indicators
from firebase import db
from firebase_cache import (
    audit_stock_daily_market,
    cleanup_invalid_stock_daily,
    get_cache_status,
    get_valid_stock_daily_series,
    get_latest_chip_daily,
    save_analysis_cache,
    save_job_log,
)
from jobs import (
    run_daily_update,
    run_on_demand_backfill,
    preload_hot_stocks,
    run_chip_history_backfill,
    today_str,
    write_margin_chips,
    write_t86_chips,
)
from stock_list import get_all_products, refresh_products_cache, search_products
from perspective_engine import generate_perspective_cards
from rule_engine import build_ai_rule_context
from signal_engine import generate_signals, generate_trade_plan, backtest_strategy
from chip_routes import analyze_chip_rows, read_chip_rows

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

MIN_ANALYSIS_ROWS = 90
RESPONSE_CACHE_TTL_SECONDS = 60
BACKFILL_COOLDOWN_SECONDS = 20 * 60
TW_TZ = pytz.timezone("Asia/Taipei")
_RESPONSE_CACHE = {}
_BACKFILL_LAST_STARTED = {}


def _cache_get(key: str):
    item = _RESPONSE_CACHE.get(key)
    if not item:
        return None
    expires_at, payload = item
    if expires_at < time.time():
        _RESPONSE_CACHE.pop(key, None)
        return None
    return payload


def _cache_set(key: str, payload, ttl: int = RESPONSE_CACHE_TTL_SECONDS):
    _RESPONSE_CACHE[key] = (time.time() + ttl, payload)
    return payload

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
    cleaned = str(mapped).upper().replace(".TW", "").replace(".TWO", "").split()[0]
    # If the result still looks like a name (not a code), try product search
    if not cleaned.replace("A", "").replace("B", "").isdigit():
        try:
            results = search_products(stock, limit=1)
            if results:
                return str(results[0]["code"]).upper()
        except Exception:
            pass
    return cleaned


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


def ensure_analysis_history(code: str):
    df, rows = get_firebase_history(code)
    source = "Firebase stock_daily"
    backfill_started = False
    if len(df) < MIN_ANALYSIS_ROWS:
        backfill_started = start_backfill_if_needed(code)
        if df.empty:
            df = fallback_history(code, 120)
            source = "fallback-demo-data; Firebase backfill started"
        else:
            source = "Firebase stock_daily; backfill started for >=90 rows"
    return df, rows, source, backfill_started


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
            "volume_ma5": safe_float(row.get("V_MA5")),
            "change_pct": safe_float(row.get("CHANGE_PCT")),
            "bb_width": safe_float(row.get("BB_WIDTH")),
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


def is_tw_trading_session():
    now = datetime.now(TW_TZ)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return (9 * 60) <= minutes <= (13 * 60 + 35)


def merge_realtime_into_df(code: str, df: pd.DataFrame):
    if not is_tw_trading_session() or fetch_realtime_board is None:
        return df, None
    realtime = fetch_realtime_board(code)
    if not realtime or realtime.get("source") != "TWSE MIS":
        return df, realtime

    price = safe_float(realtime.get("price") or realtime.get("close"))
    open_price = safe_float(realtime.get("open")) or price
    high = safe_float(realtime.get("high")) or price
    low = safe_float(realtime.get("low")) or price
    if not all(v is not None for v in [price, open_price, high, low]):
        return df, realtime

    today = datetime.now(TW_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    date_key = pd.Timestamp(today.replace(tzinfo=None))
    volume_lot = safe_float(realtime.get("volume_lot"))
    volume = volume_lot * 1000 if volume_lot is not None else 0
    next_df = df.copy()

    if date_key in next_df.index:
        existing = next_df.loc[date_key]
        next_df.loc[date_key, "Open"] = open_price or existing.get("Open")
        next_df.loc[date_key, "High"] = max(high, safe_float(existing.get("High")) or high)
        next_df.loc[date_key, "Low"] = min(low, safe_float(existing.get("Low")) or low)
        next_df.loc[date_key, "Close"] = price
        if volume:
            next_df.loc[date_key, "Volume"] = volume
    else:
        next_df.loc[date_key] = {
            "Open": open_price,
            "High": high,
            "Low": low,
            "Close": price,
            "Volume": volume,
        }
    return next_df.sort_index(), realtime


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
    now = time.time()
    last_started = _BACKFILL_LAST_STARTED.get(code, 0)
    if now - last_started < BACKFILL_COOLDOWN_SECONDS:
        return False
    cache = get_cache_status(code)
    if cache.get("firebase_enabled") and cache.get("stock_daily_count", 0) < MIN_ANALYSIS_ROWS:
        _BACKFILL_LAST_STARTED[code] = now
        start_thread(f"backfill-{code}", run_on_demand_backfill, code, 12)
        return True
    return False


def enrich_analysis_payload(result, code, df, source, chip, backfill_started=False):
    data = to_kline_payload(df)
    perspective_cards = generate_perspective_cards(data, chip or {})
    signals = generate_signals(data, chip or {}) if data else {"signals": [], "risks": [], "action": "HOLD"}
    trade_plan = generate_trade_plan(data) if data else {}
    result.update({
        "source": source,
        "normalized_stock": code,
        "meta": build_meta(code, data, source),
        "perspective_cards": perspective_cards,
        "signals": signals,
        "trade_plan": trade_plan,
        "backfill_started": backfill_started,
        "data_rows": len(data),
        "data_requirement": {"minimum_rows": MIN_ANALYSIS_ROWS, "has_enough_rows": len(data) >= MIN_ANALYSIS_ROWS},
    })
    if db is not None:
        try:
            save_analysis_cache(code, {
                "latest_date": data[-1].get("date") if data else None,
                "perspective_cards": perspective_cards,
                "signals": signals,
                "trade_plan": trade_plan,
                "data_rows": len(data),
            })
        except Exception as exc:
            result["analysis_cache_error"] = str(exc)
    return result, data


def get_chip_context(code: str, limit: int = 60):
    rows = read_chip_rows(code, limit=limit)
    analysis = analyze_chip_rows(rows)
    latest = rows[-1] if rows else (get_latest_chip_daily(code) or {})
    metrics = analysis.get("metrics") if isinstance(analysis, dict) else {}
    chip_for_rules = {**(latest or {}), **(metrics or {})}
    return rows, analysis, chip_for_rules


def has_institutional_values(row):
    if not isinstance(row, dict):
        return False
    return any(row.get(key) is not None for key in ["foreign_buy", "investment_trust_buy", "dealer_buy", "foreign", "investment_trust", "dealer"])


def is_real_chip_row(row):
    return has_institutional_values(row) and row.get("source") != "generated_seed_v1"


def try_refresh_twse_chips():
    result = {"chips": 0, "margin_rows": 0, "errors": []}
    date_text = today_str()
    try:
        write_t86_chips(date_text, result)
        write_margin_chips(date_text, result)
    except Exception as exc:
        result["errors"].append(str(exc))
    return result


def read_chip_payload(code: str, limit: int = 60):
    if db is None:
        return {"status": "failed", "message": "Database not initialized"}
    rows = read_chip_rows(code, limit=limit)
    live_refresh = None
    if not any(is_real_chip_row(row) for row in rows):
        live_refresh = try_refresh_twse_chips()
        rows = read_chip_rows(code, limit=limit)
    real_rows = [row for row in rows if is_real_chip_row(row)]
    analysis_rows = real_rows or rows
    analysis = analyze_chip_rows(analysis_rows)
    latest = analysis_rows[-1] if analysis_rows else (get_latest_chip_daily(code) or {})
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
        "has_institutional_data": any(is_real_chip_row(row) for row in rows),
        "live_refresh": live_refresh,
        "analysis": analysis,
        "updated_at": datetime.now().isoformat(),
    }


def product_universe(product_type: str = "股票", market: str = "all"):
    items = []
    type_filter = str(product_type or "all").strip()
    market_filter = str(market or "all").strip()
    for item in get_all_products():
        if type_filter != "all" and item.get("type") != type_filter:
            continue
        if market_filter != "all" and item.get("market") != market_filter:
            continue
        code = normalize_stock(item.get("code"))
        if not code:
            continue
        items.append({**item, "code": code})
    seen = set()
    result = []
    for item in items:
        if item["code"] in seen:
            continue
        seen.add(item["code"])
        result.append(item)
    return result


def run_backfill_universe(products, months: int = 12):
    result = {"status": "running", "months": months, "total": len(products), "processed": 0, "written_days": 0, "errors": [], "started_at": datetime.now().isoformat()}
    save_job_log("backfill_all_latest", result)
    for item in products:
        code = item["code"]
        try:
            r = run_on_demand_backfill(code, months, item.get("market"), item.get("type"))
            result["processed"] += 1
            result["written_days"] += int(r.get("written_days", 0))
            if r.get("errors"):
                result["errors"].append({"stock_id": code, "errors": r.get("errors", [])[:3]})
        except Exception as exc:
            result["processed"] += 1
            result["errors"].append({"stock_id": code, "error": str(exc)})
        if result["processed"] % 10 == 0:
            save_job_log("backfill_all_latest", {**result, "updated_at": datetime.now().isoformat()})
    result["status"] = "done"
    result["finished_at"] = datetime.now().isoformat()
    save_job_log("backfill_all_latest", result)
    return result


def run_backfill_missing(product_type: str = "all", market: str = "all", months: int = 24, min_rows: int = 30, limit: int = 5000):
    result = {
        "status": "running",
        "mode": "missing_stock_daily",
        "product_type": product_type,
        "market": market,
        "months": months,
        "min_rows": min_rows,
        "limit": limit,
        "universe_count": 0,
        "missing_count": 0,
        "processed": 0,
        "written_days": 0,
        "errors": [],
        "started_at": datetime.now().isoformat(),
    }
    save_job_log("backfill_missing_latest", result)

    refresh_products_cache()
    universe = product_universe(product_type=product_type, market=market)[:limit]
    result["universe_count"] = len(universe)
    missing = []
    for item in universe:
        try:
            rows = get_valid_stock_daily_series(item["code"], limit=min_rows)
            if len(rows) < min_rows:
                missing.append(item)
        except Exception as exc:
            result["errors"].append({"stock_id": item.get("code"), "phase": "scan", "error": str(exc)})
    result["missing_count"] = len(missing)

    for item in missing:
        code = item["code"]
        try:
            r = run_on_demand_backfill(code, months, item.get("market"), item.get("type"))
            result["processed"] += 1
            result["written_days"] += int(r.get("written_days", 0))
            if r.get("errors"):
                result["errors"].append({"stock_id": code, "phase": "backfill", "errors": r.get("errors", [])[:3]})
        except Exception as exc:
            result["processed"] += 1
            result["errors"].append({"stock_id": code, "phase": "backfill", "error": str(exc)})
        if result["processed"] % 10 == 0:
            save_job_log("backfill_missing_latest", {**result, "updated_at": datetime.now().isoformat()})

    result["status"] = "done"
    result["finished_at"] = datetime.now().isoformat()
    save_job_log("backfill_missing_latest", result)
    return result


@app.get("/")
def root():
    return JSONResponse({"status": "ok", "service": "TW Stock Decision API"}, media_type="application/json; charset=utf-8")


@app.get("/api/search")
def search(q: str):
    return search_products(q)


@app.get("/api/products")
def products(product_type: str = "股票", market: str = "all", limit: int = 5000):
    items = product_universe(product_type=product_type, market=market)[:limit]
    return JSONResponse({"count": len(items), "items": items[:200], "note": "items response is capped at 200 preview rows"}, media_type="application/json; charset=utf-8")


@app.get("/api/firebase/test")
def firebase_test():
    if db is None:
        return JSONResponse({"status": "failed", "firebase_enabled": False, "message": "DATABASE_URL not set"}, media_type="application/json; charset=utf-8")
    from firebase_cache import _run
    _, err = _run("SELECT 1", fetch="one")
    if err:
        return JSONResponse({"status": "failed", "firebase_enabled": False, "error": err}, media_type="application/json; charset=utf-8")
    return JSONResponse({"status": "ok", "firebase_enabled": True, "message": "Supabase connection OK", "checked_at": datetime.now().isoformat()}, media_type="application/json; charset=utf-8")


@app.get("/api/firebase/audit_all")
def firebase_audit_all(limit_stocks: int = 5000, limit_per_stock: int = 30):
    return audit_stock_daily_market(limit_stocks=limit_stocks, limit_per_stock=limit_per_stock, delete_invalid=False)


@app.get("/api/firebase/cleanup_all")
def firebase_cleanup_all(limit_stocks: int = 5000, limit_per_stock: int = 260):
    return audit_stock_daily_market(limit_stocks=limit_stocks, limit_per_stock=limit_per_stock, delete_invalid=True)


@app.get("/api/firebase/reset_all")
def firebase_reset_all(product_type: str = "股票", market: str = "all", offset: int = 0, limit: int = 500):
    if db is None:
        return JSONResponse({"status": "failed", "firebase_enabled": False, "message": "Database not initialized"}, media_type="application/json; charset=utf-8")
    from firebase_cache import delete_stock_data
    universe = product_universe(product_type=product_type, market=market)
    batch = universe[offset:offset + limit]
    deleted = 0
    for item in batch:
        delete_stock_data(item["code"])
        deleted += 1
    return JSONResponse({
        "status": "ok",
        "mode": "stock_universe_batch_reset",
        "product_type": product_type,
        "market": market,
        "offset": offset,
        "limit": limit,
        "universe_count": len(universe),
        "processed_count": len(batch),
        "deleted": deleted,
        "next_offset": offset + len(batch) if offset + len(batch) < len(universe) else None,
    }, media_type="application/json; charset=utf-8")


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


@app.get("/api/job/backfill_all")
def trigger_backfill_all(product_type: str = "股票", market: str = "上市", offset: int = 0, limit: int = 100, months: int = 12):
    universe = product_universe(product_type=product_type, market=market)
    batch = universe[offset:offset + limit]
    return start_thread(f"backfill-all-{offset}-{offset + len(batch)}", run_backfill_universe, batch, months)


@app.get("/api/job/backfill_missing")
def trigger_backfill_missing(product_type: str = "all", market: str = "all", limit: int = 5000, months: int = 24, min_rows: int = 30):
    return {
        **start_thread(f"backfill-missing-{product_type}-{market}", run_backfill_missing, product_type, market, months, min_rows, limit),
        "mode": "missing_stock_daily",
        "product_type": product_type,
        "market": market,
        "months": months,
        "min_rows": min_rows,
        "limit": limit,
        "job_log": "job_logs/backfill_missing_latest",
    }


@app.get("/api/job/backfill_all_yearly")
def trigger_backfill_all_yearly(product_type: str = "all", market: str = "all", months: int = 12):
    universe = product_universe(product_type=product_type, market=market)
    return start_thread(f"backfill-all-yearly-{len(universe)}", run_backfill_universe, universe, months)


@app.get("/api/chip/backfill_history_all")
def trigger_chip_history_backfill(months: int = 12, max_days: int = None):
    days = int(max_days or max(20, months * 22))
    return start_thread(f"chip-history-{days}d", run_chip_history_backfill, months, days)


@app.get("/api/chip/{stock}")
def chip(stock: str):
    code = normalize_stock(stock)
    return JSONResponse(jsonable_encoder(read_chip_payload(code)), media_type="application/json; charset=utf-8")


@app.get("/api/cache/status/{stock}")
def cache_status(stock: str):
    return get_cache_status(normalize_stock(stock))


@app.get("/api/kline/{stock}")
def kline(stock: str):
    code = normalize_stock(stock)
    cache_key = f"kline:{code}"
    trading_session = is_tw_trading_session()
    cached = _cache_get(cache_key)
    if cached is not None and not trading_session:
        return JSONResponse({**cached, "cache_hit": True}, media_type="application/json; charset=utf-8")
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
    df, realtime = merge_realtime_into_df(code, df)
    data = to_kline_payload(df)
    source = "Firebase stock_daily + TWSE MIS realtime" if realtime and realtime.get("source") == "TWSE MIS" else "Firebase stock_daily"
    meta = build_meta(code, data, source)
    if realtime and realtime.get("source") == "TWSE MIS":
        meta.update({k: realtime.get(k) for k in ["price", "previous_close", "change", "change_pct", "open", "high", "low", "close", "volume_lot", "time"] if realtime.get(k) is not None})
    if len(data) < MIN_ANALYSIS_ROWS:
        start_backfill_if_needed(code)
    payload = {"status": "ok", "message": "ok", "stock": stock, "normalized_stock": code, "meta": meta, "source": source, "realtime": realtime, "last_close": meta.get("close"), "last_date": data[-1]["time"] if data else None, "data": data, "cache_rows": len(rows), "data_requirement": {"minimum_rows": MIN_ANALYSIS_ROWS, "has_enough_rows": len(data) >= MIN_ANALYSIS_ROWS}}
    _cache_set(cache_key, payload, ttl=5 if trading_session else RESPONSE_CACHE_TTL_SECONDS)
    return JSONResponse(payload, media_type="application/json; charset=utf-8")


@app.get("/api/analysis/{stock}")
def analysis(stock: str):
    code = normalize_stock(stock)
    cache_key = f"analysis:{code}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return JSONResponse({**cached, "cache_hit": True}, media_type="application/json; charset=utf-8")
    df, _, source, backfill_started = ensure_analysis_history(code)
    _, _, chip = get_chip_context(code)
    result = build_rule_based_analysis(df, code)
    result, _ = enrich_analysis_payload(result, code, df, source, chip, backfill_started)
    _cache_set(cache_key, result)
    return JSONResponse(result, media_type="application/json; charset=utf-8")


@app.get("/api/dashboard/{stock}")
def dashboard(stock: str):
    code = normalize_stock(stock)
    df, _, source, backfill_started = ensure_analysis_history(code)
    kline_data = to_kline_payload(df)
    chip_rows, chip_analysis, chip = get_chip_context(code)
    analysis_result = build_rule_based_analysis(df, code)
    analysis_result, kline_data = enrich_analysis_payload(analysis_result, code, df, source, chip, backfill_started)
    basic = build_meta(code, kline_data, source)
    realtime = fetch_realtime_board(code) if fetch_realtime_board else basic
    inst = fetch_institutional(code) if fetch_institutional else {}
    margin = fetch_margin(code) if fetch_margin else {}
    board = analyze_dashboard(code, kline_data, analysis_result, realtime, inst, margin) if analyze_dashboard else {}
    return JSONResponse({"basic": {**basic, **(realtime or {})}, "kline": kline_data, "analysis": analysis_result, "dashboard": board, "chip": {"latest_chip": chip_rows[-1] if chip_rows else chip, "rows": chip_rows[-20:], "row_count": len(chip_rows), "analysis": chip_analysis}, "source": source, "backfill_started": backfill_started}, media_type="application/json; charset=utf-8")


@app.get("/api/ai/context/{stock}")
def ai_context(stock: str):
    code = normalize_stock(stock)
    df, _, source, backfill_started = ensure_analysis_history(code)
    chip_rows, chip_analysis, chip = get_chip_context(code, limit=80)
    result = build_rule_based_analysis(df, code)
    result, kline_data = enrich_analysis_payload(result, code, df, source, chip, backfill_started)
    meta = build_meta(code, kline_data, source)
    context = build_ai_rule_context(
        stock=code,
        meta=meta,
        kline=kline_data,
        analysis=result,
        perspective_cards=result.get("perspective_cards") or [],
        signals=result.get("signals") or {},
        trade_plan=result.get("trade_plan") or {},
        chip_rows=chip_rows,
        chip_analysis=chip_analysis,
        source=source,
    )
    return JSONResponse(context, media_type="application/json; charset=utf-8")


@app.get("/api/backtest/{stock}")
def backtest(stock: str):
    code = normalize_stock(stock)
    df, _, source, backfill_started = ensure_analysis_history(code)
    kline_data = to_kline_payload(df)
    result = backtest_strategy(kline_data) if len(kline_data) >= 60 else {"final_capital": 100000, "return_pct": 0, "trades": [], "message": "資料不足，已啟動背景補資料"}
    result.update({"stock": stock, "normalized_stock": code, "source": source, "data_rows": len(kline_data), "backfill_started": backfill_started})
    return JSONResponse(result, media_type="application/json; charset=utf-8")


def _build_groq_prompt(code: str, name: str, kline: list, chip_analysis: dict) -> str:
    latest = kline[-1] if kline else {}
    recent10 = kline[-10:] if len(kline) >= 10 else kline
    metrics = (chip_analysis or {}).get("metrics") or {}

    lines = [f"  {r.get('date','?')}: 開{r.get('open','?')} 高{r.get('high','?')} 低{r.get('low','?')} 收{r.get('close','?')} 量{int((r.get('volume') or 0)/1000)}千張" for r in recent10]

    def f(v, d=1): return f"{float(v):.{d}f}" if v is not None else "N/A"

    ma5, ma20, ma60 = latest.get("ma5"), latest.get("ma20"), latest.get("ma60")
    if ma5 and ma20 and ma60:
        trend = "四線多排（最強多頭）" if ma5 > ma20 > ma60 else "多頭排列" if ma5 > ma20 else "空頭排列" if ma5 < ma20 < ma60 else "均線糾結"
    else:
        trend = "均線資料不足"

    chip_status = (chip_analysis or {}).get("status", "無資料")
    chip_score = (chip_analysis or {}).get("score", "N/A")

    return f"""你是專業台股技術與籌碼分析師。請依據分析框架與以下數據，用繁體中文做深度分析。

【股票】{code} {name}

【近10日K線】
{chr(10).join(lines)}

【最新技術指標】
均線排列：{trend}（MA5={f(ma5)} MA20={f(ma20)} MA60={f(ma60)}）
RSI14：{f(latest.get('rsi14'))}（>70過熱 <30超賣）
MACD柱：{f(latest.get('macd_hist'), 3)}（正值偏多 負值偏空）
布林寬度：{f(latest.get('bb_width'), 4)}（<0.02極度收縮蓄勢 >0.08大幅開口）

【籌碼資料】
外資近5日：{metrics.get('foreign_5d_sum', 0):+.0f}張，連買{metrics.get('foreign_buy_streak', 0)}天
投信近5日：{metrics.get('investment_trust_5d_sum', 0):+.0f}張，連買{metrics.get('investment_trust_buy_streak', 0)}天
自營商近5日：{metrics.get('dealer_5d_sum', 0):+.0f}張
融資餘額：{metrics.get('margin_balance', 'N/A')}　融券餘額：{metrics.get('short_balance', 'N/A')}
券資比：{f(metrics.get('short_margin_ratio'), 1) if metrics.get('short_margin_ratio') else 'N/A'}%
籌碼狀態：{chip_status}（評分 {chip_score}/100）

【分析框架】請依以下五個維度分析：
1. 趨勢研判（均線排列、黃金/死亡交叉、盤整/突破）
2. 量價矩陣（量增價漲=積極買盤、量縮價漲=謹慎、量增價跌=賣壓、量縮價跌=洗盤）
3. 技術指標（RSI超買超賣、MACD動能方向、布林帶開口收縮）
4. 籌碼分析（外資投信方向、信用交易風險、軋空潛力）
5. 綜合結論（多/空/中性判斷 + 關鍵支撐壓力區 + 具體操作建議）

請用繁體中文、約300字，結構分點，給出專業且可操作的分析。"""


@app.get("/api/ai/groq/{stock}")
def groq_analyze(stock: str):
    import os
    import requests as req

    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        return JSONResponse({"error": "GROQ_API_KEY 未設定，請在 Fly.io secrets 加入"}, status_code=503, media_type="application/json; charset=utf-8")

    code = normalize_stock(stock)
    df, _, source, _ = ensure_analysis_history(code)
    kline_data = to_kline_payload(df)
    _, chip_analysis, _ = get_chip_context(code, limit=20)

    info = STOCK_INFO_MAP.get(code, {})
    name = info.get("name", code)
    prompt = _build_groq_prompt(code, name, kline_data, chip_analysis)

    try:
        resp = req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "meta-llama/llama-4-scout-17b-16e-instruct",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1024,
                "temperature": 0.3,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"]
        usage = data.get("usage", {})
        return JSONResponse({
            "stock": code, "name": name,
            "analysis": text,
            "model": data.get("model"),
            "tokens_used": usage.get("total_tokens"),
            "data_rows": len(kline_data),
            "source": source,
        }, media_type="application/json; charset=utf-8")
    except Exception as exc:
        return JSONResponse({"error": str(exc), "stock": code}, status_code=500, media_type="application/json; charset=utf-8")
