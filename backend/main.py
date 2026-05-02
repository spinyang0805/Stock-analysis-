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
    get_latest_chip_daily
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

# ... (前面不變略)

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

    # 🔥 新增：從 Firebase 讀最新籌碼
    chip = get_latest_chip_daily(code)

    realtime = fetch_realtime_board(code) if fetch_realtime_board else basic
    inst = fetch_institutional(code) if fetch_institutional else {}
    margin = fetch_margin(code) if fetch_margin else {}
    board = analyze_dashboard(code, kline_data, analysis_result, realtime, inst, margin) if analyze_dashboard else {}

    return JSONResponse({
        "basic": {**basic, **(realtime or {})},
        "kline": kline_data,
        "analysis": analysis_result,
        "dashboard": board,
        "chip": chip,  # ✅ 給前端用
        "source": source
    }, media_type="application/json; charset=utf-8")
