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
from stock_list import search_products

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

# 🔥 NEW SEARCH API
@app.get("/api/search")
def search(q: str):
    return search_products(q)

# ...（其餘原本程式保留不變）
