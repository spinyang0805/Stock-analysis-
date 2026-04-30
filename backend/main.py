from datetime import date, timedelta
from typing import Optional

import requests
import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="TW Stock Watch API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def yahoo_symbol(stock: str, market: str = "TW") -> str:
    stock = stock.upper().replace(".TW", "").replace(".TWO", "")
    suffix = ".TWO" if market.upper() == "TWO" else ".TW"
    return f"{stock}{suffix}"


@app.get("/")
def root():
    return {"status": "ok", "service": "TW Stock Watch API"}


@app.get("/api/realtime/{stock}")
def realtime(stock: str, market: str = "TW"):
    """Realtime/near-realtime quote from Yahoo Finance via yfinance."""
    symbol = yahoo_symbol(stock, market)
    try:
        ticker = yf.Ticker(symbol)
        fast = ticker.fast_info
        return {
            "source": "Yahoo Finance / yfinance",
            "symbol": symbol,
            "stock_id": stock,
            "price": fast.get("last_price"),
            "open": fast.get("open"),
            "high": fast.get("day_high"),
            "low": fast.get("day_low"),
            "previous_close": fast.get("previous_close"),
            "volume": fast.get("last_volume"),
            "currency": fast.get("currency"),
        }
    except Exception as exc:
        return {"source": "Yahoo Finance / yfinance", "symbol": symbol, "error": str(exc)}


@app.get("/api/history/{stock}")
def history(stock: str, start_date: Optional[str] = None):
    """Daily historical data from FinMind for technical analysis."""
    if not start_date:
        start_date = (date.today() - timedelta(days=420)).isoformat()

    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock,
        "start_date": start_date,
    }
    try:
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        return res.json()
    except Exception as exc:
        return {"status": 500, "msg": str(exc), "data": []}
