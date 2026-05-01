from datetime import date, timedelta
from typing import Optional

import requests
import yfinance as yf
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="TW Stock Watch API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def yahoo_symbol(stock: str):
    return f"{stock}.TW"

@app.get("/api/kline/{stock}")
def kline(stock: str):
    symbol = yahoo_symbol(stock)
    df = yf.download(symbol, period="6mo", interval="1d")
    df = df.reset_index()
    data = []
    for _, row in df.iterrows():
        data.append({
            "time": int(row["Date"].timestamp()),
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"])
        })
    return data

@app.get("/api/analysis/{stock}")
def analysis(stock: str):
    symbol = yahoo_symbol(stock)
    df = yf.download(symbol, period="3mo")

    df["MA5"] = df["Close"].rolling(5).mean()
    df["MA20"] = df["Close"].rolling(20).mean()

    latest = df.iloc[-1]

    if latest["MA5"] > latest["MA20"]:
        trend = "多頭"
        action = "回檔找買點"
    else:
        trend = "空頭"
        action = "避免追高"

    return {
        "trend": trend,
        "action": action
    }
