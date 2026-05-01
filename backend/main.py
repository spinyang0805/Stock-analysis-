import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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


@app.get("/")
def root():
    return {"status": "ok", "service": "TW Stock Decision API"}


@app.get("/api/kline/{stock}")
def kline(stock: str):
    symbol = yahoo_symbol(stock)
    df = yf.download(symbol, period="1y", interval="1d", progress=False)
    if df.empty:
        return []

    df = enrich_indicators(df)
    df = df.reset_index()
    data = []
    for _, row in df.iterrows():
        item = {
            "time": int(row["Date"].timestamp()),
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": float(row.get("Volume", 0)),
        }
        for key in ["MA5", "MA10", "MA20", "MA60", "BB_UPPER", "BB_MID", "BB_LOWER", "RSI14", "MACD", "MACD_SIGNAL", "MACD_HIST"]:
            val = row.get(key)
            item[key.lower()] = None if val != val else float(val)
        data.append(item)
    return data


@app.get("/api/analysis/{stock}")
def analysis(stock: str):
    symbol = yahoo_symbol(stock)
    df = yf.download(symbol, period="1y", interval="1d", progress=False)
    if df.empty:
        return {
            "stock": stock,
            "trend": "資料不足",
            "score": 0,
            "rating": "Neutral",
            "summary": "查無資料，請確認股票代號或資料源狀態。",
            "signals": [],
            "indicators": {},
            "missing_data": ["yfinance 未回傳資料"],
        }
    return build_rule_based_analysis(df, stock)
