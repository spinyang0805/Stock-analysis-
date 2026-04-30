from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/quote/{stock}")
def get_quote(stock: str):
    try:
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_{stock}.tw"
        res = requests.get(url)
        data = res.json()
        if data.get("msgArray"):
            q = data["msgArray"][0]
            return {
                "price": q.get("z"),
                "open": q.get("o"),
                "high": q.get("h"),
                "low": q.get("l"),
                "volume": q.get("v"),
                "time": q.get("t"),
            }
        return {"error": "no data"}
    except Exception as e:
        return {"error": str(e)}