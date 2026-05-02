# UPGRADED VERSION
from datetime import datetime
import time, requests
from firebase_cache import save_stock_daily, save_job_log

HEADERS={"User-Agent":"Mozilla/5.0"}
TWSE_ALL="https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL"
TPEX_ALL="https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes"

HOT_STOCKS=["2330","2317","3702","2454","2382"]

def today():
    return datetime.now().strftime("%Y%m%d")

# 每日更新（上市 + 上櫃）
def run_daily_update():
    d=today()
    result={"date":d,"stocks":0,"errors":[]}

    try:
        data=requests.get(TWSE_ALL,params={"response":"json"},headers=HEADERS,timeout=10).json().get("data",[])
        for r in data:
            save_stock_daily(r[0],d,{"close":float(r[2]),"market":"TWSE"})
            result["stocks"]+=1
    except Exception as e:
        result["errors"].append(str(e))

    try:
        data=requests.get(TPEX_ALL,params={"response":"json"},headers=HEADERS,timeout=10).json().get("data",[])
        for r in data:
            save_stock_daily(r[0],d,{"close":float(r[2]),"market":"TPEx"})
            result["stocks"]+=1
    except Exception as e:
        result["errors"].append(str(e))

    save_job_log("daily_"+d,result)
    return result

# 熱門股票預載
def preload_hot_stocks():
    d=today()
    for s in HOT_STOCKS:
        try:
            save_stock_daily(s,d,{"preload":True})
            time.sleep(0.2)
        except:
            pass
    return {"status":"ok","count":len(HOT_STOCKS)}
