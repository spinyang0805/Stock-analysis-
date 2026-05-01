from datetime import datetime
import requests

from firebase_cache import save_stock_daily, save_chip_daily, save_job_log

TWSE_ALL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL"
TWSE_T86 = "https://www.twse.com.tw/rwd/zh/fund/T86"
TWSE_MARGIN = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"


def today_str():
    return datetime.now().strftime("%Y%m%d")


def run_daily_update():
    date = today_str()
    result = {"date": date, "stocks": 0, "chips": 0, "errors": []}

    # 1. 成交資料（全市場）
    try:
        res = requests.get(TWSE_ALL, params={"response": "json"}, timeout=20)
        data = res.json().get("data", [])
        for row in data:
            stock_id = row[0]
            payload = {
                "close": float(row[2]),
                "change": float(row[3].replace("+", "")),
                "volume": float(row[5].replace(",", "")),
            }
            save_stock_daily(stock_id, date, payload)
            result["stocks"] += 1
    except Exception as e:
        result["errors"].append(str(e))

    # 2. 三大法人
    try:
        res = requests.get(TWSE_T86, params={"response": "json", "date": date, "selectType": "ALL"}, timeout=20)
        data = res.json().get("data", [])
        for row in data:
            stock_id = row[0]
            payload = {
                "foreign": int(row[4].replace(",", "")),
                "investment_trust": int(row[10].replace(",", "")),
                "dealer": int(row[11].replace(",", "")),
            }
            save_chip_daily(stock_id, date, payload)
            result["chips"] += 1
    except Exception as e:
        result["errors"].append(str(e))

    # 3. 融資融券
    try:
        res = requests.get(TWSE_MARGIN, params={"response": "json", "date": date, "selectType": "ALL"}, timeout=20)
        data = res.json().get("data", [])
        for row in data:
            stock_id = row[0]
            payload = {
                "margin": int(row[12].replace(",", "")),
                "short": int(row[15].replace(",", "")),
            }
            save_chip_daily(stock_id, date, payload)
    except Exception as e:
        result["errors"].append(str(e))

    save_job_log("daily_update_" + date, result)
    return result
