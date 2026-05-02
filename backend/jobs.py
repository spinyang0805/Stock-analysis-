from datetime import datetime
import time
import requests

from firebase_cache import save_stock_daily, save_chip_daily, save_job_log

REQUEST_TIMEOUT = 20
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}

TWSE_ALL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL"
TWSE_T86 = "https://www.twse.com.tw/rwd/zh/fund/T86"
TWSE_MARGIN = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TWSE_STOCK_DAY = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"


def today_str():
    return datetime.now().strftime("%Y%m%d")


def safe_float(value):
    try:
        if value in (None, "", "--", "---", "X0.00", "除權息"):
            return None
        return float(str(value).replace(",", "").replace("+", ""))
    except Exception:
        return None


def safe_int(value):
    n = safe_float(value)
    return None if n is None else int(n)


def roc_to_yyyymmdd(roc_date: str) -> str:
    y, m, d = roc_date.split("/")
    return f"{int(y) + 1911}{int(m):02d}{int(d):02d}"


def month_iter(months: int = 12):
    now = datetime.now()
    result = []
    for i in range(months):
        m = now.month - i
        y = now.year
        while m <= 0:
            m += 12
            y -= 1
        result.append((y, m))
    return result


def fetch_twse_stock_month(stock_id: str, year: int, month: int):
    errors = []
    written = 0
    try:
        res = requests.get(
            TWSE_STOCK_DAY,
            params={"response": "json", "date": f"{year}{month:02d}01", "stockNo": stock_id},
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        payload = res.json()
        if payload.get("stat") != "OK":
            return 0, []
        for row in payload.get("data", []):
            try:
                date = roc_to_yyyymmdd(row[0])
                doc = {
                    "market": "TWSE",
                    "volume": safe_float(row[1]),
                    "turnover": safe_float(row[2]),
                    "open": safe_float(row[3]),
                    "high": safe_float(row[4]),
                    "low": safe_float(row[5]),
                    "close": safe_float(row[6]),
                    "change": safe_float(row[7]),
                    "trades": safe_float(row[8]),
                    "source": "TWSE STOCK_DAY",
                }
                save_stock_daily(stock_id, date, doc)
                written += 1
            except Exception as exc:
                errors.append(f"stock month row error {stock_id}: {exc}")
    except Exception as exc:
        errors.append(f"stock month error {stock_id}: {exc}")
    return written, errors


def run_on_demand_backfill(stock_id: str, months: int = 12):
    stock_id = str(stock_id).strip()
    result = {
        "stock_id": stock_id,
        "months": months,
        "written_days": 0,
        "errors": [],
        "status": "running",
    }
    for year, month in month_iter(months):
        written, errors = fetch_twse_stock_month(stock_id, year, month)
        result["written_days"] += written
        result["errors"].extend(errors[:10])
        time.sleep(0.12)
    result["status"] = "done"
    save_job_log(f"on_demand_backfill_{stock_id}_{today_str()}", result)
    return result


def run_daily_update():
    date = today_str()
    result = {"date": date, "stocks": 0, "chips": 0, "errors": []}

    try:
        res = requests.get(TWSE_ALL, params={"response": "json"}, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        data = res.json().get("data", [])
        for row in data:
            try:
                stock_id = row[0]
                payload = {
                    "market": "TWSE",
                    "name": row[1] if len(row) > 1 else None,
                    "close": safe_float(row[2]),
                    "change": safe_float(row[3]),
                    "open": safe_float(row[4]) if len(row) > 4 else None,
                    "high": safe_float(row[5]) if len(row) > 5 else None,
                    "low": safe_float(row[6]) if len(row) > 6 else None,
                    "volume": safe_float(row[8]) if len(row) > 8 else None,
                    "source": "TWSE STOCK_DAY_ALL",
                }
                save_stock_daily(stock_id, date, payload)
                result["stocks"] += 1
            except Exception as exc:
                result["errors"].append(f"daily row error: {exc}")
    except Exception as e:
        result["errors"].append(str(e))

    try:
        res = requests.get(TWSE_T86, params={"response": "json", "date": date, "selectType": "ALL"}, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        data = res.json().get("data", [])
        for row in data:
            stock_id = row[0]
            payload = {
                "market": "TWSE",
                "foreign": safe_int(row[4]),
                "investment_trust": safe_int(row[10]),
                "dealer": safe_int(row[11]),
                "source": "TWSE T86",
            }
            save_chip_daily(stock_id, date, payload)
            result["chips"] += 1
    except Exception as e:
        result["errors"].append(str(e))

    try:
        res = requests.get(TWSE_MARGIN, params={"response": "json", "date": date, "selectType": "ALL"}, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        data = res.json().get("data", [])
        for row in data:
            stock_id = row[0]
            payload = {
                "market": "TWSE",
                "margin": safe_int(row[12]),
                "short": safe_int(row[15]),
                "source": "TWSE MI_MARGN",
            }
            save_chip_daily(stock_id, date, payload)
    except Exception as e:
        result["errors"].append(str(e))

    save_job_log("daily_update_" + date, result)
    return result
