from datetime import datetime, timedelta
import time
import requests
import urllib3

from firebase_cache import save_stock_daily, save_chip_daily, save_job_log

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REQUEST_TIMEOUT = 20
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}

TWSE_ALL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL"
TWSE_T86 = "https://www.twse.com.tw/rwd/zh/fund/T86"
TWSE_MARGIN = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TWSE_STOCK_DAY = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TPEX_ALL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes"

HOT_STOCKS = ["2330", "2317", "3702", "2454", "2382"]


def fetch_json(url, params=None):
    """Render sometimes fails CA validation against TWSE/TPEx.
    Try normal TLS first; if certificate verification fails, retry without verify
    so the job can still populate Firebase. Errors are surfaced to job_logs.
    """
    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        return res.json(), None
    except requests.exceptions.SSLError as ssl_exc:
        try:
            res = requests.get(url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
            res.raise_for_status()
            return res.json(), f"SSL fallback used for {url}: {ssl_exc}"
        except Exception as exc:
            return {}, f"SSL fallback failed for {url}: {exc}"
    except Exception as exc:
        return {}, str(exc)


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


def roc_date_slash(date_text: str) -> str:
    dt = datetime.strptime(date_text, "%Y%m%d")
    return f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"


def roc_to_yyyymmdd(roc_date: str) -> str:
    y, m, d = roc_date.split("/")
    return f"{int(y) + 1911}{int(m):02d}{int(d):02d}"


def recent_dates(max_days: int = 10):
    now = datetime.now()
    for i in range(max_days):
        yield (now - timedelta(days=i)).strftime("%Y%m%d")


def month_iter(months: int = 12):
    now = datetime.now()
    for i in range(months):
        m = now.month - i
        y = now.year
        while m <= 0:
            m += 12
            y -= 1
        yield y, m


def latest_twse_daily_rows(max_lookback_days: int = 10):
    errors = []
    for d in recent_dates(max_lookback_days):
        payload, err = fetch_json(TWSE_ALL, params={"response": "json", "date": d})
        if err:
            errors.append(f"TWSE {d}: {err}")
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if rows:
            return d, rows, errors
    return today_str(), [], errors


def latest_tpex_daily_rows(max_lookback_days: int = 10):
    errors = []
    for d in recent_dates(max_lookback_days):
        for params in [
            {"response": "json", "date": roc_date_slash(d)},
            {"response": "json", "date": d},
            {"response": "json"},
        ]:
            payload, err = fetch_json(TPEX_ALL, params=params)
            if err:
                errors.append(f"TPEx {d}: {err}")
            rows = []
            if isinstance(payload, dict):
                rows = payload.get("data", [])
                if not rows and isinstance(payload.get("tables"), list):
                    rows = payload.get("tables", [{}])[0].get("data", [])
            if rows:
                return d, rows, errors
    return today_str(), [], errors


def fetch_twse_stock_month(stock_id: str, year: int, month: int):
    errors = []
    written = 0
    payload, err = fetch_json(
        TWSE_STOCK_DAY,
        params={"response": "json", "date": f"{year}{month:02d}01", "stockNo": stock_id},
    )
    if err:
        errors.append(f"stock month error {stock_id}: {err}")
    if not isinstance(payload, dict) or payload.get("stat") != "OK":
        return 0, errors
    for row in payload.get("data", []):
        try:
            date_text = roc_to_yyyymmdd(row[0])
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
            if save_stock_daily(stock_id, date_text, doc):
                written += 1
        except Exception as exc:
            errors.append(f"stock month row error {stock_id}: {exc}")
    return written, errors


def run_on_demand_backfill(stock_id: str, months: int = 12):
    stock_id = str(stock_id).strip()
    result = {"stock_id": stock_id, "months": months, "written_days": 0, "errors": [], "status": "running"}
    for year, month in month_iter(months):
        written, errors = fetch_twse_stock_month(stock_id, year, month)
        result["written_days"] += written
        result["errors"].extend(errors[:10])
        time.sleep(0.12)
    result["status"] = "done"
    save_job_log(f"on_demand_backfill_{stock_id}_{today_str()}", result)
    return result


def run_daily_update():
    requested_date = today_str()
    result = {"requested_date": requested_date, "twse_date": None, "tpex_date": None, "stocks": 0, "chips": 0, "errors": []}

    twse_date, twse_rows, twse_errors = latest_twse_daily_rows()
    result["twse_date"] = twse_date
    result["errors"].extend(twse_errors[-5:])
    for row in twse_rows:
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
                "data_date": twse_date,
            }
            if save_stock_daily(stock_id, twse_date, payload):
                result["stocks"] += 1
        except Exception as exc:
            result["errors"].append(f"TWSE daily row error: {exc}")

    tpex_date, tpex_rows, tpex_errors = latest_tpex_daily_rows()
    result["tpex_date"] = tpex_date
    result["errors"].extend(tpex_errors[-5:])
    for row in tpex_rows:
        try:
            stock_id = str(row[0]).strip()
            if not stock_id or not stock_id[:1].isdigit():
                continue
            payload = {
                "market": "TPEx",
                "name": row[1] if len(row) > 1 else None,
                "close": safe_float(row[2]) if len(row) > 2 else None,
                "change": safe_float(row[3]) if len(row) > 3 else None,
                "open": safe_float(row[4]) if len(row) > 4 else None,
                "high": safe_float(row[5]) if len(row) > 5 else None,
                "low": safe_float(row[6]) if len(row) > 6 else None,
                "volume": safe_float(row[8]) if len(row) > 8 else None,
                "source": "TPEx dailyCloseQuotes",
                "data_date": tpex_date,
            }
            if save_stock_daily(stock_id, tpex_date, payload):
                result["stocks"] += 1
        except Exception as exc:
            result["errors"].append(f"TPEx daily row error: {exc}")

    payload, err = fetch_json(TWSE_T86, params={"response": "json", "date": twse_date, "selectType": "ALL"})
    if err:
        result["errors"].append(f"T86 error: {err}")
    for row in payload.get("data", []) if isinstance(payload, dict) else []:
        try:
            stock_id = row[0]
            if save_chip_daily(stock_id, twse_date, {"market": "TWSE", "foreign": safe_int(row[4]), "investment_trust": safe_int(row[10]), "dealer": safe_int(row[11]), "source_t86": "TWSE T86"}):
                result["chips"] += 1
        except Exception as exc:
            result["errors"].append(f"T86 row error: {exc}")

    payload, err = fetch_json(TWSE_MARGIN, params={"response": "json", "date": twse_date, "selectType": "ALL"})
    if err:
        result["errors"].append(f"margin error: {err}")
    for row in payload.get("data", []) if isinstance(payload, dict) else []:
        try:
            stock_id = row[0]
            save_chip_daily(stock_id, twse_date, {"market": "TWSE", "margin": safe_int(row[12]), "short": safe_int(row[15]), "source_margin": "TWSE MI_MARGN"})
        except Exception as exc:
            result["errors"].append(f"margin row error: {exc}")

    save_job_log("daily_update_" + requested_date, result)
    return result


def preload_hot_stocks():
    result = {"status": "ok", "stocks": HOT_STOCKS, "results": []}
    for stock_id in HOT_STOCKS:
        try:
            result["results"].append(run_on_demand_backfill(stock_id, months=1))
        except Exception as exc:
            result["results"].append({"stock_id": stock_id, "error": str(exc)})
        time.sleep(0.2)
    save_job_log("preload_hot_stocks_" + today_str(), result)
    return result
