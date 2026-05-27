from datetime import datetime, timedelta
import time
import requests
import urllib3

from firebase_cache import save_stock_daily, save_chip_daily, save_job_log

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REQUEST_TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}
TPEX_HEADERS = {
    **HEADERS,
    "Referer": "https://www.tpex.org.tw/",
    "Origin": "https://www.tpex.org.tw",
}

TWSE_ALL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL"
TWSE_T86 = "https://www.twse.com.tw/rwd/zh/fund/T86"
TWSE_MARGIN = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TWSE_STOCK_DAY = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
TWSE_STOCK_DAY_LEGACY = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TPEX_ALL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes"
TPEX_STOCK_DAY = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
TPEX_INSTITUTIONAL = "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"
TPEX_MARGIN = "https://www.tpex.org.tw/www/zh-tw/margin/balance"

HOT_STOCKS = ["2330", "2317", "3702", "2454", "2382"]


def fetch_json(url, params=None, headers=None, retries=2):
    h = headers or HEADERS
    last_err = None
    for attempt in range(retries + 1):
        try:
            res = requests.get(url, params=params, headers=h, timeout=REQUEST_TIMEOUT)
            if res.status_code in (520, 521, 522, 523, 524) and attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            res.raise_for_status()
            return res.json(), None
        except requests.exceptions.SSLError as ssl_exc:
            try:
                res = requests.get(url, params=params, headers=h, timeout=REQUEST_TIMEOUT, verify=False)
                if res.status_code in (520, 521, 522, 523, 524) and attempt < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                res.raise_for_status()
                return res.json(), f"SSL fallback: {ssl_exc}"
            except Exception as exc:
                last_err = str(exc)
        except Exception as exc:
            last_err = str(exc)
            if attempt < retries:
                time.sleep(1.0)
    return {}, last_err or "unknown error"


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
    parts = str(roc_date).split("/")
    if len(parts) != 3:
        return str(roc_date).replace("/", "")
    y, m, d = parts
    year = int(y) + 1911 if int(y) < 1911 else int(y)
    return f"{year}{int(m):02d}{int(d):02d}"


def recent_dates(max_days: int = 10):
    now = datetime.now()
    for i in range(max_days):
        yield (now - timedelta(days=i)).strftime("%Y%m%d")


def back_dates_from(date_text: str, max_days: int = 10):
    base = datetime.strptime(date_text, "%Y%m%d")
    for i in range(max_days):
        yield (base - timedelta(days=i)).strftime("%Y%m%d")


def recent_trading_dates(max_days: int = 260):
    d = datetime.now()
    dates = []
    while len(dates) < max_days:
        if d.weekday() < 5:
            dates.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return dates


def month_iter(months: int = 12):
    now = datetime.now()
    for i in range(months):
        m = now.month - i
        y = now.year
        while m <= 0:
            m += 12
            y -= 1
        yield y, m


def _fields(payload):
    if not isinstance(payload, dict):
        return []
    return payload.get("fields") or payload.get("fields9") or []


def _rows(payload):
    if not isinstance(payload, dict):
        return []
    if payload.get("data"):
        return payload.get("data") or []
    if isinstance(payload.get("tables"), list) and payload["tables"]:
        return payload["tables"][0].get("data", []) or []
    return []


def _idx(fields, *keywords, default=None):
    for i, name in enumerate(fields or []):
        text = str(name)
        if all(k in text for k in keywords):
            return i
    return default


def _row_value(row, idx):
    try:
        if idx is None or idx >= len(row):
            return None
        return row[idx]
    except Exception:
        return None


def _table_rows(payload):
    if not isinstance(payload, dict):
        return [], []
    tables = payload.get("tables")
    if isinstance(tables, list) and tables:
        table = tables[0] or {}
        return table.get("data", []) or [], table.get("fields", []) or []
    return _rows(payload), _fields(payload)


def latest_twse_daily_rows(max_lookback_days: int = 10):
    errors = []
    for d in recent_dates(max_lookback_days):
        payload, err = fetch_json(TWSE_ALL, params={"response": "json", "date": d})
        if err:
            errors.append(f"TWSE {d}: {err}")
        rows = _rows(payload)
        fields = _fields(payload)
        if rows:
            return d, rows, fields, errors
    return today_str(), [], [], errors


def latest_tpex_daily_rows(max_lookback_days: int = 10):
    errors = []
    for d in recent_dates(max_lookback_days):
        for params in [
            {"response": "json", "l": "zh-tw", "date": roc_date_slash(d)},
            {"response": "json", "date": roc_date_slash(d)},
            {"response": "json", "l": "zh-tw", "date": d},
            {"response": "json", "date": d},
            {"response": "json"},
        ]:
            payload, err = fetch_json(TPEX_ALL, params=params, headers=TPEX_HEADERS)
            if err:
                errors.append(f"TPEx {d}: {err}")
            rows = _rows(payload)
            fields = _fields(payload)
            if rows:
                return d, rows, fields, errors
    return today_str(), [], [], errors


def fetch_twse_stock_month(stock_id: str, year: int, month: int, product_type: str = "股票"):
    errors = []
    written = 0
    params = {"response": "json", "date": f"{year}{month:02d}01", "stockNo": stock_id}
    payload = {}
    for url in [TWSE_STOCK_DAY, TWSE_STOCK_DAY_LEGACY]:
        payload, err = fetch_json(url, params=params)
        if err:
            errors.append(f"TWSE month {url} {stock_id}: {err}")
        if isinstance(payload, dict) and payload.get("stat") == "OK" and payload.get("data"):
            break
    if not isinstance(payload, dict) or payload.get("stat") != "OK":
        return 0, errors
    fields = _fields(payload)
    date_i    = _idx(fields, "日期", default=0)
    volume_i  = _idx(fields, "成交", "股數", default=1)
    turnover_i= _idx(fields, "成交", "金額", default=2)
    open_i    = _idx(fields, "開盤", default=3)
    high_i    = _idx(fields, "最高", default=4)
    low_i     = _idx(fields, "最低", default=5)
    close_i   = _idx(fields, "收盤", default=6)
    change_i  = _idx(fields, "漲跌", default=7)
    trades_i  = _idx(fields, "成交", "筆數", default=8)
    for row in payload.get("data", []):
        try:
            date_text = roc_to_yyyymmdd(str(_row_value(row, date_i)))
            doc = {
                "market": "TWSE",
                "product_type": product_type,
                "volume": safe_float(_row_value(row, volume_i)),
                "turnover": safe_float(_row_value(row, turnover_i)),
                "open": safe_float(_row_value(row, open_i)),
                "high": safe_float(_row_value(row, high_i)),
                "low": safe_float(_row_value(row, low_i)),
                "close": safe_float(_row_value(row, close_i)),
                "change": safe_float(_row_value(row, change_i)),
                "trades": safe_float(_row_value(row, trades_i)),
                "source": "TWSE STOCK_DAY",
            }
            if save_stock_daily(stock_id, date_text, doc):
                written += 1
        except Exception as exc:
            errors.append(f"TWSE month row error {stock_id}: {exc}")
    return written, errors


def fetch_tpex_stock_month(stock_id: str, year: int, month: int, product_type: str = "股票"):
    errors = []
    written = 0
    roc_ym = f"{year - 1911}/{month:02d}"
    params_candidates = [
        {"response": "json", "l": "zh-tw", "d": roc_ym, "s": f"{stock_id},asc,0"},
        {"response": "json", "l": "zh-tw", "date": roc_ym, "stockNo": stock_id},
        {"response": "json", "date": roc_ym, "stockNo": stock_id},
        {"response": "json", "date": f"{year}{month:02d}", "stockNo": stock_id},
        {"response": "json", "date": f"{roc_ym}/01", "stockNo": stock_id},
    ]
    payload = {}
    err = None
    for params in params_candidates:
        payload, err = fetch_json(TPEX_STOCK_DAY, params=params, headers=TPEX_HEADERS)
        rows = _rows(payload)
        if rows:
            break
    if err:
        errors.append(f"TPEx month error {stock_id}: {err}")
    rows = _rows(payload)
    fields = _fields(payload)
    if not rows:
        return 0, errors or [f"TPEx no rows {stock_id} {year}-{month:02d}"]

    date_i = _idx(fields, "日期", default=0)
    close_i = _idx(fields, "收盤", default=2)
    change_i = _idx(fields, "漲跌", default=3)
    open_i = _idx(fields, "開盤", default=4)
    high_i = _idx(fields, "最高", default=5)
    low_i = _idx(fields, "最低", default=6)
    volume_i = _idx(fields, "成交", default=8)
    turnover_i = _idx(fields, "金額", default=None)
    trades_i = _idx(fields, "筆數", default=None)

    for row in rows:
        try:
            date_text = roc_to_yyyymmdd(str(_row_value(row, date_i)))
            doc = {
                "market": "TPEx",
                "product_type": product_type,
                "volume": safe_float(_row_value(row, volume_i)),
                "turnover": safe_float(_row_value(row, turnover_i)),
                "open": safe_float(_row_value(row, open_i)),
                "high": safe_float(_row_value(row, high_i)),
                "low": safe_float(_row_value(row, low_i)),
                "close": safe_float(_row_value(row, close_i)),
                "change": safe_float(_row_value(row, change_i)),
                "trades": safe_float(_row_value(row, trades_i)),
                "source": "TPEx tradingStock",
            }
            if save_stock_daily(stock_id, date_text, doc):
                written += 1
        except Exception as exc:
            errors.append(f"TPEx month row error {stock_id}: {exc}")
    return written, errors


def run_on_demand_backfill(stock_id: str, months: int = 12, market: str = "TWSE", product_type: str = "股票"):
    stock_id = str(stock_id).strip()
    market_key = "TPEx" if str(market).upper() in ("TPEX", "上櫃") else "TWSE"
    market_text = str(market or "").strip().upper()
    if market_text in ("TPEX", "上櫃", "OTC"):
        market_key = "TPEx"
    result = {"stock_id": stock_id, "market": market_key, "product_type": product_type, "months": months, "written_days": 0, "errors": [], "status": "running"}
    for year, month in month_iter(months):
        if market_key == "TPEx":
            written, errors = fetch_tpex_stock_month(stock_id, year, month, product_type=product_type)
        else:
            written, errors = fetch_twse_stock_month(stock_id, year, month, product_type=product_type)
        result["written_days"] += written
        result["errors"].extend(errors[:10])
        time.sleep(0.12)
    result["status"] = "done"
    save_job_log(f"on_demand_backfill_{stock_id}_{today_str()}", result)
    return result


def write_t86_chips(start_date: str, result: dict):
    for d in back_dates_from(start_date, 10):
        payload, err = fetch_json(TWSE_T86, params={"response": "json", "date": d, "selectType": "ALL"})
        if err:
            result["errors"].append(f"T86 {d}: {err}")
        rows = _rows(payload)
        if not rows:
            continue
        fields = _fields(payload)
        code_i = _idx(fields, "證券", "代號", default=0)
        name_i = _idx(fields, "證券", "名稱", default=1)
        foreign_i = _idx(fields, "外資", "買賣超", default=4)
        trust_i = _idx(fields, "投信", "買賣超", default=7)
        dealer_i = _idx(fields, "自營商", "買賣超", default=8)
        written = 0
        for row in rows:
            try:
                stock_id = str(_row_value(row, code_i)).strip()
                if not stock_id:
                    continue
                payload_doc = {
                    "market": "TWSE",
                    "name": _row_value(row, name_i),
                    "foreign": safe_int(_row_value(row, foreign_i)),
                    "foreign_buy": safe_int(_row_value(row, foreign_i)),
                    "investment_trust": safe_int(_row_value(row, trust_i)),
                    "investment_trust_buy": safe_int(_row_value(row, trust_i)),
                    "dealer": safe_int(_row_value(row, dealer_i)),
                    "dealer_buy": safe_int(_row_value(row, dealer_i)),
                    "source_t86": "TWSE T86",
                    "source": "TWSE T86",
                    "chip_date": d,
                }
                if save_chip_daily(stock_id, d, payload_doc):
                    written += 1
            except Exception as exc:
                result["errors"].append(f"T86 row {d}: {exc}")
        result["chips"] += written
        result["t86_date"] = d
        return written
    return 0


def write_margin_chips(start_date: str, result: dict):
    for d in back_dates_from(start_date, 10):
        payload, err = fetch_json(TWSE_MARGIN, params={"response": "json", "date": d, "selectType": "ALL"})
        if err:
            result["errors"].append(f"margin {d}: {err}")
        rows = _rows(payload)
        if not rows:
            continue
        fields = _fields(payload)
        code_i = _idx(fields, "股票", "代號", default=0)
        if code_i is None:
            code_i = _idx(fields, "證券", "代號", default=0)
        margin_i = _idx(fields, "融資", "今日餘額", default=None)
        if margin_i is None:
            margin_i = _idx(fields, "融資", "餘額", default=6)
        short_i = _idx(fields, "融券", "今日餘額", default=None)
        if short_i is None:
            short_i = _idx(fields, "融券", "餘額", default=12)
        written = 0
        for row in rows:
            try:
                stock_id = str(_row_value(row, code_i)).strip()
                if not stock_id:
                    continue
                payload_doc = {
                    "market": "TWSE",
                    "margin": safe_int(_row_value(row, margin_i)),
                    "margin_balance": safe_int(_row_value(row, margin_i)),
                    "short": safe_int(_row_value(row, short_i)),
                    "short_balance": safe_int(_row_value(row, short_i)),
                    "source_margin": "TWSE MI_MARGN",
                    "source": "TWSE MI_MARGN",
                    "margin_date": d,
                }
                if save_chip_daily(stock_id, d, payload_doc):
                    written += 1
            except Exception as exc:
                result["errors"].append(f"margin row {d}: {exc}")
        result["margin_rows"] = written
        result["margin_date"] = d
        return written
    return 0


def _parse_tpex_insti_row(row):
    if isinstance(row, str):
        parts = row.split()
    elif isinstance(row, list):
        parts = [str(x) for x in row]
    else:
        return None, None
    if len(parts) < 24:
        return None, None
    code = parts[0].strip()
    name = parts[1].strip()
    values = parts[2:]
    return code, {
        "market": "TPEx",
        "name": name,
        "foreign_buy": safe_int(values[8]) if len(values) > 8 else None,
        "investment_trust_buy": safe_int(values[11]) if len(values) > 11 else None,
        "dealer_buy": safe_int(values[20]) if len(values) > 20 else None,
        "institution_total_buy": safe_int(values[23]) if len(values) > 23 else None,
        "source_t86": "TPEx insti/dailyTrade",
        "source": "TPEx insti/dailyTrade",
    }


def write_tpex_insti_chips(date_text: str, result: dict):
    payload, err = fetch_json(TPEX_INSTITUTIONAL, params={"response": "json", "date": date_text, "sect": "AL", "type": "Daily"}, headers=TPEX_HEADERS)
    if err:
        result["errors"].append(f"TPEx insti {date_text}: {err}")
    rows, _ = _table_rows(payload)
    if not rows:
        return 0
    written = 0
    for row in rows:
        try:
            stock_id, payload_doc = _parse_tpex_insti_row(row)
            if stock_id and save_chip_daily(stock_id, date_text, {**payload_doc, "chip_date": date_text}):
                written += 1
        except Exception as exc:
            result["errors"].append(f"TPEx insti row {date_text}: {exc}")
    result["tpex_chips"] = result.get("tpex_chips", 0) + written
    result["tpex_t86_date"] = date_text
    return written


def write_tpex_margin_chips(date_text: str, result: dict):
    payload, err = fetch_json(TPEX_MARGIN, params={"response": "json", "date": date_text}, headers=TPEX_HEADERS)
    if err:
        result["errors"].append(f"TPEx margin {date_text}: {err}")
    rows, fields = _table_rows(payload)
    if not rows:
        return 0
    code_i = _idx(fields, "代號", default=0)
    name_i = _idx(fields, "名稱", default=1)
    margin_i = _idx(fields, "資餘額", default=6)
    short_i = _idx(fields, "券餘額", default=14)
    written = 0
    for row in rows:
        try:
            stock_id = str(_row_value(row, code_i)).strip()
            if not stock_id:
                continue
            payload_doc = {
                "market": "TPEx",
                "name": _row_value(row, name_i),
                "margin": safe_int(_row_value(row, margin_i)),
                "margin_balance": safe_int(_row_value(row, margin_i)),
                "short": safe_int(_row_value(row, short_i)),
                "short_balance": safe_int(_row_value(row, short_i)),
                "source_margin": "TPEx margin/balance",
                "source": "TPEx margin/balance",
                "margin_date": date_text,
            }
            if save_chip_daily(stock_id, date_text, payload_doc):
                written += 1
        except Exception as exc:
            result["errors"].append(f"TPEx margin row {date_text}: {exc}")
    result["tpex_margin_rows"] = result.get("tpex_margin_rows", 0) + written
    result["tpex_margin_date"] = date_text
    return written


def run_chip_history_backfill(months: int = 12, max_days: int = None, sleep_seconds: float = 0.25):
    days = int(max_days or max(20, months * 22))
    result = {
        "status": "running",
        "coverage": "TWSE+TPEx",
        "months": months,
        "target_trading_days": days,
        "processed_dates": 0,
        "t86_written": 0,
        "margin_written": 0,
        "tpex_t86_written": 0,
        "tpex_margin_written": 0,
        "errors": [],
        "started_at": datetime.now().isoformat(),
    }
    save_job_log("chip_history_backfill_latest", result)

    for date_text in recent_trading_dates(days):
        per_day = {"chips": 0, "margin_rows": 0, "errors": []}
        t86_written = write_t86_chips(date_text, per_day)
        margin_written = write_margin_chips(date_text, per_day)
        tpex_t86_written = write_tpex_insti_chips(date_text, per_day)
        tpex_margin_written = write_tpex_margin_chips(date_text, per_day)
        result["processed_dates"] += 1
        result["t86_written"] += int(t86_written or 0)
        result["margin_written"] += int(margin_written or 0)
        result["tpex_t86_written"] += int(tpex_t86_written or 0)
        result["tpex_margin_written"] += int(tpex_margin_written or 0)
        if per_day.get("errors"):
            result["errors"].extend(per_day["errors"][-3:])
            result["errors"] = result["errors"][-100:]
        if result["processed_dates"] % 5 == 0:
            save_job_log("chip_history_backfill_latest", {**result, "updated_at": datetime.now().isoformat()})
        time.sleep(sleep_seconds)

    result["status"] = "done"
    result["finished_at"] = datetime.now().isoformat()
    save_job_log("chip_history_backfill_latest", result)
    return result


def _parse_twse_all_row(row, fields):
    code_i = _idx(fields, "證券", "代號", default=0)
    name_i = _idx(fields, "證券", "名稱", default=1)
    volume_i = _idx(fields, "成交", "股數", default=2)
    turnover_i = _idx(fields, "成交", "金額", default=3)
    open_i = _idx(fields, "開盤", default=4)
    high_i = _idx(fields, "最高", default=5)
    low_i = _idx(fields, "最低", default=6)
    close_i = _idx(fields, "收盤", default=7)
    change_i = _idx(fields, "漲跌", "價差", default=8)
    trades_i = _idx(fields, "成交", "筆數", default=9)
    return str(_row_value(row, code_i)).strip(), {
        "market": "TWSE",
        "name": _row_value(row, name_i),
        "volume": safe_float(_row_value(row, volume_i)),
        "turnover": safe_float(_row_value(row, turnover_i)),
        "open": safe_float(_row_value(row, open_i)),
        "high": safe_float(_row_value(row, high_i)),
        "low": safe_float(_row_value(row, low_i)),
        "close": safe_float(_row_value(row, close_i)),
        "change": safe_float(_row_value(row, change_i)),
        "trades": safe_float(_row_value(row, trades_i)),
        "source": "TWSE STOCK_DAY_ALL",
    }


def _parse_tpex_row(row, fields):
    code_i = _idx(fields, "代號", default=0)
    name_i = _idx(fields, "名稱", default=1)
    close_i = _idx(fields, "收盤", default=2)
    change_i = _idx(fields, "漲跌", default=3)
    open_i = _idx(fields, "開盤", default=4)
    high_i = _idx(fields, "最高", default=5)
    low_i = _idx(fields, "最低", default=6)
    volume_i = _idx(fields, "成交", "股數", default=8)
    return str(_row_value(row, code_i)).strip(), {
        "market": "TPEx",
        "name": _row_value(row, name_i),
        "close": safe_float(_row_value(row, close_i)),
        "change": safe_float(_row_value(row, change_i)),
        "open": safe_float(_row_value(row, open_i)),
        "high": safe_float(_row_value(row, high_i)),
        "low": safe_float(_row_value(row, low_i)),
        "volume": safe_float(_row_value(row, volume_i)),
        "source": "TPEx dailyCloseQuotes",
    }


def _write_twse_day(date_text: str, result: dict):
    """Fetch and write TWSE all-market data for a single trading date."""
    payload, err = fetch_json(TWSE_ALL, params={"response": "json", "date": date_text})
    if err:
        result["errors"].append(f"TWSE {date_text}: {err}")
    rows = _rows(payload)
    fields = _fields(payload)
    written = 0
    for row in rows:
        try:
            stock_id, doc = _parse_twse_all_row(row, fields)
            doc["data_date"] = date_text
            if stock_id and save_stock_daily(stock_id, date_text, doc):
                written += 1
        except Exception as exc:
            result["errors"].append(f"TWSE row {date_text}: {exc}")
    return written, bool(rows)


def _write_tpex_day(date_text: str, result: dict):
    """Fetch and write TPEx all-market data for a single trading date."""
    rows, fields, found = [], [], False
    for params in [
        {"response": "json", "date": roc_date_slash(date_text)},
        {"response": "json", "date": date_text},
        {"response": "json"},
    ]:
        payload, err = fetch_json(TPEX_ALL, params=params)
        if err:
            result["errors"].append(f"TPEx {date_text}: {err}")
        rows = _rows(payload)
        fields = _fields(payload)
        if rows:
            found = True
            break
    written = 0
    for row in rows:
        try:
            stock_id, doc = _parse_tpex_row(row, fields)
            if not stock_id or not stock_id[:1].isdigit():
                continue
            doc["data_date"] = date_text
            if save_stock_daily(stock_id, date_text, doc):
                written += 1
        except Exception as exc:
            result["errors"].append(f"TPEx row {date_text}: {exc}")
    return written, found


def run_daily_update(lookback_days: int = 5):
    """Update stock data for the most recent trading days.

    lookback_days: also fill any recent trading days that may have been missed.
    """
    requested_date = today_str()
    result = {
        "requested_date": requested_date,
        "twse_date": None, "tpex_date": None,
        "t86_date": None, "margin_date": None,
        "tpex_t86_date": None, "tpex_margin_date": None,
        "stocks": 0, "chips": 0, "margin_rows": 0,
        "tpex_chips": 0, "tpex_margin_rows": 0,
        "errors": [], "dates_written": [],
    }

    # Write data for up to lookback_days recent trading days
    twse_latest_date = None
    tpex_latest_date = None
    for d in recent_dates(lookback_days + 3):
        twse_written, twse_found = _write_twse_day(d, result)
        if twse_found:
            result["stocks"] += twse_written
            result["dates_written"].append(d)
            if twse_latest_date is None:
                twse_latest_date = d
        tpex_written, tpex_found = _write_tpex_day(d, result)
        if tpex_found:
            result["stocks"] += tpex_written
            if tpex_latest_date is None:
                tpex_latest_date = d
        # Stop once we've covered enough trading days with actual data
        dates_with_data = len(result["dates_written"])
        if twse_found and dates_with_data >= lookback_days:
            break
        time.sleep(0.1)

    twse_date = twse_latest_date or today_str()
    tpex_date = tpex_latest_date or today_str()
    result["twse_date"] = twse_date
    result["tpex_date"] = tpex_date

    write_t86_chips(twse_date, result)
    write_margin_chips(twse_date, result)
    write_tpex_insti_chips(tpex_date, result)
    write_tpex_margin_chips(tpex_date, result)

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
