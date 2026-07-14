from datetime import datetime, timedelta
import time
import requests
import urllib3

from firebase_cache import (
    save_stock_daily, save_chip_daily, save_job_log,
    save_stock_daily_bulk, save_chip_daily_bulk, save_fundamentals_bulk,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REQUEST_TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://www.twse.com.tw/zh/trading/foreign/t86.html",
    "Origin": "https://www.twse.com.tw",
}
TPEX_HEADERS = {
    **HEADERS,
    "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/detail/day.html",
    "Origin": "https://www.tpex.org.tw",
}

TWSE_ALL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL"
TWSE_T86 = "https://www.twse.com.tw/rwd/zh/fund/T86"
TWSE_MARGIN = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TWSE_STOCK_DAY = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
TWSE_STOCK_DAY_LEGACY = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TPEX_ALL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
TPEX_ALL_LEGACY = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes"  # dead since ~2026-07, kept as last-resort fallback
TPEX_STOCK_DAY = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
TPEX_INSTITUTIONAL = "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"
TPEX_MARGIN = "https://www.tpex.org.tw/www/zh-tw/margin/balance"
TWSE_BWIBBU  = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"  # rwd path works from Fly.io
TPEX_PE_BOOK = "https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate"
MOPS_REVENUE = "https://mops.twse.com.tw/mops/web/ajax_t05st10"

HOT_STOCKS = ["2330", "2317", "3702", "2454", "2382"]


def _csv_to_payload(text: str) -> dict:
    """Convert a TWSE/TPEx CSV response into the legacy JSON payload shape
    {"fields": [...], "data": [[...], ...]} so downstream parsers keep working.

    TWSE 於 2026-07-10 起 rwd 端點改回 CSV（response=json 失效）。
    CSV 可能有標題列/彙總段，取第一個含「代號」的列當表頭。
    """
    import csv
    import io
    text = (text or "").lstrip("﻿")
    if not text.strip() or text.lstrip().startswith(("<", "{", "[")):
        return {}
    try:
        all_rows = [r for r in csv.reader(io.StringIO(text)) if r and any(str(c).strip() for c in r)]
    except Exception:
        return {}
    header_idx = None
    for i, row in enumerate(all_rows):
        if any("代號" in str(c) for c in row):
            header_idx = i
            break
    if header_idx is None:
        return {}
    fields = [str(c).strip() for c in all_rows[header_idx]]
    ncol = len(fields)
    data = [r for r in all_rows[header_idx + 1:]
            if len(r) >= ncol - 1 and len([c for c in r if str(c).strip()]) > 1]
    return {"fields": fields, "data": data, "stat": "OK", "_source_format": "csv"}


def _parse_response(res):
    """Parse a TWSE/TPEx response: JSON first, CSV fallback."""
    try:
        return res.json()
    except ValueError:
        payload = _csv_to_payload(res.text)
        if payload:
            return payload
        raise


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
            return _parse_response(res), None
        except requests.exceptions.SSLError as ssl_exc:
            try:
                res = requests.get(url, params=params, headers=h, timeout=REQUEST_TIMEOUT, verify=False)
                if res.status_code in (520, 521, 522, 523, 524) and attempt < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                res.raise_for_status()
                return _parse_response(res), f"SSL fallback: {ssl_exc}"
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
    payload, err = fetch_json(TPEX_ALL, headers=TPEX_HEADERS)
    if err:
        errors.append(f"TPEx openapi: {err}")
    date_val, rows, fields = _tpex_openapi_to_rows(payload)
    if rows:
        return date_val or today_str(), rows, fields, errors
    for d in recent_dates(max_lookback_days):
        for params in [
            {"response": "json", "l": "zh-tw", "date": roc_date_slash(d)},
            {"response": "json", "date": roc_date_slash(d)},
            {"response": "json", "l": "zh-tw", "date": d},
            {"response": "json", "date": d},
            {"response": "json"},
        ]:
            legacy_payload, err2 = fetch_json(TPEX_ALL_LEGACY, params=params, headers=TPEX_HEADERS)
            if err2:
                errors.append(f"TPEx {d}: {err2}")
            rows = _rows(legacy_payload)
            fields = _fields(legacy_payload)
            if rows:
                return d, rows, fields, errors
    return today_str(), [], [], errors


def _fetch_yfinance_twse_month(stock_id: str, year: int, month: int, product_type: str = "股票"):
    """Fallback: fetch TWSE monthly K-line via yfinance (.TW suffix)."""
    import calendar as _cal
    import datetime as _dt
    try:
        import yfinance as yf
    except ImportError:
        return 0, ["yfinance not installed"]
    try:
        start = _dt.date(year, month, 1).isoformat()
        last_day = _cal.monthrange(year, month)[1]
        end = _dt.date(year, month, last_day).isoformat()
        ticker = f"{stock_id}.TW"
        hist = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if hist is None or hist.empty:
            return 0, []
        if hasattr(hist.columns, "levels"):
            hist.columns = hist.columns.get_level_values(0)
        written, errors = 0, []
        for idx, row in hist.iterrows():
            try:
                date_str = idx.strftime("%Y%m%d")
                def _v(col, _row=row):
                    val = _row.get(col)
                    if val is None:
                        return None
                    if hasattr(val, "iloc"):
                        val = val.iloc[0]
                    elif hasattr(val, "item"):
                        val = val.item()
                    try:
                        f = float(val)
                        return f if f == f else None
                    except Exception:
                        return None
                doc = {
                    "market": "TWSE", "product_type": product_type,
                    "open": _v("Open"), "high": _v("High"),
                    "low": _v("Low"), "close": _v("Close"),
                    "volume": _v("Volume"), "source": "yfinance_twse",
                }
                if save_stock_daily(stock_id, date_str, doc):
                    written += 1
            except Exception as exc:
                errors.append(f"yf row {stock_id}: {exc}")
        return written, errors
    except Exception as exc:
        return 0, [f"yfinance error {stock_id}: {exc}"]


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
        # TWSE API blocked (e.g. from Fly.io) — fall back to yfinance
        w2, e2 = _fetch_yfinance_twse_month(stock_id, year, month, product_type)
        return w2, errors + e2
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


def _fetch_yfinance_tpex_month(stock_id: str, year: int, month: int, product_type: str = "股票"):
    """Fallback: fetch TPEx monthly K-line via yfinance (.TWO suffix)."""
    import calendar as _cal
    import datetime as _dt
    try:
        import yfinance as yf
    except ImportError:
        return 0, ["yfinance not installed"]
    try:
        start = _dt.date(year, month, 1).isoformat()
        last_day = _cal.monthrange(year, month)[1]
        end = _dt.date(year, month, last_day).isoformat()
        ticker = f"{stock_id}.TWO"
        hist = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if hist is None or hist.empty:
            return 0, [f"yfinance no data: {ticker} {year}-{month:02d}"]
        # yfinance 2.x returns MultiIndex columns for single ticker — flatten
        if hasattr(hist.columns, "levels"):
            hist.columns = hist.columns.get_level_values(0)
        written, errors = 0, []
        for idx, row in hist.iterrows():
            try:
                date_str = idx.strftime("%Y%m%d")
                def _v(col, _row=row):
                    val = _row.get(col)
                    if val is None:
                        return None
                    # unwrap Series/array to scalar
                    if hasattr(val, "iloc"):
                        val = val.iloc[0]
                    elif hasattr(val, "item"):
                        val = val.item()
                    try:
                        f = float(val)
                        return f if f == f else None  # NaN check
                    except Exception:
                        return None
                doc = {
                    "market": "TPEx", "product_type": product_type,
                    "open": _v("Open"), "high": _v("High"),
                    "low": _v("Low"), "close": _v("Close"),
                    "volume": _v("Volume"), "source": "yfinance_tpex",
                }
                if save_stock_daily(stock_id, date_str, doc):
                    written += 1
            except Exception as exc:
                errors.append(f"yf row {stock_id}: {exc}")
        return written, errors
    except Exception as exc:
        return 0, [f"yfinance error {stock_id}: {exc}"]


def fetch_tpex_stock_month(stock_id: str, year: int, month: int, product_type: str = "股票"):
    errors = []
    written = 0
    roc_ym = f"{year - 1911}/{month:02d}"
    # probe run 29302130207 確認：正確參數是 code=<代號>&date=<西元年>/<月>/<日>
    # （日期需帶完整 D，且年份是西元不是民國；stockNo/民國年/純年月都回「參數輸入錯誤」）
    params_candidates = [
        {"response": "json", "code": stock_id, "date": f"{year}/{month:02d}/01"},
        {"response": "json", "code": stock_id, "date": f"{year}/{month:02d}/15"},
        {"response": "json", "l": "zh-tw", "d": roc_ym, "s": f"{stock_id},asc,0"},
        {"response": "json", "date": roc_ym, "stockNo": stock_id},
    ]
    payload = {}
    err = None
    rows, fields = [], []
    for params in params_candidates:
        payload, err = fetch_json(TPEX_STOCK_DAY, params=params, headers=TPEX_HEADERS)
        # response 把資料包在 tables[0] 裡，_fields()/_rows() 不會往裡挖 —
        # 用 _table_rows() 才拿得到正確欄位名，否則 _idx() 全部落回硬編預設值，
        # 讀出來的欄位會整組錯位（例如把成交筆數當成成交股數）
        rows, fields = _table_rows(payload)
        if rows:
            break
    if err:
        errors.append(f"TPEx month error {stock_id}: {err}")
    if not rows:
        # TPEx API unavailable — fall back to yfinance
        w2, e2 = _fetch_yfinance_tpex_month(stock_id, year, month, product_type)
        return w2, errors + e2

    date_i = _idx(fields, "日期", default=0)
    close_i = _idx(fields, "收盤", default=2)
    change_i = _idx(fields, "漲跌", default=3)
    open_i = _idx(fields, "開盤", default=4)
    high_i = _idx(fields, "最高", default=5)
    low_i = _idx(fields, "最低", default=6)
    volume_i = _idx(fields, "成交", default=8)
    # 確定端點回的欄位是「成交仟元」不是「金額」（probe run 29302130207）
    turnover_i = _idx(fields, "金額", default=None) or _idx(fields, "仟元", default=None)
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
        # T86 fields: "外陸資買賣超股數(不含外資自營商)" at 4, "投信買賣超股數" at 10,
        # "自營商買賣超股數" at 11, "三大法人買賣超股數" at 18.
        # Use "外陸資" (not "外資") to avoid matching the parenthetical "(不含外資自營商)"
        # which also contains "自營商", causing both foreign_i and dealer_i to land on index 4.
        foreign_i = _idx(fields, "外陸資", "買賣超", default=4)
        trust_i = _idx(fields, "投信", "買賣超", default=10)
        # "自營商買賣超股數" is a longer unique key that won't match index 4
        dealer_i = _idx(fields, "自營商買賣超股數", default=11)
        total_i = _idx(fields, "三大法人", "買賣超", default=18)
        bulk_rows = []
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
                    "institution_total_buy": safe_int(_row_value(row, total_i)),
                    "source_t86": "TWSE T86",
                    "source": "TWSE T86",
                    "chip_date": d,
                }
                bulk_rows.append((stock_id, d, payload_doc))
            except Exception as exc:
                result["errors"].append(f"T86 row {d}: {exc}")
        written = save_chip_daily_bulk(bulk_rows)
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
        bulk_rows = []
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
                bulk_rows.append((stock_id, d, payload_doc))
            except Exception as exc:
                result["errors"].append(f"margin row {d}: {exc}")
        written = save_chip_daily_bulk(bulk_rows)
        result["margin_rows"] = written
        result["margin_date"] = d
        return written
    return 0


def _parse_bwibbu_row(row):
    """Parse valuation row from TWSE BWIBBU_d or TPEx peQryDate.
    Both APIs return arrays: [code, name, close, yield%, year, pe, pb, quarter]
    Returns (code, payload_dict) or (None, {}) on invalid row.
    """
    if not row or len(row) < 7:
        return None, {}
    code = str(row[0]).strip()
    if not code or not code[0].isdigit():
        return None, {}
    def _fv(v):
        try:
            s = str(v).replace(",", "").strip()
            return float(s) if s not in ("", "-", "--", "N/A") else None
        except Exception:
            return None
    close = _fv(row[2])
    dy    = _fv(row[3])
    pe    = _fv(row[5])
    pb    = _fv(row[6])
    eps   = round(close / pe, 2) if close and pe and pe > 0 else None
    return code, {
        "pe_ratio": pe, "dividend_yield": dy, "pb_ratio": pb,
        "eps": eps,
    }


def write_twse_valuation(result: dict):
    """Fetch BWIBBU_d (TWSE) + peQryDate (TPEx) for PE/PB/殖利率/EPS.
    Primary: TWSE rwd API.  Fallback: yfinance (works from any IP)."""
    today = today_str()
    twse_written = 0

    # ── Try TWSE BWIBBU_d ──────────────────────────────────────────────────────
    twse_ok = False
    try:
        r = requests.get(
            TWSE_BWIBBU, params={"response": "json"}, headers=HEADERS, timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        rows = data.get("data", [])
        if rows:
            bulk_rows = []
            for row in rows:
                code, vals = _parse_bwibbu_row(row)
                if code:
                    bulk_rows.append((code, {**vals, "valuation_date": today, "source": "twse_bwibbu"}))
            twse_written = save_fundamentals_bulk(bulk_rows)
            twse_ok = True
    except Exception as exc:
        result.setdefault("errors", []).append(f"BWIBBU_d: {exc}")

    result["twse_valuation_written"] = twse_written
    result["twse_source"] = "twse_bwibbu" if twse_ok else "skipped"
    return twse_written


def write_tpex_valuation(result: dict):
    """Fetch TPEx peQryDate for PE/PB/殖利率/EPS (works from any IP)."""
    today = today_str()
    written = 0
    for d in list(recent_trading_dates(5)):
        roc_date = f"{int(d[:4]) - 1911}/{d[4:6]}/{d[6:8]}"
        try:
            r = requests.get(
                TPEX_PE_BOOK,
                params={"response": "json", "date": roc_date},
                headers=TPEX_HEADERS,
                timeout=20,
            )
            r.raise_for_status()
            data = r.json()
            tables = data.get("tables", [])
            rows = tables[0].get("data", []) if tables else []
            if not rows:
                continue
            bulk_rows = []
            for row in rows:
                code, vals = _parse_bwibbu_row(row)
                if not code:
                    continue
                bulk_rows.append((code, {**vals, "valuation_date": today, "source": "tpex_pebook"}))
            written = save_fundamentals_bulk(bulk_rows)
            if bulk_rows and not written:
                result.setdefault("errors", []).append("TPEx save failed (fundamentals table/eps column missing?)")
            result["tpex_valuation_written"] = written
            return written
        except Exception as exc:
            result.setdefault("errors", []).append(f"TPEx PE {roc_date}: {exc}")
    result["tpex_valuation_written"] = written
    return written


def write_yfinance_fundamentals(codes: list, market: str, result: dict,
                                 sleep_sec: float = 0.3):
    """Batch-fetch PE/PB/EPS/殖利率 via yfinance for a list of codes.
    Suffix: TWSE → .TW, TPEx → .TWO"""
    try:
        import yfinance as yf
    except ImportError:
        result.setdefault("errors", []).append("yfinance not installed")
        return 0

    from firebase_cache import save_fundamentals
    today = today_str()
    suffix = ".TWO" if market.upper() in ("TPEX", "上櫃") else ".TW"
    written, skipped = 0, 0

    for code in codes:
        try:
            ticker = yf.Ticker(f"{code}{suffix}")
            try:
                raw = ticker.info
                info = raw if isinstance(raw, dict) else {}
            except Exception:
                skipped += 1
                time.sleep(sleep_sec)
                continue
            pe  = info.get("trailingPE") or info.get("forwardPE")
            pb  = info.get("priceToBook")
            eps = info.get("trailingEps")
            dy  = info.get("dividendYield")
            if dy is not None:
                if 0 < dy <= 1.0:
                    dy = round(dy * 100, 2)  # decimal form: 0.035 → 3.5%
                else:
                    dy = None  # >1 means yfinance returned wrong field for this ticker
            if pe is None and pb is None and eps is None:
                skipped += 1
                continue
            ok = save_fundamentals(code, {
                "pe_ratio": pe, "pb_ratio": pb, "eps": eps, "dividend_yield": dy,
                "valuation_date": today, "source": f"yfinance{suffix}",
            })
            if ok:
                written += 1
            else:
                result.setdefault("errors", []).append(f"DB save failed: {code} (table missing or eps column not added)")
        except Exception as exc:
            result.setdefault("errors", []).append(f"yf {code}: {exc}")
        time.sleep(sleep_sec)

    result[f"yfinance_{market}_written"] = written
    result[f"yfinance_{market}_skipped"] = skipped
    return written


def write_mops_revenue_all(result: dict, months_back: int = 0):
    """Fetch MOPS monthly revenue for all stocks (上市+上櫃) and write to fundamentals.
    Uses a requests.Session to carry cookies (bypasses MOPS WAF)."""
    from firebase_cache import save_fundamentals
    now = datetime.now()
    m = now.month - months_back
    y = now.year
    while m <= 0:
        m += 12
        y -= 1
    roc_year = y - 1911
    revenue_date = f"{y}-{m:02d}"

    # Warm up session with cookies so MOPS WAF lets the POST through
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://mops.twse.com.tw/mops/web/t05st10", timeout=15)
    except Exception:
        pass

    mops_headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://mops.twse.com.tw/mops/web/t05st10",
        "Origin": "https://mops.twse.com.tw",
    }

    total_written = 0
    result.setdefault("errors", [])
    for typek, label in [("sii", "上市"), ("otc", "上櫃")]:
        try:
            r = session.post(
                MOPS_REVENUE,
                data={"firstin": "1", "off": "1", "TYPEK": typek,
                      "year": str(roc_year), "mon": f"{m:02d}"},
                headers=mops_headers,
                timeout=30,
            )
            r.raise_for_status()
            if r.text.strip().startswith("<"):
                result["errors"].append(f"MOPS {label}: 安全機制封鎖（非 JSON 回應）")
                continue
            data = r.json()
            written = 0
            for row in data.get("aaData", []):
                if not row or len(row) < 6:
                    continue
                code = str(row[0]).strip()
                if not code or not code[0].isdigit():
                    continue
                def _num(s):
                    try:
                        return float(str(s).replace(",", ""))
                    except Exception:
                        return None
                rev      = _num(row[2])
                rev_last = _num(row[3])
                rev_yoy  = _num(row[5])
                if rev is None:
                    continue
                mom = round((rev / rev_last - 1) * 100, 2) if rev_last else None
                yoy = round((rev / rev_yoy  - 1) * 100, 2) if rev_yoy  else None
                if save_fundamentals(code, {
                    "revenue": int(rev), "revenue_mom": mom, "revenue_yoy": yoy,
                    "revenue_date": revenue_date, "source": f"mops_{typek}",
                }):
                    written += 1
            result[f"{label}_revenue_written"] = written
            total_written += written
            time.sleep(0.5)
        except Exception as exc:
            result["errors"].append(f"MOPS {label}: {exc}")

    result["revenue_date"] = revenue_date
    result["total_revenue_written"] = total_written
    return total_written


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
    bulk_rows = []
    for row in rows:
        try:
            stock_id, payload_doc = _parse_tpex_insti_row(row)
            if stock_id:
                bulk_rows.append((stock_id, date_text, {**payload_doc, "chip_date": date_text}))
        except Exception as exc:
            result["errors"].append(f"TPEx insti row {date_text}: {exc}")
    written = save_chip_daily_bulk(bulk_rows)
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
    bulk_rows = []
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
            bulk_rows.append((stock_id, date_text, payload_doc))
        except Exception as exc:
            result["errors"].append(f"TPEx margin row {date_text}: {exc}")
    written = save_chip_daily_bulk(bulk_rows)
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


def _roc_any_to_yyyymmdd(text: str) -> str | None:
    text = str(text or "").strip()
    if "/" in text:
        return roc_to_yyyymmdd(text)
    if text.isdigit() and len(text) == 7:  # e.g. 1150713
        return f"{int(text[:3]) + 1911}{text[3:]}"
    if text.isdigit() and len(text) == 8:
        return text
    return None


def _write_twse_day(date_text: str, result: dict):
    """Fetch and write TWSE all-market data for a single trading date."""
    payload, err = fetch_json(TWSE_ALL, params={"response": "json", "date": date_text})
    if err:
        result["errors"].append(f"TWSE {date_text}: {err}")
    rows = _rows(payload)
    fields = _fields(payload)
    # CSV 版多了「日期」欄且伺服器可能忽略 date 參數只回最新交易日 —
    # 以資料列自己的日期為準，與請求日不符就跳過，避免誤標寫入。
    date_i = _idx(fields, "日期")
    if rows and date_i is not None:
        row_date = _roc_any_to_yyyymmdd(_row_value(rows[0], date_i))
        if row_date and row_date != date_text:
            result["errors"].append(
                f"TWSE {date_text}: server returned data for {row_date}, skipped to avoid mislabeling")
            return 0, False
    bulk_rows = []
    for row in rows:
        try:
            stock_id, doc = _parse_twse_all_row(row, fields)
            doc["data_date"] = date_text
            if stock_id:
                bulk_rows.append((stock_id, date_text, doc))
        except Exception as exc:
            result["errors"].append(f"TWSE row {date_text}: {exc}")
    written = save_stock_daily_bulk(bulk_rows)
    return written, bool(rows)


def _tpex_payload_date(payload) -> str | None:
    """Extract the trading date TPEx reports inside its own response (ROC formats)."""
    if not isinstance(payload, dict):
        return None
    candidates = [payload.get("date"), payload.get("reportDate")]
    tables = payload.get("tables")
    if isinstance(tables, list) and tables:
        candidates.extend([(tables[0] or {}).get("date"), (tables[0] or {}).get("reportDate")])
    for value in candidates:
        text = str(value or "").strip()
        if not text:
            continue
        if "/" in text:
            return roc_to_yyyymmdd(text)
        if text.isdigit() and len(text) == 7:  # e.g. 1150710
            return f"{int(text[:3]) + 1911}{text[3:]}"
        if text.isdigit() and len(text) == 8:
            return text
    return None


def _tpex_openapi_to_rows(payload):
    """Convert TPEx OpenAPI mainboard_daily_close_quotes response (JSON array
    of dicts, English field names) into the legacy (date, rows, fields) shape
    so `_parse_tpex_row` can stay unchanged. No date param on this endpoint —
    it always returns the latest trading day, same limitation as TWSE
    STOCK_DAY_ALL (probe run 29302130207)."""
    if not isinstance(payload, list) or not payload:
        return None, [], []
    fields = ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數", "成交金額", "成交筆數"]
    rows = []
    date_val = None
    for item in payload:
        if not isinstance(item, dict):
            continue
        code = str(item.get("SecuritiesCompanyCode") or "").strip()
        if not code:
            continue
        if date_val is None:
            date_val = _roc_any_to_yyyymmdd(str(item.get("Date") or ""))
        rows.append([
            code, item.get("CompanyName"), item.get("Close"), item.get("Change"),
            item.get("Open"), item.get("High"), item.get("Low"),
            item.get("TradingShares"), item.get("TransactionAmount"), item.get("TransactionNumber"),
        ])
    return date_val, rows, fields


def _write_tpex_day(date_text: str, result: dict):
    """Fetch and write TPEx all-market data for a single trading date.

    Primary: TPEx OpenAPI mainboard_daily_close_quotes. Falls back to the
    legacy www afterTrading/dailyCloseQuotes endpoint (dead as of 2026-07,
    kept in case OpenAPI disappears too — see T1 in
    .omc/plans/2026-07-14-pipeline-fix-spec.md).
    """
    payload, err = fetch_json(TPEX_ALL, headers=TPEX_HEADERS)
    if err:
        result["errors"].append(f"TPEx openapi {date_text}: {err}")
    actual_date, rows, fields = _tpex_openapi_to_rows(payload)

    if not rows:
        for params in [
            {"response": "json", "date": roc_date_slash(date_text)},
            {"response": "json", "date": date_text},
            {"response": "json"},
        ]:
            # TPEx rejects requests without its own Referer/Origin — must use TPEX_HEADERS
            legacy_payload, err2 = fetch_json(TPEX_ALL_LEGACY, params=params, headers=TPEX_HEADERS)
            if err2:
                result["errors"].append(f"TPEx legacy {date_text}: {err2}")
            legacy_rows = _rows(legacy_payload)
            legacy_fields = _fields(legacy_payload)
            if not legacy_fields:
                _, legacy_fields = _table_rows(legacy_payload)
            if legacy_rows:
                rows, fields = legacy_rows, legacy_fields
                actual_date = _tpex_payload_date(legacy_payload) or date_text
                break
    if not rows:
        return 0, False

    # Trust the date TPEx reports itself. When asked for a date with no data the
    # API silently falls back to the latest trading day — writing those rows
    # under the requested date corrupts the DB with mislabeled duplicates.
    actual_date = actual_date or date_text
    if actual_date != date_text:
        result["errors"].append(
            f"TPEx {date_text}: server returned data for {actual_date}, skipped to avoid mislabeling")
        return 0, False
    bulk_rows = []
    for row in rows:
        try:
            stock_id, doc = _parse_tpex_row(row, fields)
            if not stock_id or not stock_id[:1].isdigit():
                continue
            doc["data_date"] = actual_date
            bulk_rows.append((stock_id, actual_date, doc))
        except Exception as exc:
            result["errors"].append(f"TPEx row {date_text}: {exc}")
    written = save_stock_daily_bulk(bulk_rows)
    return written, True


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
