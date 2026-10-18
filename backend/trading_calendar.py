"""trading_calendar.py — 台股交易日曆（平日 − TWSE 休市日 + 已知補班交易日）。

休市日來源：TWSE holidaySchedule API。該 API 忽略 queryYear，永遠只回「當年度」，
所以每次抓到的年份都併入本機快取（backend/.cache/holidays.json），歷年資料會逐年累積。
「開始交易日 / 最後交易日」標記列其實是交易日，不算休市。

calendar_source():
  "api"      本次執行成功從 TWSE 取得
  "cache"    API 連不上，使用本機快取
  "fallback" 完全沒有休市資料，只能用「平日」判斷 → 呼叫端做破壞性動作前必須檢查

covers(d): 該日期所屬年份是否有休市資料。未涵蓋的年份只能確定週末非交易日，
平日一律視為交易日（未知），刪資料時請先確認 covers(d)。
"""
import json
import os
import re
import time
from datetime import datetime, timedelta

import requests

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(BACKEND_DIR, ".cache", "holidays.json")
HOLIDAY_URL = "https://www.twse.com.tw/holidaySchedule/holidaySchedule"
CACHE_TTL_SEC = 24 * 3600

# 週六補行交易日（證交所公告仍開市的週六）
MAKEUP_SATURDAYS = {"20170930"}

_MARKER_RE = re.compile(r"(開始|最後)交易日")
_state = {"loaded": False, "holidays": {}, "years": set(), "source": "fallback"}


def _parse_api(payload):
    """回傳 ({yyyymmdd: name} 休市日, {年份} 涵蓋年份)；標記列不算休市。"""
    holidays, years = {}, set()
    for row in (payload or {}).get("data") or []:
        try:
            d = str(row[0]).replace("-", "").replace("/", "")
            if len(d) != 8 or not d.isdigit():
                continue
            years.add(int(d[:4]))
            if _MARKER_RE.search(str(row[1])):
                continue
            holidays[d] = str(row[1])
        except Exception:
            continue
    return holidays, years


def _fetch_api():
    # 先試民國年再試西元年（實測 API 兩者都會忽略而回當年度）；每個 param 若涵蓋不同年份就合併
    holidays, years = {}, set()
    now_year = datetime.now().year
    for qy in (str(now_year - 1911), str(now_year)):
        try:
            r = requests.get(HOLIDAY_URL, params={"response": "json", "queryYear": qy},
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            r.raise_for_status()
            h, y = _parse_api(json.loads(r.content.decode("utf-8")))
            holidays.update(h)
            years |= y
        except Exception:
            continue
    return holidays, years


def _read_cache():
    try:
        with open(CACHE_PATH, encoding="utf-8") as f:
            c = json.load(f)
        return c.get("holidays", {}), {int(y) for y in c.get("years", [])}, float(c.get("fetched_at", 0))
    except Exception:
        return {}, set(), 0.0


def _write_cache(holidays, years):
    try:
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump({"fetched_at": time.time(), "years": sorted(years), "holidays": holidays},
                      f, ensure_ascii=False, indent=1, sort_keys=True)
    except Exception:
        pass


def load(force: bool = False):
    if _state["loaded"] and not force:
        return
    c_hol, c_years, fetched_at = _read_cache()
    if c_years and not force and time.time() - fetched_at < CACHE_TTL_SEC:
        _state.update(loaded=True, holidays=c_hol, years=c_years, source="cache")
        return
    a_hol, a_years = _fetch_api()
    if a_years:
        # 併入快取：API 只回當年度，舊年度靠快取保留（同年度以 API 為準）
        keep = {d: n for d, n in c_hol.items() if int(d[:4]) not in a_years}
        holidays = {**keep, **a_hol}
        years = c_years | a_years
        _write_cache(holidays, years)
        _state.update(loaded=True, holidays=holidays, years=years, source="api")
    elif c_years:
        _state.update(loaded=True, holidays=c_hol, years=c_years, source="cache")
    else:
        _state.update(loaded=True, holidays={}, years=set(), source="fallback")


def calendar_source() -> str:
    load()
    return _state["source"]


def covers(yyyymmdd: str) -> bool:
    load()
    return int(yyyymmdd[:4]) in _state["years"]


def holiday_name(yyyymmdd: str):
    load()
    return _state["holidays"].get(yyyymmdd)


def is_trading_day(yyyymmdd: str) -> bool:
    if yyyymmdd in MAKEUP_SATURDAYS:
        return True
    if datetime.strptime(yyyymmdd, "%Y%m%d").weekday() >= 5:
        return False
    load()
    return yyyymmdd not in _state["holidays"]


def trading_days(start: str, end: str):
    """start~end（含）之間的交易日，舊 → 新。"""
    d = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    out = []
    while d <= e:
        s = d.strftime("%Y%m%d")
        if is_trading_day(s):
            out.append(s)
        d += timedelta(days=1)
    return out
