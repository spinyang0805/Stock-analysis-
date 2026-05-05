from __future__ import annotations

from functools import lru_cache
from typing import Dict, List
import requests

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
TIMEOUT = 4

TWSE_LISTED = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_LISTED = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TWSE_ETF = "https://openapi.twse.com.tw/v1/opendata/t187ap03_ETF"

SEED_PRODUCTS = [
    {"code": "0050", "name": "元大台灣50", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "0056", "name": "元大高股息", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "006208", "name": "富邦台50", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00878", "name": "國泰永續高股息", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00919", "name": "群益台灣精選高息", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00679B", "name": "元大美債20年", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00687B", "name": "國泰20年美債", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00720B", "name": "元大投資級公司債", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00725B", "name": "國泰投資級公司債", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00857B", "name": "永豐20年美公債", "market": "上市", "type": "債券ETF", "industry": "債券"},
]

FALLBACK_STOCKS = [
    {"code": "2330", "name": "台積電", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "2317", "name": "鴻海", "market": "上市", "type": "股票", "industry": "其他電子"},
    {"code": "2454", "name": "聯發科", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "2308", "name": "台達電", "market": "上市", "type": "股票", "industry": "電子零組件"},
    {"code": "2382", "name": "廣達", "market": "上市", "type": "股票", "industry": "電腦及週邊"},
    {"code": "3231", "name": "緯創", "market": "上市", "type": "股票", "industry": "電腦及週邊"},
    {"code": "2324", "name": "仁寶", "market": "上市", "type": "股票", "industry": "電腦及週邊"},
    {"code": "2357", "name": "華碩", "market": "上市", "type": "股票", "industry": "電腦及週邊"},
    {"code": "3702", "name": "大聯大", "market": "上市", "type": "股票", "industry": "電子通路"},
    {"code": "2303", "name": "聯電", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "3711", "name": "日月光投控", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "2881", "name": "富邦金", "market": "上市", "type": "股票", "industry": "金融"},
    {"code": "2882", "name": "國泰金", "market": "上市", "type": "股票", "industry": "金融"},
    {"code": "2891", "name": "中信金", "market": "上市", "type": "股票", "industry": "金融"},
    {"code": "2412", "name": "中華電", "market": "上市", "type": "股票", "industry": "通信網路"},
    {"code": "3008", "name": "大立光", "market": "上市", "type": "股票", "industry": "光電"},
    {"code": "3034", "name": "聯詠", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "3661", "name": "世芯-KY", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "6446", "name": "藥華藥", "market": "上櫃", "type": "股票", "industry": "生技醫療"},
    {"code": "5347", "name": "世界", "market": "上櫃", "type": "股票", "industry": "半導體"},
    {"code": "6488", "name": "環球晶", "market": "上櫃", "type": "股票", "industry": "半導體"},
    {"code": "8069", "name": "元太", "market": "上櫃", "type": "股票", "industry": "光電"},
    {"code": "4966", "name": "譜瑞-KY", "market": "上櫃", "type": "股票", "industry": "半導體"},
]


def _get_json(url: str):
    try:
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception:
        return []


def _pick(row: Dict, keys: List[str], default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return default


def _dedupe(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    result = []
    for item in items:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        if not code or not name:
            continue
        key = (code, item.get("type", ""))
        if key in seen:
            continue
        seen.add(key)
        result.append({
            "code": code,
            "name": name,
            "market": item.get("market") or "--",
            "type": item.get("type") or "股票",
            "industry": item.get("industry") or item.get("type") or "--",
        })
    return result


def _infer_type(code: str, name: str) -> str:
    upper = str(code).upper()
    text = f"{code} {name}"
    if upper.endswith("B") or "債" in text:
        return "債券ETF"
    if upper.startswith("00"):
        return "ETF"
    return "股票"


def _firebase_products(limit: int = 5000) -> List[Dict[str, str]]:
    try:
        from firebase import db
        if db is None:
            return []
        items = []
        for doc in db.collection("stock_daily").limit(limit).stream():
            data = doc.to_dict() or {}
            latest = data.get("latest") or {}
            code = data.get("stock_id") or doc.id
            name = latest.get("name") or code
            market_raw = latest.get("market") or data.get("market") or "--"
            market = "上櫃" if str(market_raw).upper() == "TPEX" else "上市" if str(market_raw).upper() == "TWSE" else market_raw
            product_type = latest.get("product_type") or _infer_type(code, name)
            items.append({"code": str(code), "name": str(name), "market": market, "type": product_type, "industry": product_type})
        return items
    except Exception:
        return []


def _listed_stocks() -> List[Dict[str, str]]:
    items = []
    for row in _get_json(TWSE_LISTED):
        code = _pick(row, ["公司代號", "Code", "SecuritiesCompanyCode"])
        name = _pick(row, ["公司名稱", "CompanyName", "名稱"])
        industry = _pick(row, ["產業別", "Industry", "產業類別"], "股票")
        if code:
            items.append({"code": code, "name": name, "market": "上市", "type": _infer_type(code, name), "industry": industry})
    return items


def _tpex_stocks() -> List[Dict[str, str]]:
    items = []
    for row in _get_json(TPEX_LISTED):
        code = _pick(row, ["SecuritiesCompanyCode", "公司代號", "Code"])
        name = _pick(row, ["CompanyName", "公司名稱", "名稱"])
        industry = _pick(row, ["Industry", "產業別", "產業類別"], "股票")
        if code:
            items.append({"code": code, "name": name, "market": "上櫃", "type": _infer_type(code, name), "industry": industry})
    return items


def _etfs() -> List[Dict[str, str]]:
    items = []
    for row in _get_json(TWSE_ETF):
        code = _pick(row, ["證券代號", "基金代號", "Code", "代號"])
        name = _pick(row, ["證券名稱", "基金名稱", "Name", "名稱"])
        if code:
            product_type = _infer_type(code, name)
            items.append({"code": code, "name": name, "market": "上市", "type": product_type, "industry": product_type})
    return items


@lru_cache(maxsize=1)
def get_all_products() -> List[Dict[str, str]]:
    # Fast path: existing Firebase documents are the most important list for reset/cleanup.
    firebase_items = _firebase_products()
    if firebase_items:
        return _dedupe(firebase_items + SEED_PRODUCTS + FALLBACK_STOCKS)

    # External product lists are best-effort only and must not block the UI for minutes.
    items = []
    items.extend(_listed_stocks())
    items.extend(_tpex_stocks())
    items.extend(_etfs())
    items.extend(SEED_PRODUCTS)
    items.extend(FALLBACK_STOCKS)
    return _dedupe(items)


def search_products(query: str, limit: int = 12) -> List[Dict[str, str]]:
    q = str(query or "").strip().lower()
    if not q:
        return []

    exact = []
    partial = []
    for item in get_all_products():
        code = item["code"].lower()
        name = item["name"].lower()
        market = item.get("market", "").lower()
        typ = item.get("type", "").lower()
        haystack = f"{code} {name} {market} {typ}"
        if q == code or q == name:
            exact.append(item)
        elif q in haystack:
            partial.append(item)
    return (exact + partial)[:limit]
