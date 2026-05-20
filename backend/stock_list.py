from __future__ import annotations

from functools import lru_cache
from typing import Dict, List
import re

import requests

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
TIMEOUT = 8

TWSE_LISTED = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_LISTED = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TWSE_ETF = "https://openapi.twse.com.tw/v1/opendata/t187ap03_ETF"

SEED_PRODUCTS = [
    {"code": "2330", "name": "台積電", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "2317", "name": "鴻海", "market": "上市", "type": "股票", "industry": "其他電子"},
    {"code": "2454", "name": "聯發科", "market": "上市", "type": "股票", "industry": "半導體"},
    {"code": "2308", "name": "台達電", "market": "上市", "type": "股票", "industry": "電子零組件"},
    {"code": "2382", "name": "廣達", "market": "上市", "type": "股票", "industry": "電腦周邊"},
    {"code": "1402", "name": "遠東新", "market": "上市", "type": "股票", "industry": "紡織"},
    {"code": "3702", "name": "大聯大", "market": "上市", "type": "股票", "industry": "電子通路"},
    {"code": "0050", "name": "元大台灣50", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "0056", "name": "元大高股息", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "006208", "name": "富邦台50", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00878", "name": "國泰永續高股息", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00919", "name": "群益台灣精選高息", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00981A", "name": "主動統一台股增長", "market": "上市", "type": "ETF", "industry": "ETF"},
    {"code": "00679B", "name": "元大美債20年", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00687B", "name": "國泰20年美債", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00720B", "name": "元大投資級公司債", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00725B", "name": "國泰投資級公司債", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00857B", "name": "永豐20年美公債", "market": "上市", "type": "債券ETF", "industry": "債券"},
]


def _get_json(url: str):
    try:
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.encoding = "utf-8-sig"
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


def _valid_code(code: str) -> bool:
    code = str(code or "").strip().upper()
    if re.fullmatch(r"[1-9][0-9]{3}", code):
        return True
    if re.fullmatch(r"00[5-9][0-9]{1,3}", code):
        return True
    if re.fullmatch(r"00[5-9][0-9]{2,3}[AB]", code):
        return True
    return False


def _norm_market(value: str) -> str:
    text = str(value or "").strip().upper()
    if text in ("TPEX", "TPEx".upper(), "上櫃", "OTC"):
        return "上櫃"
    if text in ("TWSE", "上市"):
        return "上市"
    return str(value or "上市").strip() or "上市"


def _infer_type(code: str, name: str) -> str:
    upper = str(code or "").upper()
    text = f"{code} {name}".upper()
    if upper.endswith("B") or "債" in text or "BOND" in text:
        return "債券ETF"
    if upper.startswith("00") or upper.endswith("A") or "ETF" in text:
        return "ETF"
    return "股票"


def _dedupe(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    result = []
    for item in items:
        code = str(item.get("code", "")).strip().upper()
        if not _valid_code(code) or code in seen:
            continue
        seen.add(code)
        name = str(item.get("name") or code).strip()
        product_type = item.get("type") or _infer_type(code, name)
        result.append({
            "code": code,
            "name": name,
            "market": _norm_market(item.get("market")),
            "type": product_type,
            "industry": item.get("industry") or product_type,
        })
    return result


def _firebase_products(limit: int = 5000) -> List[Dict[str, str]]:
    try:
        from firebase import db
        if db is None:
            return []
        items = []
        for collection in ["product_universe", "stock_daily"]:
            for doc in db.collection(collection).limit(limit).stream():
                data = doc.to_dict() or {}
                latest = data.get("latest") or {}
                code = str(data.get("code") or data.get("stock_id") or doc.id).strip().upper()
                name = data.get("name") or latest.get("name") or code
                market = _norm_market(data.get("market") or latest.get("market"))
                product_type = data.get("type") or latest.get("product_type") or _infer_type(code, name)
                items.append({"code": code, "name": name, "market": market, "type": product_type, "industry": data.get("industry") or product_type})
        return items
    except Exception:
        return []


def _listed_stocks() -> List[Dict[str, str]]:
    items = []
    for row in _get_json(TWSE_LISTED):
        code = _pick(row, ["公司代號", "Code", "SecuritiesCompanyCode"])
        name = _pick(row, ["公司名稱", "CompanyName", "簡稱", "Name"])
        industry = _pick(row, ["產業別", "Industry"], "股票")
        if code:
            items.append({"code": code, "name": name, "market": "上市", "type": _infer_type(code, name), "industry": industry})
    return items


def _tpex_stocks() -> List[Dict[str, str]]:
    items = []
    for row in _get_json(TPEX_LISTED):
        code = _pick(row, ["SecuritiesCompanyCode", "公司代號", "Code"])
        name = _pick(row, ["CompanyName", "公司名稱", "簡稱", "Name"])
        industry = _pick(row, ["Industry", "產業別"], "股票")
        if code:
            items.append({"code": code, "name": name, "market": "上櫃", "type": _infer_type(code, name), "industry": industry})
    return items


def _etfs() -> List[Dict[str, str]]:
    items = []
    for row in _get_json(TWSE_ETF):
        code = _pick(row, ["基金代號", "證券代號", "Code", "代號"])
        name = _pick(row, ["基金名稱", "證券名稱", "Name", "名稱"])
        if code:
            product_type = _infer_type(code, name)
            items.append({"code": code, "name": name, "market": "上市", "type": product_type, "industry": product_type})
    return items


@lru_cache(maxsize=1)
def get_all_products() -> List[Dict[str, str]]:
    items = []
    items.extend(_listed_stocks())
    items.extend(_tpex_stocks())
    items.extend(_etfs())
    items.extend(_firebase_products())
    items.extend(SEED_PRODUCTS)
    return _dedupe(items)


def refresh_products_cache() -> int:
    get_all_products.cache_clear()
    return len(get_all_products())


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
