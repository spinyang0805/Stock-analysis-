from __future__ import annotations

from functools import lru_cache
from typing import Dict, List
import requests

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
TIMEOUT = 12

TWSE_LISTED = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_LISTED = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TWSE_ETF = "https://openapi.twse.com.tw/v1/opendata/t187ap03_ETF"
# Bond endpoints are not as stable across TWSE/TPEx. We still include a curated seed
# and merge remote results when the open data response is available.
TWSE_BOND_CANDIDATES = [
    "https://openapi.twse.com.tw/v1/opendata/t187ap03_B",
    "https://openapi.twse.com.tw/v1/opendata/t187ap03_C",
]

SEED_PRODUCTS = [
    {"code": "00679B", "name": "元大美債20年", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00687B", "name": "國泰20年美債", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00720B", "name": "元大投資級公司債", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00725B", "name": "國泰投資級公司債", "market": "上市", "type": "債券ETF", "industry": "債券"},
    {"code": "00857B", "name": "永豐20年美公債", "market": "上市", "type": "債券ETF", "industry": "債券"},
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


def _listed_stocks() -> List[Dict[str, str]]:
    items = []
    for row in _get_json(TWSE_LISTED):
        code = _pick(row, ["公司代號", "Code", "SecuritiesCompanyCode"])
        name = _pick(row, ["公司名稱", "CompanyName", "名稱"])
        industry = _pick(row, ["產業別", "Industry", "產業類別"], "股票")
        if code and code.isdigit():
            items.append({"code": code, "name": name, "market": "上市", "type": "股票", "industry": industry})
    return items


def _tpex_stocks() -> List[Dict[str, str]]:
    items = []
    for row in _get_json(TPEX_LISTED):
        code = _pick(row, ["SecuritiesCompanyCode", "公司代號", "Code"])
        name = _pick(row, ["CompanyName", "公司名稱", "名稱"])
        industry = _pick(row, ["Industry", "產業別", "產業類別"], "股票")
        if code and code.isdigit():
            items.append({"code": code, "name": name, "market": "上櫃", "type": "股票", "industry": industry})
    return items


def _etfs() -> List[Dict[str, str]]:
    items = []
    for row in _get_json(TWSE_ETF):
        code = _pick(row, ["證券代號", "基金代號", "Code", "代號"])
        name = _pick(row, ["證券名稱", "基金名稱", "Name", "名稱"])
        if code:
            product_type = "債券ETF" if "債" in name or code.upper().endswith("B") else "ETF"
            items.append({"code": code, "name": name, "market": "上市", "type": product_type, "industry": product_type})
    return items


def _bonds() -> List[Dict[str, str]]:
    items = []
    for url in TWSE_BOND_CANDIDATES:
        for row in _get_json(url):
            code = _pick(row, ["證券代號", "債券代號", "Code", "代號"])
            name = _pick(row, ["證券名稱", "債券名稱", "Name", "名稱"])
            if code and name:
                items.append({"code": code, "name": name, "market": "上市", "type": "債券", "industry": "債券"})
    return items


@lru_cache(maxsize=1)
def get_all_products() -> List[Dict[str, str]]:
    items = []
    items.extend(SEED_PRODUCTS)
    items.extend(_listed_stocks())
    items.extend(_tpex_stocks())
    items.extend(_etfs())
    items.extend(_bonds())
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
