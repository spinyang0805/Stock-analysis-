from datetime import datetime
from typing import Any, Dict, List
import requests

REQUEST_TIMEOUT = 4

STOCK_INFO_MAP = {
    "2330": {"name": "台積電", "market": "上市", "industry": "半導體"},
    "3702": {"name": "大聯大", "market": "上市", "industry": "電子通路"},
    "2317": {"name": "鴻海", "market": "上市", "industry": "其他電子"},
    "2454": {"name": "聯發科", "market": "上市", "industry": "半導體"},
    "2382": {"name": "廣達", "market": "上市", "industry": "電腦及週邊"},
    "3231": {"name": "緯創", "market": "上市", "industry": "電腦及週邊"},
    "2324": {"name": "仁寶", "market": "上市", "industry": "電腦及週邊"},
    "2308": {"name": "台達電", "market": "上市", "industry": "電子零組件"},
    "2357": {"name": "華碩", "market": "上市", "industry": "電腦及週邊"},
}


def _fallback_basic(code: str, source: str = "realtime-unavailable") -> Dict[str, Any]:
    info = STOCK_INFO_MAP.get(str(code), {})
    return {
        "code": str(code),
        "name": info.get("name", str(code)),
        "market": info.get("market", "--"),
        "industry": info.get("industry", "--"),
        "price": None,
        "previous_close": None,
        "change": None,
        "change_pct": None,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "volume_lot": None,
        "time": None,
        "bids": [],
        "asks": [],
        "source": source,
    }


def _to_num(value):
    try:
        if value in (None, "", "-", "--", "_"):
            return None
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def _to_int(value):
    n = _to_num(value)
    return None if n is None else int(n)


def fetch_realtime_board(code: str) -> Dict[str, Any]:
    code = str(code)
    fallback = _fallback_basic(code)
    headers = {"User-Agent": "Mozilla/5.0", "Referer": f"https://mis.twse.com.tw/stock/fibest.jsp?stock={code}"}
    for prefix, market in [("tse", "上市"), ("otc", "上櫃")]:
        try:
            url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
            params = {"ex_ch": f"{prefix}_{code}.tw", "json": "1", "delay": "0", "_": int(datetime.now().timestamp() * 1000)}
            data = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT).json()
            rows = data.get("msgArray") or []
            if not rows:
                continue
            q = rows[0]
            buy_prices = [x for x in str(q.get("g", "")).split("_") if x]
            buy_qty = [x for x in str(q.get("b", "")).split("_") if x]
            sell_prices = [x for x in str(q.get("f", "")).split("_") if x]
            sell_qty = [x for x in str(q.get("a", "")).split("_") if x]
            bids = [{"price": _to_num(p), "qty": _to_int(buy_qty[i]) if i < len(buy_qty) else None} for i, p in enumerate(buy_prices[:5])]
            asks = [{"price": _to_num(p), "qty": _to_int(sell_qty[i]) if i < len(sell_qty) else None} for i, p in enumerate(sell_prices[:5])]
            price = _to_num(q.get("z")) or _to_num(q.get("y"))
            prev = _to_num(q.get("y"))
            info = STOCK_INFO_MAP.get(code, {})
            return {
                "code": code,
                "name": q.get("n") or info.get("name") or code,
                "market": market or info.get("market", "--"),
                "industry": info.get("industry", "--"),
                "price": price,
                "previous_close": prev,
                "change": None if price is None or prev is None else round(price - prev, 2),
                "change_pct": None if price is None or prev in (None, 0) else round((price - prev) / prev * 100, 2),
                "open": _to_num(q.get("o")),
                "high": _to_num(q.get("h")),
                "low": _to_num(q.get("l")),
                "close": price,
                "volume_lot": _to_int(q.get("v")),
                "time": q.get("t"),
                "bids": bids,
                "asks": asks,
                "source": "TWSE MIS",
            }
        except Exception:
            continue
    return fallback


def fetch_institutional(code: str, date: str = None) -> Dict[str, Any]:
    date = date or datetime.now().strftime("%Y%m%d")
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    try:
        payload = requests.get(url, params={"response": "json", "date": date, "selectType": "ALL"}, timeout=REQUEST_TIMEOUT).json()
        for row in payload.get("data", []):
            if row[0] == code:
                return {"date": date, "foreign": _to_int(row[4]), "investment_trust": _to_int(row[10]), "dealer": _to_int(row[11]), "source": "TWSE T86"}
    except Exception:
        pass
    return {"date": date, "foreign": None, "investment_trust": None, "dealer": None, "source": "T86-unavailable"}


def fetch_margin(code: str, date: str = None) -> Dict[str, Any]:
    date = date or datetime.now().strftime("%Y%m%d")
    url = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
    try:
        payload = requests.get(url, params={"response": "json", "date": date, "selectType": "ALL"}, timeout=REQUEST_TIMEOUT).json()
        for row in payload.get("data", []):
            if row[0] == code:
                return {"date": date, "margin_balance": _to_int(row[12]), "short_balance": _to_int(row[15]), "source": "TWSE MI_MARGN"}
    except Exception:
        pass
    return {"date": date, "margin_balance": None, "short_balance": None, "source": "margin-unavailable"}


def analyze_dashboard(code: str, kline: List[Dict[str, Any]], analysis: Dict[str, Any], realtime: Dict[str, Any], inst: Dict[str, Any], margin: Dict[str, Any]) -> Dict[str, Any]:
    latest = kline[-1] if kline else {}
    ma5, ma20, ma60 = latest.get("ma5"), latest.get("ma20"), latest.get("ma60")
    rsi, macd, macd_signal = latest.get("rsi14"), latest.get("macd"), latest.get("macd_signal")
    vol5 = sum((x.get("volume") or 0) for x in kline[-5:]) / max(1, len(kline[-5:])) if kline else 0
    vol_now = latest.get("volume") or 0
    score = int(analysis.get("score") or 0)
    foreign = inst.get("foreign") or 0
    investment = inst.get("investment_trust") or 0
    margin_balance = margin.get("margin_balance")
    short_balance = margin.get("short_balance")
    chip_score = 0
    if foreign > 0:
        chip_score += 12
    if investment > 0:
        chip_score += 10
    if margin_balance is not None and short_balance is not None and short_balance > 0:
        chip_score += 4
    if margin_balance is None:
        chip_note = "融資融券資料待盤後更新"
    elif foreign < 0 and margin_balance > 0:
        chip_note = "外資偏賣，需留意散戶接手風險"
    else:
        chip_note = "籌碼結構中性偏觀察"
    total = score + chip_score
    trend = "多頭趨勢" if total >= 30 else "中性偏多" if total >= 10 else "偏空觀望" if total < -10 else "區間整理"
    return {
        "technical": {"trend_direction": trend, "ma_state": "多頭排列" if ma5 and ma20 and ma60 and ma5 > ma20 > ma60 else "均線整理/未成多排", "rsi": rsi, "macd_state": "MACD偏多" if macd is not None and macd_signal is not None and macd > macd_signal else "MACD偏弱/整理", "volume_state": "量能放大" if vol_now > vol5 * 1.2 else "量能普通/收斂"},
        "chip": {"foreign": inst.get("foreign"), "investment_trust": inst.get("investment_trust"), "dealer": inst.get("dealer"), "margin_balance": margin.get("margin_balance"), "short_balance": margin.get("short_balance"), "chip_score": chip_score, "chip_note": chip_note},
        "scenario": {"breakout": "放量站回短均線，可觀察追價延續" if total >= 20 else "需先突破近期壓力再確認", "pullback": "回測MA20不破可觀察低接" if ma20 else "資料不足", "risk": "若跌破近期低點，短線轉弱需控風險"},
        "final": f"{code} 綜合分數 {total}，目前判斷為「{trend}」。{chip_note}。",
        "total_score": total,
    }
