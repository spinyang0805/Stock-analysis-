from typing import Any, Dict, List, Optional


def _num(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _latest(kline: List[Dict[str, Any]]) -> Dict[str, Any]:
    return kline[-1] if kline else {}


def _prev(kline: List[Dict[str, Any]], n: int = 1) -> Dict[str, Any]:
    return kline[-1 - n] if len(kline) > n else {}


def _chip_value(chip: Dict[str, Any], *keys: str, default: float = 0) -> float:
    for key in keys:
        value = chip.get(key)
        if value is not None:
            return _num(value, default) or default
    data = chip.get("data") if isinstance(chip.get("data"), dict) else {}
    for key in keys:
        value = data.get(key)
        if value is not None:
            return _num(value, default) or default
    indicators = chip.get("indicators") if isinstance(chip.get("indicators"), dict) else {}
    for section in indicators.values():
        if isinstance(section, dict):
            for key in keys:
                value = section.get(key)
                if value is not None:
                    return _num(value, default) or default
    return default


def generate_perspective_cards(kline: List[Dict[str, Any]], chip: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    chip = chip or {}
    if not kline:
        return [{
            "category": "data",
            "title": "資料狀態",
            "status": "資料不足",
            "level": "warning",
            "meaning": "目前沒有足夠K線資料，無法完成多面向分析。",
            "logic": "len(kline) > 0",
        }]

    last = _latest(kline)
    prev = _prev(kline)
    close = _num(last.get("close"))
    low = _num(last.get("low"))
    high_prev = _num(prev.get("high"))
    ma5 = _num(last.get("ma5"))
    ma10 = _num(last.get("ma10"))
    ma20 = _num(last.get("ma20"))
    ma60 = _num(last.get("ma60"))
    volume = _num(last.get("volume"), 0) or 0
    volume_ma5 = _num(last.get("volume_ma5") or last.get("v_ma5"), 0) or 0
    change_pct = _num(last.get("change_pct"), 0) or 0
    bb_upper = _num(last.get("bb_upper"))
    bb_width = _num(last.get("bb_width"))
    prev_bb_width = _num(prev.get("bb_width"))
    price_20d_max = max([_num(x.get("high"), 0) or 0 for x in kline[-20:]]) if len(kline) >= 20 else None

    foreign_5d = _chip_value(chip, "foreign_5d_sum", "foreign_5d")
    trust_5d = _chip_value(chip, "trust_5d_sum", "trust_5d", "investment_trust_5d_sum")
    dealer_5d = _chip_value(chip, "dealer_5d_sum", "dealer_5d")
    margin_ratio = _chip_value(chip, "margin_ratio", "margin_usage_ratio")
    short_margin_ratio = _chip_value(chip, "short_margin_ratio", "margin_short_ratio", "券資比")

    cards: List[Dict[str, Any]] = []

    if ma5 and ma10 and ma20 and ma60 and ma5 > ma10 > ma20 > ma60:
        trend_status, trend_level, trend_meaning, trend_logic = "四線多排", "strong_bullish", "5日、10日、20日、60日均線由上而下排列，代表最強勢多頭結構。", "MA5 > MA10 > MA20 > MA60"
    elif ma5 and ma20 and ma60 and ma5 > ma20 > ma60:
        trend_status, trend_level, trend_meaning, trend_logic = "多頭排列", "bullish", "短中長期均線依序向上，代表趨勢偏強。", "MA5 > MA20 > MA60"
    elif ma5 and ma20 and ma60 and ma5 < ma20 < ma60:
        trend_status, trend_level, trend_meaning, trend_logic = "空頭排列", "bearish", "均線向下發散，反彈容易遇到壓力。", "MA5 < MA20 < MA60"
    else:
        trend_status, trend_level, trend_meaning, trend_logic = "盤整觀望", "neutral", "均線尚未形成明確方向，多空力量仍在拉鋸。", "MA alignment"
    cards.append({"category": "trend", "title": "趨勢面", "status": trend_status, "level": trend_level, "meaning": trend_meaning, "logic": trend_logic})

    if bb_upper and close and close > bb_upper and bb_width and prev_bb_width and bb_width > prev_bb_width * 1.2:
        vp_status, vp_level, vp_meaning, vp_logic = "開布林突破", "strong_bullish", "布林通道由收斂轉擴張且股價突破上軌，代表波動與攻擊力同步放大。", "BB_WIDTH[0] > BB_WIDTH[1] * 1.2 AND Close > BB_UPPER"
    elif volume_ma5 and volume > volume_ma5 and change_pct > 3:
        vp_status, vp_level, vp_meaning, vp_logic = "量增價漲", "bullish", "成交量放大且股價上漲，屬於健康多頭攻擊訊號。", "Volume > Volume_MA5 AND Change > 3%"
    elif volume_ma5 and volume > volume_ma5 and change_pct < -3:
        vp_status, vp_level, vp_meaning, vp_logic = "量增價跌", "bearish", "量能放大但價格下跌，可能是出貨或恐慌賣壓。", "Volume > Volume_MA5 AND Change < -3%"
    elif volume_ma5 and volume < volume_ma5 and change_pct > 0:
        vp_status, vp_level, vp_meaning, vp_logic = "量縮價漲", "warning", "價格上漲但量能不足，追價意願偏弱。", "Volume < Volume_MA5 AND Change > 0"
    else:
        vp_status, vp_level, vp_meaning, vp_logic = "量價中性", "neutral", "目前量價結構沒有明顯攻擊或出貨訊號。", "Volume / Price matrix"
    cards.append({"category": "volume_price", "title": "量價面", "status": vp_status, "level": vp_level, "meaning": vp_meaning, "logic": vp_logic})

    institutional_5d = foreign_5d + trust_5d + dealer_5d
    if trust_5d > 0 and trust_5d >= abs(institutional_5d) * 0.3:
        chip_status, chip_level, chip_meaning = "投信偏多", "bullish", "投信買盤具連續性，若量能配合容易形成波段走勢。"
    elif institutional_5d > 0:
        chip_status, chip_level, chip_meaning = "法人買超", "bullish", "外資、投信與自營商近5日合計偏買，籌碼面偏多。"
    elif institutional_5d < 0:
        chip_status, chip_level, chip_meaning = "法人賣超", "bearish", "法人近5日合計偏賣，代表資金流出壓力。"
    else:
        chip_status, chip_level, chip_meaning = "籌碼中性", "neutral", "目前法人籌碼未明顯偏向，需等待連續性。"
    cards.append({"category": "chip", "title": "籌碼面", "status": chip_status, "level": chip_level, "meaning": chip_meaning, "logic": "foreign_5d_sum + trust_5d_sum + dealer_5d_sum"})

    if short_margin_ratio > 30 and price_20d_max and close and close >= price_20d_max:
        credit_status, credit_level, credit_meaning, credit_logic = "軋空條件成立", "strong_bullish", "券資比偏高且股價突破20日高點，空方可能被迫回補推升股價。", "Short_Margin_Ratio > 30% AND Close >= Price_20D_Max"
    elif margin_ratio > 60:
        credit_status, credit_level, credit_meaning, credit_logic = "融資過高", "warning", "融資使用率偏高，若股價轉弱容易造成多殺多。", "Margin_Ratio > 60%"
    else:
        credit_status, credit_level, credit_meaning, credit_logic = "信用正常", "neutral", "目前信用交易沒有明顯軋空或斷頭條件。", "Short/Margin risk check"
    cards.append({"category": "credit", "title": "信用交易", "status": credit_status, "level": credit_level, "meaning": credit_meaning, "logic": credit_logic})

    bias20 = ((close - ma20) / ma20 * 100) if close and ma20 else 0
    if ma60 and close and close < ma60:
        risk_status, risk_level, risk_meaning, risk_logic = "跌破季線", "bearish", "股價跌破MA60，中期趨勢轉弱，需降低風險。", "Close < MA60"
    elif margin_ratio > 60:
        risk_status, risk_level, risk_meaning, risk_logic = "籌碼過熱", "warning", "融資過高，若價格轉弱可能放大下跌。", "Margin_Ratio > 60%"
    elif bias20 > 15:
        risk_status, risk_level, risk_meaning, risk_logic = "正乖離過大", "warning", "股價距離月線過遠，短線追高風險增加。", "Bias20 > 15%"
    elif low and high_prev and low > high_prev:
        risk_status, risk_level, risk_meaning, risk_logic = "跳空強勢但留意缺口", "warning", "向上跳空代表力道強，但短線也要留意缺口回補。", "Low[0] > High[1]"
    else:
        risk_status, risk_level, risk_meaning, risk_logic = "風險可控", "bullish", "目前未出現主要技術或信用風險訊號。", "Close >= MA60 AND risk filters pass"
    cards.append({"category": "risk", "title": "風險面", "status": risk_status, "level": risk_level, "meaning": risk_meaning, "logic": risk_logic})

    return cards
