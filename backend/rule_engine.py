from datetime import datetime
from typing import Any, Dict, List, Optional


AI_ANALYSIS_PROMPT = """You are a Taiwan stock analysis assistant.

Use only the JSON provided by the system. Do not invent missing prices, chip data, credit data, or shareholder data.
Return Traditional Chinese JSON only, without Markdown.

Required output schema:
{
  "stock": "string",
  "data_quality": {
    "usable": true,
    "warnings": ["string"]
  },
  "investment_view": {
    "direction": "bullish|neutral|bearish|uncertain",
    "confidence": 0,
    "timeframe": "short|swing|medium",
    "summary": "string"
  },
  "key_reasons": [
    {"type": "technical|chip|credit|risk|data", "text": "string", "evidence": "string"}
  ],
  "risk_controls": {
    "invalid_price": null,
    "support": null,
    "resistance": null,
    "position_sizing_note": "string"
  },
  "watch_next": ["string"],
  "not_included": ["string"]
}

Decision rules:
- Treat this as analysis and risk guidance, not guaranteed investment advice.
- If data_coverage shows missing or stale required data, lower confidence and explain.
- Prioritize rule_cards with available=true and level in strong_bullish, bullish, bearish, warning.
- Use missing_data to explicitly list what cannot be judged.
- Never recommend all-in, leverage, or guaranteed profit.
"""


def _num(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _round(value: Any, digits: int = 2) -> Optional[float]:
    value = _num(value)
    return round(value, digits) if value is not None else None


def _rule(
    rule_id: str,
    category: str,
    title: str,
    level: str,
    score: int,
    reason: str,
    logic: str,
    data: Optional[Dict[str, Any]] = None,
    available: bool = True,
) -> Dict[str, Any]:
    return {
        "id": rule_id,
        "category": category,
        "title": title,
        "level": level,
        "score": score,
        "reason": reason,
        "logic": logic,
        "available": available,
        "data": data or {},
    }


def _latest(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return rows[-1] if rows else {}


def _prev(rows: List[Dict[str, Any]], n: int = 1) -> Dict[str, Any]:
    return rows[-1 - n] if len(rows) > n else {}


def _ma_slope(kline: List[Dict[str, Any]], key: str, days: int = 5) -> Optional[float]:
    if len(kline) <= days:
        return None
    current = _num(kline[-1].get(key))
    past = _num(kline[-1 - days].get(key))
    if current is None or past is None:
        return None
    return round(current - past, 4)


def _prior_range(kline: List[Dict[str, Any]], days: int = 20):
    prior = kline[-days - 1:-1] if len(kline) > days else []
    highs = [_num(row.get("high")) for row in prior]
    lows = [_num(row.get("low")) for row in prior]
    highs = [value for value in highs if value is not None]
    lows = [value for value in lows if value is not None]
    return (max(highs) if highs else None, min(lows) if lows else None)


def _chip_latest(chip_rows: List[Dict[str, Any]], fallback: Dict[str, Any]) -> Dict[str, Any]:
    latest = _latest(chip_rows)
    return latest if latest else (fallback or {})


def _coverage(
    stock: str,
    kline: List[Dict[str, Any]],
    chip_rows: List[Dict[str, Any]],
    chip_metrics: Dict[str, Any],
    source: str,
) -> Dict[str, Any]:
    latest_k = _latest(kline)
    latest_chip = _chip_latest(chip_rows, {})
    chip_required = ["foreign_buy", "investment_trust_buy", "dealer_buy"]
    credit_required = ["margin_balance", "short_balance", "short_margin_ratio"]
    return {
        "stock": stock,
        "source": source,
        "generated_at": datetime.now().isoformat(),
        "stock_daily": {
            "available": len(kline) > 0,
            "rows": len(kline),
            "latest_date": latest_k.get("date"),
            "required_rows": 90,
            "enough_for_core_rules": len(kline) >= 60,
            "enough_for_reliable_analysis": len(kline) >= 90,
        },
        "chip_daily": {
            "available": len(chip_rows) > 0,
            "rows": len(chip_rows),
            "latest_date": latest_chip.get("date"),
            "has_institutional_fields": all(
                latest_chip.get(key) is not None for key in chip_required
            ),
        },
        "credit": {
            "available": any(chip_metrics.get(key) is not None for key in credit_required),
            "has_margin_short": chip_metrics.get("margin_balance") is not None
            and chip_metrics.get("short_balance") is not None,
            "has_short_margin_ratio": chip_metrics.get("short_margin_ratio") is not None,
        },
    }


def build_ai_rule_context(
    stock: str,
    meta: Dict[str, Any],
    kline: List[Dict[str, Any]],
    analysis: Dict[str, Any],
    perspective_cards: List[Dict[str, Any]],
    signals: Dict[str, Any],
    trade_plan: Dict[str, Any],
    chip_rows: List[Dict[str, Any]],
    chip_analysis: Dict[str, Any],
    source: str,
) -> Dict[str, Any]:
    latest = _latest(kline)
    prev = _prev(kline)
    chip_metrics = chip_analysis.get("metrics") if isinstance(chip_analysis, dict) else {}
    chip_metrics = chip_metrics or {}
    latest_chip = _chip_latest(chip_rows, {})
    coverage = _coverage(stock, kline, chip_rows, chip_metrics, source)

    close = _num(latest.get("close"))
    open_price = _num(latest.get("open"))
    low = _num(latest.get("low"))
    high = _num(latest.get("high"))
    prev_close = _num(prev.get("close"))
    prev_high = _num(prev.get("high"))
    prev_low = _num(prev.get("low"))
    ma5 = _num(latest.get("ma5"))
    ma10 = _num(latest.get("ma10"))
    ma20 = _num(latest.get("ma20"))
    ma60 = _num(latest.get("ma60"))
    ma60_slope = _ma_slope(kline, "ma60", 5)
    volume = _num(latest.get("volume"))
    volume_ma5 = _num(latest.get("volume_ma5"))
    change_pct = _num(latest.get("change_pct"), 0) or 0
    bb_upper = _num(latest.get("bb_upper"))
    bb_lower = _num(latest.get("bb_lower"))
    bb_width = _num(latest.get("bb_width"))
    prev_bb_width = _num(prev.get("bb_width"))
    price_20d_max_prev, price_20d_min_prev = _prior_range(kline, 20)
    bias20 = ((close - ma20) / ma20 * 100) if close and ma20 else None

    foreign_5d = _num(chip_metrics.get("foreign_5d_sum"), 0) or 0
    trust_5d = _num(chip_metrics.get("investment_trust_5d_sum"), 0) or 0
    dealer_5d = _num(chip_metrics.get("dealer_5d_sum"), 0) or 0
    institutional_5d = foreign_5d + trust_5d + dealer_5d
    institutional_1d = sum(
        _num(latest_chip.get(key), 0) or 0
        for key in ["foreign_buy", "investment_trust_buy", "dealer_buy"]
    )
    short_margin_ratio = _num(chip_metrics.get("short_margin_ratio"))

    rules: List[Dict[str, Any]] = []

    if all(value is not None for value in [ma5, ma10, ma20, ma60]):
        if ma5 > ma10 > ma20 > ma60 and (ma60_slope or 0) > 0:
            rules.append(_rule("trend.ma_bull_stack", "technical", "MA bullish alignment", "strong_bullish", 30, "MA5 > MA10 > MA20 > MA60 and MA60 is rising.", "MA5 > MA10 > MA20 > MA60 AND slope(MA60,5D) > 0", {"ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60, "ma60_slope_5d": ma60_slope}))
        elif ma5 < ma20 < ma60:
            rules.append(_rule("trend.ma_bear_stack", "technical", "MA bearish alignment", "bearish", -25, "Short trend is below medium and long averages.", "MA5 < MA20 < MA60", {"ma5": ma5, "ma20": ma20, "ma60": ma60}))
        else:
            rules.append(_rule("trend.ma_mixed", "technical", "MA mixed state", "neutral", 0, "Moving averages do not show a clean one-sided trend.", "MA alignment mixed", {"ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60}))
    else:
        rules.append(_rule("trend.ma_unavailable", "technical", "MA unavailable", "warning", 0, "Not enough price history for MA trend judgement.", "Need MA5/MA10/MA20/MA60", available=False))

    if close is not None and price_20d_max_prev is not None and close > price_20d_max_prev:
        rules.append(_rule("breakout.20d_high", "technical", "20D breakout", "bullish", 14, "Close is above the prior 20-day high.", "Close > prior 20D high", {"close": close, "prior_20d_high": price_20d_max_prev}))
    elif close is not None and price_20d_min_prev is not None and close < price_20d_min_prev:
        rules.append(_rule("breakdown.20d_low", "technical", "20D breakdown", "bearish", -14, "Close is below the prior 20-day low.", "Close < prior 20D low", {"close": close, "prior_20d_low": price_20d_min_prev}))

    if low is not None and prev_high is not None and low > prev_high:
        rules.append(_rule("price.gap_up", "technical", "Gap up", "warning", 6, "Today low is above previous high; momentum is strong but may need pullback risk control.", "Low[0] > High[-1]", {"low": low, "previous_high": prev_high}))
    elif high is not None and prev_low is not None and high < prev_low:
        rules.append(_rule("price.gap_down", "technical", "Gap down", "bearish", -12, "Today high is below previous low; price action is weak.", "High[0] < Low[-1]", {"high": high, "previous_low": prev_low}))

    if bb_upper and bb_width and prev_bb_width and close and close > bb_upper and bb_width > prev_bb_width * 1.2:
        rules.append(_rule("volatility.bb_breakout", "technical", "Bollinger expansion breakout", "bullish", 22, "Close is above upper Bollinger band while band width expands.", "Close > BB_UPPER AND BB_WIDTH > prev_BB_WIDTH * 1.2", {"close": close, "bb_upper": bb_upper, "bb_width": bb_width, "prev_bb_width": prev_bb_width}))
    elif bb_lower and close and close < bb_lower:
        rules.append(_rule("volatility.bb_breakdown", "technical", "Bollinger lower break", "bearish", -16, "Close is below lower Bollinger band.", "Close < BB_LOWER", {"close": close, "bb_lower": bb_lower}))

    if volume and volume_ma5 and change_pct > 3 and volume > volume_ma5:
        rules.append(_rule("volume.price_up_confirmed", "technical", "Volume-backed rise", "bullish", 18, "Price rises more than 3% with volume above 5-day average.", "Volume > V_MA5 AND ChangePct > 3", {"volume": volume, "volume_ma5": volume_ma5, "change_pct": change_pct}))
    elif volume and volume_ma5 and change_pct < -3 and volume > volume_ma5:
        rules.append(_rule("volume.price_down_confirmed", "technical", "Volume-backed fall", "bearish", -22, "Price falls more than 3% with volume above 5-day average.", "Volume > V_MA5 AND ChangePct < -3", {"volume": volume, "volume_ma5": volume_ma5, "change_pct": change_pct}))

    if bias20 is not None and bias20 > 15:
        rules.append(_rule("risk.bias20_overheat", "risk", "20D bias overheated", "warning", -8, "Close is more than 15% above MA20.", "Bias20 > 15", {"bias20": round(bias20, 2), "close": close, "ma20": ma20}))

    if chip_rows:
        if institutional_5d > 0:
            rules.append(_rule("chip.institutional_5d_buy", "chip", "Institutional 5D net buy", "bullish", 16, "Foreign, investment trust and dealer 5-day sum is positive.", "foreign_5d + trust_5d + dealer_5d > 0", {"institutional_5d": round(institutional_5d, 2), "foreign_5d": foreign_5d, "investment_trust_5d": trust_5d, "dealer_5d": dealer_5d}))
        elif institutional_5d < 0:
            rules.append(_rule("chip.institutional_5d_sell", "chip", "Institutional 5D net sell", "bearish", -16, "Foreign, investment trust and dealer 5-day sum is negative.", "foreign_5d + trust_5d + dealer_5d < 0", {"institutional_5d": round(institutional_5d, 2), "foreign_5d": foreign_5d, "investment_trust_5d": trust_5d, "dealer_5d": dealer_5d}))

        if close and open_price and prev_close and close < open_price and close > prev_close and institutional_1d > 0:
            rules.append(_rule("chip.black_k_accumulation", "chip", "Black-K accumulation", "bullish", 12, "Close is below open but above previous close while institutions net buy.", "Close < Open AND Close > PrevClose AND institutional_1d > 0", {"open": open_price, "close": close, "previous_close": prev_close, "institutional_1d": institutional_1d, "chip_date": latest_chip.get("date")}))
    else:
        rules.append(_rule("chip.unavailable", "chip", "Chip unavailable", "warning", 0, "No chip_daily rows are available for this stock.", "Need chip_daily rows", available=False))

    if short_margin_ratio is not None and close is not None and price_20d_max_prev is not None and short_margin_ratio > 30 and close >= price_20d_max_prev:
        rules.append(_rule("credit.short_squeeze", "credit", "Short squeeze watch", "bullish", 10, "Short/margin ratio is above 30% and price is at or above prior 20-day high.", "Short_Margin_Ratio > 30 AND Close >= prior 20D high", {"short_margin_ratio": short_margin_ratio, "close": close, "prior_20d_high": price_20d_max_prev}))
    elif short_margin_ratio is None:
        rules.append(_rule("credit.short_margin_unavailable", "credit", "Credit unavailable", "warning", 0, "Missing margin balance, short balance, or short/margin ratio.", "Need margin_balance and short_balance", available=False))

    missing_data = list(analysis.get("missing_data") or [])
    unavailable = [
        "large_holder_400_lots",
        "shareholder_concentration",
        "retail_shareholder_count",
        "margin_maintenance_ratio",
    ]
    missing_data.extend([
        "large_holder_400_lots/shareholder_concentration: not in current Firestore schema",
        "retail_shareholder_count: not in current Firestore schema",
        "margin_maintenance_ratio: not in current Firestore schema",
    ])

    total_score = sum(rule["score"] for rule in rules if rule.get("available"))
    if total_score >= 35:
        direction = "bullish"
    elif total_score <= -25:
        direction = "bearish"
    elif abs(total_score) < 10:
        direction = "neutral"
    else:
        direction = "uncertain"

    confidence = 45
    if coverage["stock_daily"]["enough_for_reliable_analysis"]:
        confidence += 20
    elif coverage["stock_daily"]["enough_for_core_rules"]:
        confidence += 10
    if coverage["chip_daily"]["available"]:
        confidence += 15
    if coverage["credit"]["has_short_margin_ratio"]:
        confidence += 10
    confidence = max(0, min(90, confidence))

    return {
        "schema_version": "ai_stock_context_v1",
        "stock": stock,
        "meta": meta,
        "data_coverage": coverage,
        "market_data": {
            "latest": latest,
            "previous": prev,
            "prior_20d_high": _round(price_20d_max_prev),
            "prior_20d_low": _round(price_20d_min_prev),
            "recent_kline": kline[-80:],
        },
        "technical": {
            "score": analysis.get("score"),
            "rating": analysis.get("rating"),
            "trend": analysis.get("trend"),
            "summary": analysis.get("summary"),
            "indicators": analysis.get("indicators") or {},
        },
        "chip": {
            "latest": latest_chip,
            "latest_rows": chip_rows[-20:],
            "analysis": chip_analysis,
        },
        "ui_context": {
            "perspective_cards": perspective_cards,
            "signals": signals,
            "trade_plan": trade_plan,
        },
        "rule_engine": {
            "direction": direction,
            "score": total_score,
            "confidence": confidence,
            "rule_cards": sorted(rules, key=lambda item: abs(item.get("score", 0)), reverse=True),
        },
        "missing_data": sorted(set(str(item) for item in missing_data if item)),
        "unavailable_decision_inputs": unavailable,
        "ai_prompt_template": AI_ANALYSIS_PROMPT,
    }
