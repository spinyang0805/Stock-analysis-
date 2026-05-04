from typing import Any, Dict, List


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def generate_signals(kline: List[Dict[str, Any]], chip: Dict[str, Any]) -> Dict[str, Any]:
    if len(kline) < 60:
        return {"signals": [], "risks": [], "action": "HOLD"}

    last = kline[-1]
    close = _num(last.get("close"))
    ma5 = _num(last.get("ma5"))
    ma10 = _num(last.get("ma10"))
    ma20 = _num(last.get("ma20"))
    ma60 = _num(last.get("ma60"))
    bb_upper = _num(last.get("bb_upper"))
    change_pct = _num(last.get("change_pct"))

    foreign_5d = _num(chip.get("foreign_5d_sum", 0))
    margin_ratio = _num(chip.get("margin_ratio", 0))
    short_ratio = _num(chip.get("short_margin_ratio", 0))

    signals, risks, actions = [], [], []

    if ma5 > ma10 > ma20 > ma60:
        signals.append("四線多排")
        actions.append("STRONG_BUY")

    if bb_upper and close > bb_upper:
        signals.append("開布林")
        actions.append("BUY")

    if close > ma20 and change_pct > 0:
        signals.append("回檔轉強")
        actions.append("BUY")

    if foreign_5d > 0:
        signals.append("外資偏多")

    if short_ratio > 30:
        signals.append("軋空")

    if margin_ratio > 60:
        risks.append("融資過高")

    if close < ma60:
        risks.append("跌破季線")

    if "STRONG_BUY" in actions:
        action = "STRONG_BUY"
    elif "BUY" in actions:
        action = "BUY"
    elif risks:
        action = "SELL"
    else:
        action = "HOLD"

    return {"signals": signals, "risks": risks, "action": action}


def generate_trade_plan(kline: List[Dict[str, Any]]) -> Dict[str, Any]:
    last = kline[-1]
    close = _num(last.get("close"))
    ma20 = _num(last.get("ma20"))
    ma60 = _num(last.get("ma60"))
    bb_upper = _num(last.get("bb_upper"))

    return {
        "buy_zone": round(ma20 * 1.01, 2) if ma20 else None,
        "breakout": round(bb_upper, 2) if bb_upper else None,
        "stop_loss": round(ma60 * 0.98, 2) if ma60 else None,
        "target": round(close * 1.1, 2) if close else None,
    }


def backtest_strategy(kline: List[Dict[str, Any]]) -> Dict[str, Any]:
    capital = 100000
    position = 0
    trades = []

    for i in range(60, len(kline)):
        row = kline[i]
        close = _num(row.get("close"))
        ma20 = _num(row.get("ma20"))
        ma60 = _num(row.get("ma60"))

        if position == 0 and close > ma20:
            position = capital / close
            trades.append({"type": "BUY", "price": close})
        elif position > 0 and close < ma60:
            capital = position * close
            position = 0
            trades.append({"type": "SELL", "price": close})

    final = capital if position == 0 else position * _num(kline[-1].get("close"))

    return {
        "final_capital": round(final, 2),
        "return_pct": round((final - 100000) / 100000 * 100, 2),
        "trades": trades
    }
