from datetime import datetime
import sys
import threading
import time

_INSTALLED = False


def _main():
    return sys.modules.get("main") or sys.modules.get("__main__")


def _num(value, default=0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _analyze_chip(chip):
    foreign = _num(chip.get("foreign") or chip.get("foreign_buy_sell") or chip.get("foreign_net"))
    trust = _num(chip.get("investment_trust") or chip.get("trust") or chip.get("trust_buy_sell"))
    dealer = _num(chip.get("dealer") or chip.get("dealer_buy_sell"))
    margin = _num(chip.get("margin") or chip.get("margin_balance"), None)
    short = _num(chip.get("short") or chip.get("short_balance"), None)

    score = 0
    reasons = []

    if foreign > 0:
        score += 20
        reasons.append("外資買超，籌碼偏多。")
    elif foreign < 0:
        score -= 20
        reasons.append("外資賣超，籌碼偏空。")
    else:
        reasons.append("外資資料持平或尚未完整。")

    if trust > 0:
        score += 25
        reasons.append("投信買超，通常代表中期法人支撐。")
    elif trust < 0:
        score -= 15
        reasons.append("投信賣超，中期籌碼偏弱。")

    if dealer > 0:
        score += 8
        reasons.append("自營商買超，短線籌碼加分。")
    elif dealer < 0:
        score -= 8
        reasons.append("自營商賣超，短線籌碼扣分。")

    if margin is not None and short is not None and margin > 0:
        short_ratio = round(short / margin * 100, 2)
        if short_ratio > 30:
            score += 8
            reasons.append("券資比偏高，若股價轉強可能有軋空機會。")
        elif margin > 0 and short_ratio < 5:
            reasons.append("券資比偏低，籌碼軋空力道有限。")
    else:
        short_ratio = None
        reasons.append("融資融券資料尚未完整。")

    if score >= 30:
        status = "籌碼偏多"
        level = "bullish"
    elif score <= -25:
        status = "籌碼偏空"
        level = "bearish"
    else:
        status = "籌碼中性"
        level = "neutral"

    return {
        "score": score,
        "status": status,
        "level": level,
        "meaning": "籌碼面用來判斷法人與信用交易是否支持股價趨勢。外資、投信、自營商偏買超通常有利於趨勢延續；若法人賣超或融資過高，則需留意反轉風險。",
        "reasons": reasons,
        "metrics": {
            "foreign": foreign,
            "investment_trust": trust,
            "dealer": dealer,
            "margin": margin,
            "short": short,
            "short_margin_ratio": short_ratio,
        },
    }


def _install(app):
    global _INSTALLED
    if _INSTALLED:
        return

    @app.get("/api/chip/{stock}")
    def chip_analysis(stock: str):
        m = _main()
        code = m.normalize_stock(stock) if hasattr(m, "normalize_stock") else str(stock).strip().upper()
        chip = {}
        try:
            chip = m.get_latest_chip_daily(code) or {}
        except Exception as exc:
            chip = {"error": str(exc)}
        result = _analyze_chip(chip)
        return {
            "status": "ok",
            "stock": stock,
            "normalized_stock": code,
            "source": "Firebase chip_data latest",
            "latest_chip": chip,
            "analysis": result,
            "updated_at": datetime.now().isoformat(),
        }

    _INSTALLED = True


def boot():
    def wait():
        for _ in range(120):
            m = _main()
            if m and hasattr(m, "app"):
                _install(m.app)
                return
            time.sleep(0.1)
    threading.Thread(target=wait, daemon=True).start()


boot()
