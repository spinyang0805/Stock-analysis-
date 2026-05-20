from datetime import datetime, timedelta
import math
import random
import sys
import threading
import time

from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

try:
    from jobs import today_str, write_margin_chips, write_t86_chips
except Exception:
    today_str = write_margin_chips = write_t86_chips = None

_INSTALLED = False
_CHIP_RESPONSE_CACHE = {}
CHIP_RESPONSE_CACHE_TTL_SECONDS = 60


def _cache_get(key):
    item = _CHIP_RESPONSE_CACHE.get(key)
    if not item:
        return None
    expires_at, payload = item
    if expires_at < time.time():
        _CHIP_RESPONSE_CACHE.pop(key, None)
        return None
    return payload


def _cache_set(key, payload):
    _CHIP_RESPONSE_CACHE[key] = (time.time() + CHIP_RESPONSE_CACHE_TTL_SECONDS, payload)
    return payload


def _has_institutional_row(row):
    if not isinstance(row, dict):
        return False
    return any(row.get(key) is not None for key in ["foreign_buy", "investment_trust_buy", "dealer_buy", "foreign", "investment_trust", "dealer"])


def _has_real_institutional_rows(rows):
    return any(_has_institutional_row(row) and row.get("source") != "generated_seed_v1" for row in rows or [])


def _try_live_chip_backfill(code):
    if write_t86_chips is None or write_margin_chips is None or today_str is None:
        return {"attempted": False, "reason": "jobs_not_available"}
    result = {"chips": 0, "margin_rows": 0, "errors": []}
    date_text = today_str()
    try:
        write_t86_chips(date_text, result)
        write_margin_chips(date_text, result)
        return {"attempted": True, "date": date_text, "chips": result.get("chips", 0), "margin_rows": result.get("margin_rows", 0), "errors": result.get("errors", [])[-5:]}
    except Exception as exc:
        return {"attempted": True, "date": date_text, "error": str(exc)}


def _main():
    return sys.modules.get("main") or sys.modules.get("__main__")


def _json(payload):
    return JSONResponse(jsonable_encoder(payload), media_type="application/json; charset=utf-8")


def _num(value, default=0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _date_list(days=20):
    dates = []
    d = datetime.now()
    while len(dates) < days:
        if d.weekday() < 5:
            dates.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return list(reversed(dates))


def _mock_chip_rows(code, days=20):
    random.seed(str(code))
    rows = []
    base = (sum(ord(c) for c in str(code)) % 11) - 5
    for i, date in enumerate(_date_list(days)):
        wave = math.sin(i / 3) * 2
        foreign = round((base + wave + random.uniform(-3, 3)) * 1000, 0)
        trust = round((base / 2 + random.uniform(-2, 2)) * 350, 0)
        dealer = round(random.uniform(-1.5, 1.5) * 500, 0)
        margin_balance = round(8000 + random.uniform(-600, 600) + i * random.uniform(-30, 30), 0)
        short_balance = round(max(0, 900 + random.uniform(-200, 300)), 0)
        rows.append({
            "date": date,
            "foreign_buy": foreign,
            "investment_trust_buy": trust,
            "dealer_buy": dealer,
            "margin_balance": margin_balance,
            "short_balance": short_balance,
            "source": "generated_seed_v1",
        })
    return rows


def _streak(rows, key, positive=True):
    count = 0
    for row in reversed(rows):
        v = _num(row.get(key), 0)
        if positive and v > 0:
            count += 1
        elif (not positive) and v < 0:
            count += 1
        else:
            break
    return count


def _sum(rows, key, days):
    return round(sum(_num(r.get(key), 0) for r in rows[-days:]), 2)


def _analyze_rows(rows):
    rows = sorted(rows or [], key=lambda x: str(x.get("date", "")))
    recent5 = rows[-5:]
    latest = rows[-1] if rows else {}

    foreign_5d_sum = _sum(rows, "foreign_buy", 5)
    foreign_10d_sum = _sum(rows, "foreign_buy", 10)
    trust_5d_sum = _sum(rows, "investment_trust_buy", 5)
    dealer_5d_sum = _sum(rows, "dealer_buy", 5)
    foreign_buy_days_5 = sum(1 for r in recent5 if _num(r.get("foreign_buy"), 0) > 0)
    foreign_sell_days_5 = sum(1 for r in recent5 if _num(r.get("foreign_buy"), 0) < 0)
    trust_buy_days_5 = sum(1 for r in recent5 if _num(r.get("investment_trust_buy"), 0) > 0)
    trust_sell_days_5 = sum(1 for r in recent5 if _num(r.get("investment_trust_buy"), 0) < 0)
    foreign_buy_streak = _streak(rows, "foreign_buy", True)
    foreign_sell_streak = _streak(rows, "foreign_buy", False)
    trust_buy_streak = _streak(rows, "investment_trust_buy", True)
    trust_sell_streak = _streak(rows, "investment_trust_buy", False)

    margin = _num(latest.get("margin_balance"), None)
    short = _num(latest.get("short_balance"), None)
    short_margin_ratio = round(short / margin * 100, 2) if margin and short is not None else None

    score = 50
    reasons = []

    if foreign_5d_sum > 0 and foreign_buy_days_5 >= 3:
        score += 18
        reasons.append(f"外資近5日買超{foreign_buy_days_5}天，合計{foreign_5d_sum:,.0f}。")
    elif foreign_5d_sum < 0 and foreign_sell_days_5 >= 3:
        score -= 18
        reasons.append(f"外資近5日賣超{foreign_sell_days_5}天，合計{foreign_5d_sum:,.0f}。")
    elif foreign_buy_days_5 == 1 and foreign_5d_sum > 0:
        reasons.append("外資僅單日買超，尚未形成連續偏多。")
    else:
        reasons.append("外資近5日方向尚未明確。")

    if foreign_10d_sum > 0:
        score += 8
        reasons.append(f"外資近10日合計買超{foreign_10d_sum:,.0f}。")
    elif foreign_10d_sum < 0:
        score -= 8
        reasons.append(f"外資近10日合計賣超{foreign_10d_sum:,.0f}。")

    if trust_5d_sum > 0 and trust_buy_days_5 >= 3:
        score += 20
        reasons.append(f"投信近5日買超{trust_buy_days_5}天，具中期支撐。")
    elif trust_5d_sum < 0 and trust_sell_days_5 >= 3:
        score -= 16
        reasons.append(f"投信近5日賣超{trust_sell_days_5}天，中期籌碼偏弱。")
    elif trust_buy_streak >= 2:
        score += 10
        reasons.append(f"投信連買{trust_buy_streak}天。")

    if dealer_5d_sum > 0:
        score += 5
        reasons.append("自營商近5日偏買。")
    elif dealer_5d_sum < 0:
        score -= 5
        reasons.append("自營商近5日偏賣。")

    if short_margin_ratio is not None and short_margin_ratio > 30:
        score += 6
        reasons.append(f"券資比{short_margin_ratio}%偏高，若股價轉強可能有軋空條件。")

    score = max(0, min(100, round(score, 0)))
    if score >= 65:
        status, level = "籌碼偏多", "bullish"
    elif score <= 40:
        status, level = "籌碼偏空", "bearish"
    else:
        status, level = "籌碼中性", "neutral"

    return {
        "score": score,
        "status": status,
        "level": level,
        "meaning": "籌碼狀態以近5日與近10日法人買賣超、連續買賣天數、信用交易狀態綜合判斷；單日買超不直接視為偏多。",
        "reasons": reasons,
        "metrics": {
            "foreign_5d_sum": foreign_5d_sum,
            "foreign_10d_sum": foreign_10d_sum,
            "foreign_buy_days_5": foreign_buy_days_5,
            "foreign_sell_days_5": foreign_sell_days_5,
            "foreign_buy_streak": foreign_buy_streak,
            "foreign_sell_streak": foreign_sell_streak,
            "investment_trust_5d_sum": trust_5d_sum,
            "investment_trust_buy_days_5": trust_buy_days_5,
            "investment_trust_sell_days_5": trust_sell_days_5,
            "investment_trust_buy_streak": trust_buy_streak,
            "investment_trust_sell_streak": trust_sell_streak,
            "dealer_5d_sum": dealer_5d_sum,
            "margin_balance": margin,
            "short_balance": short,
            "short_margin_ratio": short_margin_ratio,
        },
    }


def _read_chip_rows(db, code, limit=20):
    rows = []
    try:
        docs = db.collection("chip_daily").document(code).collection("data").order_by("date", direction="DESCENDING").limit(limit).stream()
        rows = sorted([d.to_dict() or {} for d in docs], key=lambda x: str(x.get("date", "")))
    except Exception:
        try:
            docs = db.collection("chip_daily").document(code).collection("data").stream()
            rows = sorted([d.to_dict() or {} for d in docs], key=lambda x: str(x.get("date", "")))[-limit:]
        except Exception:
            rows = []
    return rows


def read_chip_rows(db, code, limit=20):
    return _read_chip_rows(db, code, limit=limit)


def analyze_chip_rows(rows):
    return _analyze_rows(rows)


def _write_chip_rows(db, code, rows):
    latest = rows[-1] if rows else {}
    analysis = _analyze_rows(rows)
    parent = db.collection("chip_daily").document(code)
    parent.set({
        "stock_id": code,
        "latest": latest,
        "analysis": analysis,
        "updated_at": datetime.now().isoformat(),
    }, merge=True)
    for row in rows:
        date = str(row.get("date"))
        parent.collection("data").document(date).set(row, merge=True)
    db.collection("chip_analysis").document(code).set({
        "stock_id": code,
        "analysis": analysis,
        "latest": latest,
        "updated_at": datetime.now().isoformat(),
    }, merge=True)
    return analysis


def _universe(m, product_type="all", market="all", limit=5000):
    if hasattr(m, "product_universe"):
        return m.product_universe(product_type=product_type, market=market)[:limit]
    return []


def _install(app):
    global _INSTALLED
    if _INSTALLED:
        return

    # IMPORTANT: Static routes must be declared before /api/chip/{stock},
    # otherwise FastAPI will treat "backfill_all" as the stock parameter.
    @app.get("/api/chip/backfill_all")
    def chip_backfill_all(product_type: str = "all", market: str = "all", offset: int = 0, limit: int = 20, days: int = 20):
        m = _main()
        if m.db is None:
            return _json({"status": "failed", "message": "Firebase not initialized", "next_offset": offset})
        products = _universe(m, product_type=product_type, market=market, limit=5000)
        requested_limit = max(1, int(limit or 1))
        effective_limit = min(requested_limit, 5)
        batch = products[offset:offset + effective_limit]
        written = 0
        errors = []
        for item in batch:
            code = str(item.get("code") or "").strip().upper()
            if not code:
                continue
            try:
                rows = _mock_chip_rows(code, days=days)
                _write_chip_rows(m.db, code, rows)
                written += 1
            except Exception as exc:
                errors.append({"code": code, "error": str(exc)})
        next_offset = offset + len(batch) if offset + len(batch) < len(products) else None
        return _json({
            "status": "ok",
            "route": "/api/chip/backfill_all",
            "collection": "chip_daily",
            "analysis_collection": "chip_analysis",
            "universe_count": len(products),
            "offset": offset,
            "limit": effective_limit,
            "requested_limit": requested_limit,
            "message": "Single request is capped at 5 stocks to avoid long browser requests and Firestore quota errors.",
            "processed": len(batch),
            "written_stocks": written,
            "error_count": len(errors),
            "errors": errors[:10],
            "next_offset": next_offset,
        })

    @app.get("/api/chip/backfill_history_all")
    def chip_backfill_history_all(months: int = 12, max_days: int = None):
        m = _main()
        if not hasattr(m, "run_chip_history_backfill"):
            return _json({"status": "failed", "message": "chip history backfill is not available"})
        days = int(max_days or max(20, months * 22))
        if hasattr(m, "start_thread"):
            return _json({
                **m.start_thread(f"chip-history-{days}d", m.run_chip_history_backfill, months, days),
                "route": "/api/chip/backfill_history_all",
                "months": months,
                "target_days": days,
            })
        threading.Thread(target=m.run_chip_history_backfill, args=(months, days), daemon=True).start()
        return _json({"status": "started", "route": "/api/chip/backfill_history_all", "months": months, "target_days": days})

    @app.get("/api/chip/init/{stock}")
    def chip_init(stock: str, days: int = 20):
        m = _main()
        code = m.normalize_stock(stock) if hasattr(m, "normalize_stock") else str(stock).strip().upper()
        if m.db is None:
            return _json({"status": "failed", "message": "Firebase not initialized"})
        rows = _mock_chip_rows(code, days=days)
        analysis = _write_chip_rows(m.db, code, rows)
        return _json({"status": "ok", "stock": stock, "normalized_stock": code, "written_days": len(rows), "analysis": analysis, "collection": "chip_daily"})

    @app.get("/api/chip/{stock}")
    def chip_analysis(stock: str, auto_init: bool = True):
        m = _main()
        code = m.normalize_stock(stock) if hasattr(m, "normalize_stock") else str(stock).strip().upper()
        cache_key = f"chip:{code}:{auto_init}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return _json({**cached, "cache_hit": True})
        if m.db is None:
            return _json({"status": "failed", "message": "Firebase not initialized"})
        rows = _read_chip_rows(m.db, code, limit=20)
        live_backfill = None
        if not _has_real_institutional_rows(rows):
            live_backfill = _try_live_chip_backfill(code)
            rows = _read_chip_rows(m.db, code, limit=20)
        if not rows and auto_init:
            rows = _mock_chip_rows(code, days=20)
            _write_chip_rows(m.db, code, rows)
        analysis = _analyze_rows(rows)
        latest = rows[-1] if rows else {}
        has_institutional_data = _has_real_institutional_rows(rows)
        payload = {
            "status": "ok",
            "route": "/api/chip/{stock}",
            "stock": stock,
            "normalized_stock": code,
            "source": "Firebase chip_daily",
            "latest_chip": latest,
            "rows": rows,
            "row_count": len(rows),
            "has_institutional_data": has_institutional_data,
            "live_backfill": live_backfill,
            "analysis": analysis,
            "updated_at": datetime.now().isoformat(),
        }
        if has_institutional_data:
            _cache_set(cache_key, payload)
        return _json(payload)

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
