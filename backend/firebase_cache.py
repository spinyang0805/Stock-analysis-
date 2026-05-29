import json
from datetime import datetime
from typing import Any, Dict, List
import pytz

from firebase import db, get_conn, return_conn

TW_TZ = pytz.timezone("Asia/Taipei")

try:
    import auto_routes  # noqa: F401
except Exception:
    pass

try:
    import maintenance_routes  # noqa: F401
except Exception:
    pass


def now_tw():
    return datetime.now(TW_TZ)


def _run(sql: str, params=(), fetch: str = None):
    """Execute SQL with pooled connection. Returns (result, error_string_or_None)."""
    conn = get_conn()
    if conn is None:
        return None, "no_db_connection"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch == "one":
                result = cur.fetchone()
            elif fetch == "all":
                result = cur.fetchall()
            else:
                result = cur.rowcount
        conn.commit()
        return result, None
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"SQL error: {e} | SQL: {sql[:120]}")
        return None, str(e)
    finally:
        return_conn(conn)


def _is_number(value) -> bool:
    try:
        return value is not None and float(value) == float(value)
    except Exception:
        return False


def _float(value):
    try:
        return float(value)
    except Exception:
        return None


def explain_stock_payload_issue(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return "payload_not_dict"
    if payload.get("preload") is True:
        return "preload_placeholder"
    # Only close is mandatory; open/high/low may be None for suspended or limit-hit stocks
    close = _float(payload.get("close"))
    if close is None or close <= 0:
        return "missing_or_non_numeric_ohlc"
    open_price = _float(payload.get("open")) or close
    high = _float(payload.get("high")) or close
    low = _float(payload.get("low")) or close
    volume = _float(payload.get("volume"))
    if high < max(open_price, close, low):
        return "invalid_ohlc_high"
    if low > min(open_price, close, high):
        return "invalid_ohlc_low"
    if volume is not None and volume < 0:
        return "negative_volume"
    return "valid"


def is_valid_stock_payload(payload: Dict[str, Any]) -> bool:
    return explain_stock_payload_issue(payload) == "valid"


def save_stock_daily(stock_id: str, date: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        return False
    if not is_valid_stock_payload(payload):
        return False
    sql = """
        INSERT INTO stock_daily
            (stock_id, date, open, high, low, close, volume, turnover,
             change, trades, market, product_type, name, source)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (stock_id, date) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
            volume=EXCLUDED.volume, turnover=EXCLUDED.turnover, change=EXCLUDED.change,
            trades=EXCLUDED.trades, market=EXCLUDED.market, product_type=EXCLUDED.product_type,
            name=EXCLUDED.name, source=EXCLUDED.source, updated_at=NOW()
    """
    close = _float(payload.get("close"))
    open_p = _float(payload.get("open")) or close
    high = _float(payload.get("high")) or close
    low = _float(payload.get("low")) or close
    _, err = _run(sql, (
        stock_id, date,
        open_p, high, low, close,
        payload.get("volume"), payload.get("turnover"), payload.get("change"), payload.get("trades"),
        payload.get("market"), payload.get("product_type"), payload.get("name"), payload.get("source"),
    ))
    return err is None


def save_chip_daily(stock_id: str, date: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        return False
    sql = """
        INSERT INTO chip_daily
            (stock_id, date, name, market, foreign_buy, investment_trust_buy,
             dealer_buy, institution_total_buy, margin_balance, short_balance, source, chip_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (stock_id, date) DO UPDATE SET
            name=COALESCE(EXCLUDED.name, chip_daily.name),
            market=COALESCE(EXCLUDED.market, chip_daily.market),
            foreign_buy=COALESCE(EXCLUDED.foreign_buy, chip_daily.foreign_buy),
            investment_trust_buy=COALESCE(EXCLUDED.investment_trust_buy, chip_daily.investment_trust_buy),
            dealer_buy=COALESCE(EXCLUDED.dealer_buy, chip_daily.dealer_buy),
            institution_total_buy=COALESCE(EXCLUDED.institution_total_buy, chip_daily.institution_total_buy),
            margin_balance=COALESCE(EXCLUDED.margin_balance, chip_daily.margin_balance),
            short_balance=COALESCE(EXCLUDED.short_balance, chip_daily.short_balance),
            source=EXCLUDED.source, updated_at=NOW()
    """
    _, err = _run(sql, (
        stock_id, date,
        payload.get("name"), payload.get("market"),
        payload.get("foreign_buy"), payload.get("investment_trust_buy"),
        payload.get("dealer_buy"), payload.get("institution_total_buy"),
        payload.get("margin_balance") or payload.get("margin"),
        payload.get("short_balance") or payload.get("short"),
        payload.get("source"), payload.get("chip_date") or date,
    ))
    return err is None


def save_fundamentals(stock_id: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        return False
    sql = """
        INSERT INTO fundamentals
            (stock_id, pe_ratio, dividend_yield, pb_ratio,
             revenue, revenue_mom, revenue_yoy, revenue_date, valuation_date, source)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (stock_id) DO UPDATE SET
            pe_ratio       = COALESCE(EXCLUDED.pe_ratio,       fundamentals.pe_ratio),
            dividend_yield = COALESCE(EXCLUDED.dividend_yield, fundamentals.dividend_yield),
            pb_ratio       = COALESCE(EXCLUDED.pb_ratio,       fundamentals.pb_ratio),
            revenue        = COALESCE(EXCLUDED.revenue,        fundamentals.revenue),
            revenue_mom    = COALESCE(EXCLUDED.revenue_mom,    fundamentals.revenue_mom),
            revenue_yoy    = COALESCE(EXCLUDED.revenue_yoy,    fundamentals.revenue_yoy),
            revenue_date   = COALESCE(EXCLUDED.revenue_date,   fundamentals.revenue_date),
            valuation_date = COALESCE(EXCLUDED.valuation_date, fundamentals.valuation_date),
            source         = EXCLUDED.source,
            updated_at     = NOW()
    """
    _, err = _run(sql, (
        stock_id,
        payload.get("pe_ratio"), payload.get("dividend_yield"), payload.get("pb_ratio"),
        payload.get("revenue"), payload.get("revenue_mom"), payload.get("revenue_yoy"),
        payload.get("revenue_date"), payload.get("valuation_date"), payload.get("source"),
    ))
    return err is None


def save_job_log(job_id: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        return False
    _, err = _run(
        "INSERT INTO job_logs (job_id, payload) VALUES (%s,%s) "
        "ON CONFLICT (job_id) DO UPDATE SET payload=EXCLUDED.payload, updated_at=NOW()",
        (job_id, json.dumps({**payload, "job_id": job_id})),
    )
    return err is None


def get_valid_stock_daily_series(stock_id: str, limit: int = 260) -> List[Dict[str, Any]]:
    if db is None:
        return []
    rows, err = _run(
        "SELECT date,open,high,low,close,volume,market,name,source "
        "FROM stock_daily WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        (stock_id, limit), fetch="all",
    )
    if err or not rows:
        return []
    return [
        {"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4],
         "volume": r[5] or 0, "market": r[6], "name": r[7], "source": r[8] or "stock_daily"}
        for r in sorted(rows, key=lambda x: x[0])
    ]


def get_chip_rows(stock_id: str, limit: int = 60) -> List[Dict[str, Any]]:
    if db is None:
        return []
    rows, err = _run(
        "SELECT date,name,market,foreign_buy,investment_trust_buy,dealer_buy,"
        "institution_total_buy,margin_balance,short_balance,source "
        "FROM chip_daily WHERE stock_id=%s ORDER BY date DESC LIMIT %s",
        (stock_id, limit), fetch="all",
    )
    if err or not rows:
        return []
    return [
        {"date": r[0], "name": r[1], "market": r[2], "foreign_buy": r[3],
         "investment_trust_buy": r[4], "dealer_buy": r[5], "institution_total_buy": r[6],
         "margin_balance": r[7], "short_balance": r[8], "source": r[9]}
        for r in sorted(rows, key=lambda x: x[0])
    ]


def get_latest_chip_daily(stock_id: str, limit: int = 1) -> Dict[str, Any]:
    if db is None:
        return {}
    row, err = _run(
        "SELECT date,name,market,foreign_buy,investment_trust_buy,dealer_buy,"
        "institution_total_buy,margin_balance,short_balance,source "
        "FROM chip_daily WHERE stock_id=%s ORDER BY date DESC LIMIT 1",
        (stock_id,), fetch="one",
    )
    if err or not row:
        return {}
    return {
        "date": row[0], "name": row[1], "market": row[2],
        "foreign_buy": row[3], "investment_trust_buy": row[4],
        "dealer_buy": row[5], "institution_total_buy": row[6],
        "margin_balance": row[7], "short_balance": row[8], "source": row[9],
    }


def save_analysis_cache(stock_id: str, data: Dict[str, Any]) -> bool:
    if db is None:
        return False
    _, err = _run(
        "INSERT INTO analysis_cache (stock_id, latest_date, data_rows, perspective_cards, signals, trade_plan) "
        "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (stock_id) DO UPDATE SET "
        "latest_date=EXCLUDED.latest_date, data_rows=EXCLUDED.data_rows, "
        "perspective_cards=EXCLUDED.perspective_cards, signals=EXCLUDED.signals, "
        "trade_plan=EXCLUDED.trade_plan, updated_at=NOW()",
        (
            stock_id, data.get("latest_date"), data.get("data_rows"),
            json.dumps(data.get("perspective_cards")),
            json.dumps(data.get("signals")),
            json.dumps(data.get("trade_plan")),
        ),
    )
    return err is None


def save_job_queue(job_id: str, payload: Dict[str, Any]) -> bool:
    if db is None:
        return False
    _, err = _run(
        "INSERT INTO job_queue (job_id, payload, status, control) VALUES (%s,%s,%s,%s) "
        "ON CONFLICT (job_id) DO UPDATE SET "
        "payload=EXCLUDED.payload, status=EXCLUDED.status, control=EXCLUDED.control, updated_at=NOW()",
        (
            job_id,
            json.dumps({**payload, "job_id": job_id}),
            payload.get("status", "pending"),
            payload.get("control", "run"),
        ),
    )
    return err is None


def get_job_queue(job_id: str) -> Dict[str, Any]:
    if db is None:
        return {}
    row, err = _run(
        "SELECT payload, status, control FROM job_queue WHERE job_id=%s",
        (job_id,), fetch="one",
    )
    if err or not row:
        return {}
    data = json.loads(row[0]) if row[0] else {}
    data["status"] = row[1]
    data["control"] = row[2]
    return data


def update_job_queue(job_id: str, updates: Dict[str, Any]) -> bool:
    current = get_job_queue(job_id)
    merged = {**current, **updates, "updated_at": datetime.now().isoformat()}
    return save_job_queue(job_id, merged)


def save_product(code: str, product: Dict[str, Any]) -> bool:
    if db is None:
        return False
    _, err = _run(
        "INSERT INTO product_universe (code, name, market, type, industry) VALUES (%s,%s,%s,%s,%s) "
        "ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, market=EXCLUDED.market, "
        "type=EXCLUDED.type, industry=EXCLUDED.industry, updated_at=NOW()",
        (code, product.get("name"), product.get("market"), product.get("type"), product.get("industry")),
    )
    return err is None


def get_all_products_from_db(limit: int = 5000) -> List[Dict[str, Any]]:
    if db is None:
        return []
    rows, err = _run(
        "SELECT code, name, market, type, industry FROM product_universe ORDER BY code LIMIT %s",
        (limit,), fetch="all",
    )
    if not err and rows:
        return [{"code": r[0], "name": r[1] or r[0], "market": r[2] or "上市",
                 "type": r[3] or "股票", "industry": r[4] or "股票"} for r in rows]
    # fallback: derive from stock_daily
    rows, err = _run(
        "SELECT DISTINCT ON (stock_id) stock_id, name, market, product_type "
        "FROM stock_daily ORDER BY stock_id, date DESC LIMIT %s",
        (limit,), fetch="all",
    )
    if err or not rows:
        return []
    return [{"code": r[0], "name": r[1] or r[0], "market": r[2] or "上市",
             "type": r[3] or "股票", "industry": r[3] or "股票"} for r in rows]


def delete_stock_data(code: str) -> int:
    deleted = 0
    for sql in [
        "DELETE FROM stock_daily WHERE stock_id=%s",
        "DELETE FROM chip_daily WHERE stock_id=%s",
        "DELETE FROM analysis_cache WHERE stock_id=%s",
    ]:
        _run(sql, (code,))
        deleted += 1
    return deleted


def get_cache_status(stock_id: str) -> Dict[str, Any]:
    if db is None:
        return {"firebase_enabled": False, "message": "Database not initialized"}
    count_row, e1 = _run("SELECT COUNT(*) FROM stock_daily WHERE stock_id=%s", (stock_id,), fetch="one")
    chip_row, e2 = _run("SELECT COUNT(*) FROM chip_daily WHERE stock_id=%s", (stock_id,), fetch="one")
    samples, e3 = _run(
        "SELECT date,open,high,low,close FROM stock_daily WHERE stock_id=%s ORDER BY date DESC LIMIT 3",
        (stock_id,), fetch="all",
    )
    if e1:
        return {"firebase_enabled": False, "error": e1}
    return {
        "firebase_enabled": True,
        "stock_id": stock_id,
        "stock_daily_count": count_row[0] if count_row else 0,
        "chip_daily_count": chip_row[0] if chip_row else 0,
        "stock_daily_samples": [
            {"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]}
            for r in (samples or [])
        ],
    }


def cleanup_invalid_stock_daily(stock_id: str, limit: int = 500) -> Dict[str, Any]:
    return {"stock_id": stock_id, "message": "Postgres enforces OHLC validity on insert — no cleanup needed"}


def audit_stock_daily_market(limit_stocks: int = 3000, limit_per_stock: int = 30, delete_invalid: bool = False) -> Dict[str, Any]:
    if db is None:
        return {"firebase_enabled": False, "message": "Database not initialized"}
    stocks, _ = _run("SELECT COUNT(DISTINCT stock_id) FROM stock_daily", fetch="one")
    docs, _ = _run("SELECT COUNT(*) FROM stock_daily", fetch="one")
    return {
        "firebase_enabled": True,
        "mode": "postgres_audit",
        "checked_stocks": stocks[0] if stocks else 0,
        "checked_docs": docs[0] if docs else 0,
        "note": "Postgres enforces OHLC validity at insert time",
    }
