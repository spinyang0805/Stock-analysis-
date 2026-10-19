"""alert_levels.py — build the next trading day's intraday alert levels and write them to Supabase
(public.alert_levels, migration 006). Read by the pg_cron job intraday_alerts() every minute.

Levels (all on today's raw price scale, i.e. tradable tomorrow):
  tracked picks (open / exit_signalled): stop (below), take-profit (above)
      priority 1 when the user holds the stock, else 2
  holdings: trailing stop (below, priority 1), close below tomorrow's MA20 (below, priority 2)
The day's rows are replaced atomically (DELETE for_date + INSERT in one transaction).
"""
import os
import sys
from datetime import datetime, timedelta

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)
import trading_calendar as cal  # noqa: E402

from tuning import track  # noqa: E402


def next_trading_day(as_of):
    d = datetime.strptime(as_of, "%Y%m%d")
    for _ in range(20):
        d += timedelta(days=1)
        s = d.strftime("%Y%m%d")
        if cal.is_trading_day(s):
            return s
    return None


def build(out, state, style=None):
    rows = {}

    def add(code, kind, direction, price, label, priority):
        if price is None or not price > 0:
            return
        key = (code, kind)
        if key not in rows or priority < rows[key][5]:
            rows[key] = (code, kind, direction, float(price), label, priority)

    for p in state:
        if p["status"] not in ("open", "exit_signalled"):
            continue
        pri = 1 if p.get("user_holds") else 2
        add(p["stock_id"], "stop", "below", p.get("stop_price"), f"{p['name']} 追蹤停損", pri)
        add(p["stock_id"], "take_profit", "above", p.get("tp_price"), f"{p['name']} 達目標價（參考）", 2)
    for s in out.get("sell_alerts", []):
        lv = out.get("intraday_levels", {}).get(s["stock_id"], {})
        if not long_term(s["stock_id"], style):
            add(s["stock_id"], "trailing_stop", "below", lv.get("trailing_stop"), f"{s['name']} 持股移動停損", 1)
        add(s["stock_id"], "ma20_break", "below", lv.get("ma20_break"), f"{s['name']} 跌破月線", 2)
    return list(rows.values())


def long_term(code, style):
    """存股 holdings (style set, or ETF without a style — same default as positions.advise) get no trailing-stop
    alert: the holdings advice already says they are not sold on short-term signals."""
    s = (style or {}).get(code)
    return s == "存股" or (s is None and str(code).startswith("00"))


# evening actions that mean "keep holding / average down" → the intraday alerts must not say "reduce / stop out"
HOLD_LONG_ACTIONS = {"分批攤平（長線）", "長抱等待（基本面佳）", "分批加碼", "續抱（暫緩加碼）", "不攤平，續抱觀察"}


def build_context(out, state, top=10, style=None, advice=None, extra_picks=None):
    """Rows for public.intraday_context (migration 009): every stock the event-based intraday alerts watch.
    Role priority: holding > tracked > pick (one row per code).
    advice = {code: evening holdings advice} (positions.advise): the holding's stop is the advice's own stop
    (None when the advice is to keep holding), long-term / averaging-down holdings get dip alerts instead of
    sell alerts, and adding is only suggested when the advice allows it. Without advice → trailing stop."""
    lv = dict(out.get("intraday_levels") or {})
    for x in extra_picks or []:                     # 組合拳 watchlist: carries its own context values
        lv.setdefault(x["code"], {"ctx": x["ctx"]})
    rows = {}

    def add(code, name, role, stop, target, lt=False, allow=False, stop_label=None):
        c = (lv.get(code) or {}).get("ctx") or {}
        if code in rows or not c.get("prev_close"):
            return
        rows[code] = {"code": code, "name": name, "role": role, "etf": str(code).startswith("00"),
                      "prev_close": c["prev_close"], "ma20": c.get("ma20"), "ma60": c.get("ma60"),
                      "ma20_up": c.get("ma20_up"), "atr": c.get("atr"), "avg_vol20": c.get("avg_vol20"),
                      "hh20": c.get("hh20"), "ll20": c.get("ll20"),
                      "stop": stop if stop and stop > 0 else None, "target": target if target and target > 0 else None,
                      "long_term": bool(lt), "allow_add": bool(allow), "stop_label": stop_label}

    for s in out.get("sell_alerts", []):
        sid = s["stock_id"]
        a = (advice or {}).get(sid)
        lt = long_term(sid, style) or bool(a and a.get("action") in HOLD_LONG_ACTIONS)
        if a is not None:
            stop = None if lt else a.get("stop")
            allow = a.get("allow_add", False)
        else:
            stop, allow = (None if lt else s.get("trailing_stop")), False
        # in profit the stop protects the gain (停利, act on the close); at a loss it is a real 停損
        label = ("停利" if (a.get("pnl") or 0) >= 0 else "停損") if a is not None else None
        add(sid, s.get("name"), "holding", stop, None, lt, allow, label)
    for p in state:
        if p["status"] in ("open", "exit_signalled", "waiting"):
            add(p["stock_id"], p.get("name"), "tracked", p.get("stop_price"), p.get("tp_price"))
    for b in out.get("buy", [])[:top]:
        add(b["stock_id"], b.get("name"), "pick", b.get("stop_loss_if_fill_at_ref"), b.get("take_profit_if_fill_at_ref"))
    for x in extra_picks or []:
        add(x["code"], x.get("name"), "pick", x.get("stop"), x.get("target"))
    return list(rows.values())


CTX_COLS = ["code", "name", "role", "etf", "prev_close", "ma20", "ma60", "ma20_up", "atr", "avg_vol20",
            "hh20", "ll20", "stop", "target", "long_term", "allow_add", "stop_label"]


def write_context(cur, for_date, rows):
    cur.execute("SELECT to_regclass('public.intraday_context') IS NOT NULL")
    if not cur.fetchone()[0]:
        return None                       # migration 009 not applied yet
    cur.execute("DELETE FROM public.intraday_context WHERE for_date = %s", (for_date,))
    sql = (f"INSERT INTO public.intraday_context (for_date, {', '.join(CTX_COLS)}) "
           f"VALUES (%s, {', '.join(['%s'] * len(CTX_COLS))})")
    for r in rows:
        cur.execute(sql, [for_date] + [r[k] for k in CTX_COLS])
    return len(rows)


def write(out, state, conn=None, top=10, style=None, advice=None, extra_picks=None):
    for_date = next_trading_day(out["as_of"])
    rows = build(out, state, style)
    if not for_date:
        print("[alerts] no next trading day found; skipped")
        return 0
    own = conn is None
    if own:
        import psycopg2
        from export_static_json import load_env_file
        load_env_file()
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.alert_levels WHERE for_date = %s", (for_date,))
            for r in rows:
                cur.execute("INSERT INTO public.alert_levels (for_date, code, kind, direction, price, label, priority) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s)", (for_date,) + r)
            n_ctx = write_context(cur, for_date, build_context(out, state, top, style, advice, extra_picks))
        conn.commit()
    finally:
        if own:
            conn.close()
    print(f"[alerts] {len(rows)} levels for {for_date} "
          f"({sum(1 for r in rows if r[5] == 1)} priority-1); "
          f"context {n_ctx if n_ctx is not None else 'skipped (009 not applied)'}")
    return len(rows)


if __name__ == "__main__":
    import json
    d = os.path.join(os.path.dirname(BACKEND), "history", "out", "daily")
    meta = json.load(open(os.path.join(d, "latest.json"), encoding="utf-8"))
    rec = json.load(open(os.path.join(d, f"recommendations_{meta['as_of']}.json"), encoding="utf-8"))
    write(rec, track.load_state(os.path.join(d, "tracked_picks.json")))
