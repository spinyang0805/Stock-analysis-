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


def build(out, state):
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
        add(s["stock_id"], "trailing_stop", "below", lv.get("trailing_stop"), f"{s['name']} 持股移動停損", 1)
        add(s["stock_id"], "ma20_break", "below", lv.get("ma20_break"), f"{s['name']} 跌破月線", 2)
    return list(rows.values())


def write(out, state, conn=None):
    for_date = next_trading_day(out["as_of"])
    rows = build(out, state)
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
        conn.commit()
    finally:
        if own:
            conn.close()
    print(f"[alerts] {len(rows)} levels for {for_date} "
          f"({sum(1 for r in rows if r[5] == 1)} priority-1)")
    return len(rows)


if __name__ == "__main__":
    import json
    d = os.path.join(os.path.dirname(BACKEND), "history", "out", "daily")
    meta = json.load(open(os.path.join(d, "latest.json"), encoding="utf-8"))
    rec = json.load(open(os.path.join(d, f"recommendations_{meta['as_of']}.json"), encoding="utf-8"))
    write(rec, track.load_state(os.path.join(d, "tracked_picks.json")))
