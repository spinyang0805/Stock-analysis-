"""etf_watch.py — morning-report 「ETF 持有觀察」 for 009816 / 00919 (+ 0050 as reference).

Public part (no amounts): close, day change, drawdown from the all-time high and the 52-week high (adjusted for official ex-dividend
results from TWSE TWT49U and for splits), next ex-dividend estimate (last ex-div date +
median interval of the last 4), advice line, drawdown alert level (−10 / −20 / −30%).
Private part: payment-account months left, from a private plan config (private_store key 'etf_plan', JSON
{"start_balance": 200000, "from": "2026-11", "pay": 19220, "day": 1}) — never written to the repo.

  python backend/tuning/etf_watch.py --selftest
  python backend/tuning/etf_watch.py            # print the section from local history/ parquet
"""
import json
import os
import sys
from datetime import date, datetime

WATCH = [("009816", "凱基台灣TOP50", True), ("00919", "群益台灣精選高息", True), ("0050", "元大台灣50", False)]
LEVELS = (-0.10, -0.20, -0.30)
WINDOW_MONTHS = 26                   # prices and ex-dividend events cover the SAME window (else the adjustment is partial)
DIV_MIN = 0.002                      # a reference-price gap below 0.2% is rounding, not a dividend


def adjust_series(rows, exdiv):
    """rows: [(date 'YYYYMMDD', close)] ascending; exdiv: {date: (before, after)} official 前收盤價 / 參考價.
    → (dates, raw close, adjusted close, ex-div events). Back-adjusted so the last adjusted close equals the raw one."""
    dates = [r[0] for r in rows]
    raw = [float(r[1]) for r in rows]
    fac, events, f = [1.0] * len(rows), [], 1.0
    for i in range(len(rows) - 1, -1, -1):
        fac[i] = f
        if dates[i] in exdiv:
            before, after = exdiv[dates[i]]
            if before > 0 and 0 < after < before:
                events.append((dates[i], round(before - after, 4)))
                f *= after / before
        elif i > 0 and raw[i] > 0:                      # split (e.g. 0050 1:4 in 2025): prev/close near an integer ≥ 2
            rr = raw[i - 1] / raw[i]
            if rr >= 1.8 and abs(rr - round(rr)) < 0.08 * round(rr):
                f /= round(rr)
    return dates, raw, [r * k for r, k in zip(raw, fac)], sorted(events)


def next_ex_div(events, today):
    """Estimate from the last ex-div date + median gap of the last 4 gaps; None when < 2 events."""
    if len(events) < 2:
        return None
    ds = [datetime.strptime(d, "%Y%m%d").date() for d, _ in events[-5:]]
    gaps = sorted((b - a).days for a, b in zip(ds, ds[1:]))
    gap = gaps[len(gaps) // 2]
    nxt = ds[-1]
    from datetime import timedelta
    while nxt <= today:
        nxt += timedelta(days=gap)
    return nxt


def level_of(dd):
    n = 0
    for i, t in enumerate(LEVELS, 1):
        if dd <= t:
            n = i
    return n


def analyse(code, name, rows, today, exdiv=None):
    stamp = today.strftime("%Y%m%d")
    rows = [r for r in rows if r[0] <= stamp]
    exdiv = {d: v for d, v in (exdiv or {}).items() if d <= stamp}
    dates, raw, adj, events = adjust_series(rows, exdiv)
    if len(adj) < 30:
        return None
    last = adj[-1]
    ath = max(adj)
    n52 = adj[-250:]
    dd_ath, dd_52 = last / ath - 1, last / max(n52) - 1
    day = adj[-1] / adj[-2] - 1 if adj[-2] else 0.0         # adjusted: an ex-div day is not a loss
    return {"code": code, "name": name, "date": dates[-1], "close": raw[-1], "day": day, "dd_ath": dd_ath,
            "dd_52w": dd_52, "since": dates[0], "level": level_of(dd_ath), "ex_div": next_ex_div(events, today),
            "last_ex": events[-1] if events else None}


def advice(a):
    if a["level"] >= 3:
        return "距高點已跌逾 30%：不賣，檢視是否有可加碼的閒錢，別動用扣款戶"
    if a["level"] == 2:
        return "距高點跌逾 20%：續抱，閒置資金可分批加碼，不要追殺"
    if a["level"] == 1:
        return "距高點跌逾 10%：屬正常回檔，續抱不動"
    if a["day"] <= -0.03:
        return "單日大跌：先確認是大盤還是個別因素，不因單日波動賣出"
    return "續抱，無動作"


def payment_months_left(plan, today):
    """Months of payments the payment account still covers. Deductions on `day` of each month from `from` (YYYY-MM),
    and never after `until` (YYYY-MM, last loan payment) when given. → (months left, balance now, deductions so far)."""
    if not plan:
        return None
    y, m = (int(x) for x in plan["from"].split("-"))
    end = tuple(int(x) for x in plan["until"].split("-")) if plan.get("until") else None
    day = int(plan.get("day", 1))
    k, cur = 0, date(y, m, min(day, 28))
    while cur <= today and (end is None or (y, m) <= end):
        k += 1
        m += 1
        if m > 12:
            y, m = y + 1, 1
        cur = date(y, m, min(day, 28))
    bal = plan["start_balance"] - k * plan["pay"]
    left = max(0, int(bal // plan["pay"]))
    if end is not None:                               # cannot need more months than payments remain
        left = min(left, max(0, (end[0] - y) * 12 + end[1] - m + 1))
    return left, bal, k


def _add_months(y, m, k):
    t = y * 12 + (m - 1) + k
    return t // 12, t % 12 + 1


def plan_from_tracker(data, updated):
    """loan_tracker row (site 借貸試算 → 目前狀況) → plan for payment_months_left: the 扣款戶 balance the user saved on
    `updated` (date; a zero balance is valid), monthly payment from the loan terms, deducted on the first payment's
    day of month, only within the first..last payment months of the loan."""
    acc = {a.get("key"): a for a in (data.get("accounts") or []) if isinstance(a, dict)}
    if "deduct" not in acc:
        return None
    bal = max(0.0, float(acc["deduct"].get("amount") or 0))
    loan, rate, n = float(data.get("loan") or 0), float(data.get("rate") or 0), int(data.get("months") or 0)
    fp = str(data.get("firstPayment") or "")
    if loan <= 0 or n <= 0 or len(fp) != 10:
        return None
    r = rate / 100 / 12
    pay = loan / n if r == 0 else loan * r / (1 - (1 + r) ** -n)
    fy, fm, day = int(fp[:4]), int(fp[5:7]), int(fp[8:10])
    y, m = updated.year, updated.month
    if updated.day >= day:                            # this month's deduction is already in the saved balance
        y, m = _add_months(y, m, 1)
    if (y, m) < (fy, fm):
        y, m = fy, fm
    ly, lm = _add_months(fy, fm, n - 1)
    return {"start_balance": bal, "from": f"{y:04d}-{m:02d}", "until": f"{ly:04d}-{lm:02d}", "pay": round(pay), "day": day}


def render(items, plan_info=None, private=False):
    if not items:
        return ""
    L = ["", "### ETF 持有觀察", ""]
    for a in items:
        ex = f"｜預估下次除息 {a['ex_div']:%m/%d}" if a["ex_div"] else ""
        L.append(f"- {a['name']}（{a['code']}）收 {a['close']:.2f}（{a['day']:+.2%}）｜距近 2 年高 {a['dd_ath']:+.1%}、"
                 f"距 52 週高 {a['dd_52w']:+.1%}{ex}")
        if a["code"] != "0050":
            L.append(f"  → {advice(a)}")
    L.append(f"（高點以還原除息價計，只看近 2 年多，資料自 {min(a['since'] for a in items)} 起）")
    if private and plan_info:
        left, bal, k = plan_info
        L.append(f"扣款戶：已扣 {k} 期，約剩 {bal:,.0f} 元，還可扣 {left} 個月" + ("（⚠ 不足 6 個月，注意補錢）" if left < 6 else ""))
    return "\n".join(L) + "\n"


def line_alerts(items, today):
    """LINE lines: drawdown level crossed (today), day drop ≤ −5%, ex-div within 3 days. Counts toward the LINE quota."""
    out = []
    for a in items:
        if a["code"] == "0050":
            continue
        if a["level"] >= 1:
            out.append(f"{a['name']} 距高點 {a['dd_ath']:+.0%}（跌破 −{a['level'] * 10}% 線）")
        if a["day"] <= -0.05:
            out.append(f"{a['name']} 單日 {a['day']:+.1%}")
        if a["ex_div"] and 0 <= (a["ex_div"] - today).days <= 3:
            out.append(f"{a['name']} 預估 {a['ex_div']:%m/%d} 除息")
    return out


def load_rows(codes, start):
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from tuning import data
    df = data.load_prices("parquet", start, extra_codes=codes)
    out = {}
    for c in codes:
        g = df[df["stock_id"] == c].sort_values("date")
        out[c] = [(r.date, r.close) for r in g.itertuples() if r.close == r.close]
    return out


def load_exdiv(codes, months=WINDOW_MONTHS):
    """Official TWSE 除權除息結果 for the last `months` months → {code: {date: (前收盤價, 參考價)}}. Network (cached)."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    import pandas as pd
    from tuning import adjust
    fx = adjust.ExchangeFetcher(budget=WINDOW_MONTHS + 5, interval=1.5)
    out = {c: {} for c in codes}
    cur = pd.Timestamp.today().to_period("M")
    for k in range(months):
        data, _ = fx._month("twse", str(cur - k).replace("-", ""))
        for r in data:
            if r["stock_id"] in out:
                out[r["stock_id"]][r["date"]] = (r["before"], r["after"])
    return out


def build(today=None, plan=None):
    today = today or date.today()
    codes = [c for c, _, _ in WATCH]
    import pandas as pd
    start = (pd.Timestamp(today).to_period('M') - (WINDOW_MONTHS - 1)).start_time.strftime('%Y%m%d')
    rows = load_rows(codes, start)
    try:
        ex = load_exdiv(codes)
    except Exception as exc:                          # without ex-dividend data the drawdown would be wrong → skip the section
        raise RuntimeError(f"ex-dividend data unavailable: {type(exc).__name__}: {str(exc)[:120]}")
    items = [a for c, n, _ in WATCH if (a := analyse(c, n, rows.get(c, []), today, ex.get(c)))]
    return items, payment_months_left(plan, today)


def selftest():
    rows = [(f"202601{i:02d}", 100.0) for i in range(1, 29)]
    rows += [("20260201", 99.0)] + [(f"202602{i:02d}", 99.0) for i in range(2, 10)]
    exd = {"20260201": (100.0, 99.0)}
    dates, raw, adj, ev = adjust_series(rows, exd)
    assert ev == [("20260201", 1.0)], ev
    assert abs(adj[-1] - 99.0) < 1e-9 and abs(adj[0] - 99.0) < 1e-9, (adj[0], adj[-1])   # flat after adjusting
    a = analyse("X", "測試", rows, date(2026, 2, 10), exd)
    assert a and abs(a["dd_ath"]) < 1e-9 and a["level"] == 0
    sp = [("20260101", 400.0), ("20260102", 400.0), ("20260103", 100.0), ("20260104", 100.0)]
    assert abs(adjust_series(sp, {})[2][0] - 100.0) < 1e-9, "1:4 split must be adjusted"
    assert level_of(-0.09) == 0 and level_of(-0.10) == 1 and level_of(-0.25) == 2 and level_of(-0.31) == 3
    p = {"start_balance": 200000, "from": "2026-11", "pay": 19220, "day": 1}
    assert payment_months_left(p, date(2026, 10, 20))[:2] == (10, 200000)
    left, bal, k = payment_months_left(p, date(2026, 11, 2))
    assert (k, bal, left) == (1, 200000 - 19220, 9), (k, bal, left)
    assert payment_months_left(None, date(2026, 11, 2)) is None
    tr = {"loan": 2000000, "rate": 2.9, "months": 120, "firstPayment": "2026-05-10",
          "accounts": [{"key": "deduct", "amount": 100000}]}
    pl = plan_from_tracker(tr, date(2026, 10, 20))
    assert pl == {"start_balance": 100000.0, "from": "2026-11", "until": "2036-04", "pay": 19220, "day": 10}, pl
    assert plan_from_tracker(tr, date(2026, 4, 1))["from"] == "2026-05", "no deduction before the first payment"
    z = plan_from_tracker({**tr, "accounts": [{"key": "deduct", "amount": 0}]}, date(2026, 10, 20))
    assert z and payment_months_left(z, date(2026, 10, 21))[0] == 0, "zero balance is a valid (empty) account"
    late = plan_from_tracker(tr, date(2036, 3, 20))
    assert payment_months_left(late, date(2037, 1, 1))[2] == 1, "no deductions after the last payment (2036-04)"
    assert plan_from_tracker(tr, date(2026, 10, 5))["from"] == "2026-10"
    assert payment_months_left(pl, date(2026, 11, 10))[:3] == (4, 100000 - 19220, 1)
    print("etf_watch selftest OK")
    return 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    its, pm = build()
    print(render(its, pm, private=True))
    print(line_alerts(its, date.today()))
