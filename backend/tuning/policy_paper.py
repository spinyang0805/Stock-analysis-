"""policy_paper.py — frozen forward PAPER test of 0050 / cash allocation rules (no real orders; public, no private data).

Real-money policy "C10" (user approved 2026-10-20): 50% 0050 / 50% TWD cash, rebalanced QUARTERLY (decided at the
quarter's last trading-day close, traded at the next session's open). This file tracks C10 plus alternatives on paper.
The rules below are FROZEN: never restart, never tune. Day 0 = 2026-10-20 close, first purchases at 2026-10-21 open.

Lines: C10 (quarterly 50/50), C1 (monthly 50/50), C8 (drawdown trigger), VT15 (weekly 15% vol target), BH (0050 buy&hold).
Costs as research_etf_long.py: fee 0.1425% x 0.6, ETF tax 0.1% on sells, 10 bp slippage per side; cash earns 0%.
0050 cash dividends are credited to the line's cash on the ex-date (derived from the exchange adjustment factors).

  python backend/tuning/policy_paper.py --source parquet --adjust-source exchange [--state public/data/policy_paper.json]
  python backend/tuning/policy_paper.py --selftest

Every run recomputes everything from day 0 (idempotent).
"""
import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta

import numpy as np

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATE = os.path.join(REPO, "public", "data", "policy_paper.json")
FEE = 0.001425 * 0.6
TAX_ETF = 0.001
SLIP = 0.001
BUY_M = (1 + SLIP) * (1 + FEE)                 # cash paid per 1 of notional bought
SELL_M = (1 - SLIP) * (1 - FEE - TAX_ETF)      # cash received per 1 of notional sold
START_DAY0 = "20261020"

RULES = {
    "day0": "2026-10-20", "first_trade": "2026-10-21 open", "nav_start": 1.0, "asset": "0050",
    "costs": {"commission": FEE, "etf_tax_sell": TAX_ETF, "slippage_per_side": SLIP, "cash_rate": 0.0},
    "dividends": "0050 cash dividends credited to the line's cash on the ex-date",
    "lines": {
        "C10": "50% 0050 / 50% cash; decided at each quarter's last trading-day close (Mar/Jun/Sep/Dec), traded next open",
        "C1": "50% 0050 / 50% cash; decided at each month's last trading-day close, traded next open",
        "C8": "H = highest 0050 total-return close since day 0; invested 100%: close <= 0.90*H -> next open 40% 0050 / 60% cash; "
              "reduced: close >= 0.95*H -> next open 100% 0050; starts 100% invested",
        "VT15": "weekly (last session of the week) E = min(1, 0.15 / sigma), sigma = 60-day stdev of daily 0050 total returns "
                "x sqrt(252) (pre-day-0 history as warm-up), rounded to 1%; traded next open",
        "BH": "0050 buy and hold (reference)",
    },
}
LINES = list(RULES["lines"])


def _d(s):
    return datetime.strptime(s, "%Y%m%d").date()


def _iso(s):
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


def _weekday_next(d):
    x = d + timedelta(days=1)
    while x.weekday() >= 5:
        x += timedelta(days=1)
    return x


CAL_NEXT = _weekday_next     # next session after the NEWEST bar; main() swaps in the exchange calendar (may give None)


def next_session(dts, i):
    """Next session after bar i: the next bar for history; for the newest bar the calendar, or None when it
    cannot be determined (then no decision is made this run - the next run decides it from the next bar)."""
    return dts[i + 1] if i + 1 < len(dts) else CAL_NEXT(dts[i])


def is_period_end(dts, i, months):
    d, nx = dts[i], next_session(dts, i)
    return nx is not None and d.month in months and nx.month != d.month


def is_week_end(dts, i):
    d, nx = dts[i], next_session(dts, i)
    return nx is not None and nx.isocalendar()[:2] != d.isocalendar()[:2]


def build_corporate(dts, c, af, typed, i0):
    """Typed per-bar cash dividends and share multipliers from adj_factor changes. An adj_factor change with no
    matching typed event stops the run (SystemExit): nothing is published on a guess."""
    div, split, actions = np.zeros(len(dts)), np.ones(len(dts)), []
    bars = [x.strftime("%Y%m%d") for x in dts]
    by_bar = {}
    for e in typed:                      # same rule as adjust_stock: first bar ON OR AFTER the event date
        pos = int(np.searchsorted(bars, e["date"], side="left"))
        if 0 < pos < len(bars):
            by_bar.setdefault(pos, []).append(e)
    for i in range(1, len(dts)):
        f = af[i - 1] / af[i]
        if abs(f - 1) < 1e-4:
            continue
        evs = by_bar.get(i, [])
        if not evs or abs(np.prod([e["factor"] for e in evs]) / f - 1) > 1e-3:
            sys.exit(f"[policy_paper] untyped adjustment factor {f:.6f} on {dts[i]}; JSON left untouched")
        for e in evs:
            if e["kind"] == "dividend":
                div[i] += c[i - 1] * (1 - e["factor"])
            else:
                split[i] *= e["share_ratio"]
            if i > i0:
                actions.append({"date": dts[i].isoformat(), "kind": e["kind"], "factor": round(e["factor"], 6)})
    return div, split, actions


def guard_rollback(path, new_updated):
    """Refuse to replace a published file with an older or differently-started one."""
    if not os.path.exists(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            old = json.load(f)
    except (OSError, ValueError):
        return
    if old.get("meta", {}).get("start") not in (None, RULES["day0"]):
        sys.exit("[policy_paper] existing JSON has a different day 0; refusing to overwrite")
    if old.get("updated") and old["updated"] > new_updated:
        sys.exit(f"[policy_paper] new data ends {new_updated} < published {old['updated']}; refusing to roll back")


def vol_target(a, i):
    """E = min(1, 0.15 / annualised 60-day stdev of daily total returns at close i), rounded to 1%."""
    if i < 60:
        return 1.0
    r = a[i - 59:i + 1] / a[i - 60:i] - 1
    s = float(np.std(r, ddof=1)) * np.sqrt(252)
    return 1.0 if s <= 0 else round(min(1.0, 0.15 / s), 2)


def simulate(rule, dts, o, c, a, div, i0, split=None, fill=None):
    """dts: dates; o/c: raw open/close; a: total-return close (adjusted); div: cash dividend per share on the ex-date;
    i0: index of day 0. Returns dict(nav=[...] from i0, weight, pending, shares, cash)."""
    cash, sh, pending, state = 1.0, 0.0, None, "in"
    log = []
    hi = a[i0]
    pending = vol_target(a, i0) if rule == "VT15" else (0.5 if rule in ("C10", "C1") else 1.0)
    pdate = dts[i0]                  # close at which the pending order was decided
    nav = []
    for i in range(i0, len(dts)):
        if i > i0:
            if split is not None and split[i] != 1.0:
                sh *= split[i]
            if div[i] > 0:
                cash += sh * div[i]
            if pending is not None and (fill is None or fill[i]):    # unfillable bar: defer to next fillable open
                v = cash + sh * o[i]
                cur = sh * o[i]
                gap = pending * v - cur
                if gap > 0:            # solve the size so the weight AFTER costs equals the target
                    x = min(gap / (1 + pending * (BUY_M - 1)), cash / BUY_M)
                    sh += x / o[i]
                    cash -= x * BUY_M
                elif gap < 0:
                    x = -gap / (1 - pending * (1 - SELL_M))
                    sh -= x / o[i]
                    cash += x * SELL_M
                log.append({"decided": pdate.isoformat(), "filled": dts[i].isoformat(),
                            "target_weight": round(float(pending), 4)})
                pending = None
        nav.append(cash + sh * c[i])
        new = None
        if rule == "C10" and is_period_end(dts, i, (3, 6, 9, 12)):
            new = 0.5
        elif rule == "C1" and is_period_end(dts, i, tuple(range(1, 13))):
            new = 0.5
        elif rule == "C8":
            hi = max(hi, a[i])
            if state == "in" and a[i] <= 0.90 * hi:
                state, new = "out", 0.4
            elif state == "out" and a[i] >= 0.95 * hi:
                state, new = "in", 1.0
        elif rule == "VT15" and is_week_end(dts, i):
            new = vol_target(a, i)
        if new is not None:
            pending, pdate = new, dts[i]
    return {"log": log, "nav": nav, "weight": sh * c[-1] / nav[-1], "pending": pending, "shares": sh, "cash": cash}


def stats(nav, dts, i0):
    arr = np.array(nav)
    peak = np.maximum.accumulate(arr)
    years = (dts[-1] - dts[i0]).days / 365.25
    cagr = float(arr[-1] ** (1 / years) - 1) if years >= 1 else None     # not annualised before one year
    return {"max_drawdown": round(float((arr / peak - 1).min()), 4), "cagr": None if cagr is None else round(cagr, 4),
            "total_return": round(float(arr[-1] - 1), 4)}


def next_decision(today):
    """Next C10 decision date: last trading day of the next Mar/Jun/Sep/Dec on/after today (exchange calendar if available)."""
    is_td = lambda d: d.weekday() < 5     # display only; confirm against the exchange calendar
    y, m = today.year, today.month
    for _ in range(14):
        if m in (3, 6, 9, 12):
            x = date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)
            while not is_td(x):
                x -= timedelta(days=1)
            if x >= today:
                return x.isoformat()
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return None


def compute(dts, o, c, a, div, i0, split=None, fill=None):
    """Whole JSON payload from aligned arrays (dts: list of date)."""
    lines = {}
    for rule in LINES:
        r = simulate(rule, dts, o, c, a, div, i0, split, fill)          # BH matches no decision rule: buy once, hold
        st = stats(r["nav"], dts, i0)
        lines[rule] = {"nav": [[dts[i0 + k].isoformat(), round(float(v), 5)] for k, v in enumerate(r["nav"])],
                       "weight": round(float(r["weight"]), 4), "decision_log": r["log"],
                       "pending_weight": None if r["pending"] is None else round(float(r["pending"]), 4), **st}
    return lines


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"])
    ap.add_argument("--state", default=STATE)
    ap.add_argument("--selftest", action="store_true", help="synthetic-price self-check, no data needed")
    from tuning.run_tuning import add_common_args
    add_common_args(ap)
    return ap.parse_args(argv)


def main(argv=None):
    if "--selftest" in (argv if argv is not None else sys.argv[1:]):
        return selftest()
    a_ = parse_args(argv)
    from tuning.data import log
    from tuning.run_tuning import prepare
    a_.no_holdings = False
    a_.holdings = "0050"
    sys.path.insert(0, os.path.join(REPO, "backend"))
    global CAL_NEXT
    import trading_calendar as tc

    def cal_next(d):
        x = d + timedelta(days=1)
        for _ in range(15):
            if not tc.covers(x.strftime("%Y%m%d")):
                return None                  # calendar does not cover that date: undetermined
            if tc.is_trading_day(x.strftime("%Y%m%d")):
                return x
            x += timedelta(days=1)
        return None
    CAL_NEXT = cal_next
    # fixed history start (60-day sigma warm-up before day 0), independent of today's date
    years = (date.today() - date(2026, 5, 1)).days / 365.25 + 0.02
    prep = prepare(a_, 0, years, codes=["0050"])
    g = prep["adjusted"].get("0050")
    if g is None or START_DAY0 not in set(g["date"]):
        sys.exit("[policy_paper] 0050 data or day 0 missing; existing JSON left untouched")
    g = g.sort_values("date").reset_index(drop=True)
    i0 = list(g["date"]).index(START_DAY0)
    if i0 < 60:
        sys.exit(f"[policy_paper] only {i0} sessions before day 0 (< 60 for sigma warm-up); JSON left untouched")
    dts = [_d(v) for v in g["date"]]
    o, c = g["open"].to_numpy(float), g["close"].to_numpy(float)
    a = g["aclose"].to_numpy(float)
    af = g["adj_factor"].to_numpy(float)
    fill = g["fill_ok"].to_numpy(bool) if "fill_ok" in g else np.ones(len(g), bool)
    from tuning import adjust as adj_mod
    fetcher = adj_mod.FinMindFetcher(budget=a_.finmind_budget, interval=a_.finmind_interval, offline=a_.finmind_offline)
    exchange = adj_mod.ExchangeFetcher(budget=a_.exchange_budget, interval=a_.exchange_interval)
    typed = adj_mod.typed_events("0050", fetcher, exchange, a_.adjust_source, g["date"].iloc[0], g["date"].iloc[-1])
    div, split, actions = build_corporate(dts, c, af, typed, i0)
    payload = {"meta": {"start": RULES["day0"], "rules": RULES, "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M")},
               "corporate_actions": actions}
    payload["lines"] = compute(dts, o, c, a, div, i0, split, fill)
    payload["updated"] = g["date"].iloc[-1]
    payload["sessions"] = len(dts) - 1 - i0
    payload["next_c10_decision"] = next_decision(dts[-1])
    guard_rollback(a_.state, payload["updated"])
    os.makedirs(os.path.dirname(a_.state), exist_ok=True)
    tmp = a_.state + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    os.replace(tmp, a_.state)
    log(f"[policy_paper] {payload['sessions']} sessions after day 0 -> {a_.state}")


def selftest():
    # synthetic weekday calendar 2027-01-01 .. 2028-06-30
    dts, d = [], date(2027, 1, 1)
    while d <= date(2028, 6, 30):
        if d.weekday() < 5:
            dts.append(d)
        d += timedelta(days=1)
    n = len(dts)
    # 1) quarterly vs monthly decision dates
    q = [dts[i].isoformat() for i in range(n) if is_period_end(dts, i, (3, 6, 9, 12))]
    m = [dts[i] for i in range(n) if is_period_end(dts, i, tuple(range(1, 13)))]
    assert q[:4] == ["2027-03-31", "2027-06-30", "2027-09-30", "2027-12-31"], q[:4]
    assert len(q) == 6 and len(m) == 18, (len(q), len(m))
    # 2) cost deduction: flat price, BH nav after the first buy = 1 / BUY_M exactly
    flat = np.full(n, 100.0)
    z = np.zeros(n)
    r = simulate("BH", dts, flat, flat, flat, z, 0)
    assert abs(r["nav"][0] - 1) < 1e-12 and abs(r["nav"][1] - 1 / BUY_M) < 1e-9, r["nav"][:2]
    w1 = simulate("C10", dts[:2], flat[:2], flat[:2], flat[:2], z[:2], 0)
    assert abs(w1["weight"] - 0.5) < 1e-9, w1["weight"]          # first buy lands exactly at 50% after costs
    half = flat.copy()
    half[20:] = 50.0
    sp = np.ones(n)
    sp[20] = 2.0
    rs = simulate("BH", dts, flat, half, flat, z, 0, sp)
    ns = simulate("BH", dts, flat, flat, flat, z, 0)
    assert abs(rs["nav"][-1] - ns["nav"][-1]) < 1e-9 and rs["cash"] < 1e-9, "split must not change value or create cash"
    r10 = simulate("C10", dts, flat, flat, flat, z, 0)       # 50/50: only half the money pays cost, later rebalances ~free
    assert 1 - r10["nav"][-1] > 0 and r10["nav"][-1] > r["nav"][-1], (r10["nav"][-1], r["nav"][-1])
    # dividend goes to cash on the ex-date
    dv = z.copy()
    dv[10] = 2.0
    rd = simulate("BH", dts, flat, flat, flat, dv, 0)
    assert abs(rd["cash"] - rd["shares"] * 2.0) < 1e-9
    # 3) C8 switches: 100 -> 85 (<= 0.90H) reduces to 40%; back to 96 (>= 0.95H) -> 100% again
    px = np.full(n, 100.0)
    px[30:60] = 85.0
    px[60:] = 96.0
    r8 = simulate("C8", dts, px, px, px, z, 0)
    # reconstruct weight just before day 60 (after reduce trade at the open of day 31)
    r8_mid = simulate("C8", dts[:59], px[:59], px[:59], px[:59], z[:59], 0)
    assert abs(r8_mid["weight"] - 0.4) < 0.01, r8_mid["weight"]
    assert r8["weight"] > 0.99, r8["weight"]
    # 4) VT15: calm market -> E = 1
    assert vol_target(flat, 100) == 1.0
    rng = np.random.default_rng(0)
    wild = 100 * np.cumprod(1 + rng.normal(0, 0.03, n))
    assert 0 < vol_target(wild, 200) < 0.6
    # 5) history uses the bars themselves: a holiday-shortened month still ends on its last BAR
    hol = [x for x in dts if x != date(2027, 3, 31)]           # pretend 03-31 was a holiday
    j = hol.index(date(2027, 3, 30))
    assert is_period_end(hol, j, (3, 6, 9, 12)), "last bar of the month is the period end"
    # 6) newest bar: calendar undetermined -> no decision; determined -> decision
    global CAL_NEXT
    keep, CAL_NEXT = CAL_NEXT, (lambda d: None)
    try:
        last = [date(2027, 12, 30), date(2027, 12, 31)]
        assert not is_period_end(last, 1, (3, 6, 9, 12)) and not is_week_end(last, 1)
    finally:
        CAL_NEXT = keep
    assert is_period_end(last, 1, (3, 6, 9, 12))
    # 7) typed corporate actions: dividend -> cash, split -> shares, untyped factor -> exit
    d5 = [date(2027, 1, 4) + timedelta(days=k) for k in range(5)]
    c5 = np.array([100.0, 100.0, 98.0, 50.0, 50.0])
    af5 = np.array([0.5 * 0.98, 0.5 * 0.98, 0.5, 1.0, 1.0])     # ex-div 2% at idx2, split 1:2 at idx3 (f=0.5)
    af5 = np.array([0.5 * 0.98, 0.5 * 0.98, 0.5, 1.0, 1.0])
    ty = [{"date": "20270106", "kind": "dividend", "factor": 0.98, "share_ratio": 1.0},
          {"date": "20270107", "kind": "split", "factor": 0.5, "share_ratio": 2.0}]
    dv5, sp5, _ = build_corporate(d5, c5, af5, ty, 0)
    assert abs(dv5[2] - 2.0) < 1e-9 and sp5[3] == 2.0 and dv5[3] == 0
    try:
        build_corporate(d5, c5, af5, ty[:1], 0)
        raise AssertionError("untyped factor must exit")
    except SystemExit:
        pass
    # 8) unfillable bar defers the order to the next fillable open
    fl = np.ones(n, bool)
    fl[1] = False
    rf = simulate("BH", dts, flat, flat, flat, z, 0, None, fl)
    assert rf["log"][0]["filled"] == dts[2].isoformat() and rf["log"][0]["decided"] == dts[0].isoformat(), rf["log"][0]
    # 7b) event dated on a non-bar day attaches to the first bar on/after it (as adjust_stock does)
    d6 = [date(2027, 1, 4), date(2027, 1, 6), date(2027, 1, 8)]            # 01-07 is not a bar
    _, sp6, _ = build_corporate(d6, np.array([100.0, 100.0, 50.0]), np.array([0.5, 0.5, 1.0]),
                                [{"date": "20270107", "kind": "split", "factor": 0.5, "share_ratio": 2.0}], 0)
    assert sp6[2] == 2.0 and sp6[1] == 1.0
    # 9) rollback guard
    import tempfile
    tf = os.path.join(tempfile.mkdtemp(), "x.json")
    json.dump({"meta": {"start": RULES["day0"]}, "updated": "20261105"}, open(tf, "w"))
    guard_rollback(tf, "20261105")
    try:
        guard_rollback(tf, "20261104")
        raise AssertionError("rollback must exit")
    except SystemExit:
        pass
    print("policy_paper selftest OK")


if __name__ == "__main__":
    if __package__ in (None, ""):
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        __package__ = "tuning"
    main()
