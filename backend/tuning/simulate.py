"""simulate.py — one position simulator shared by the live tracker (track.py) and the backtest of the
deployed exit policy (Phase B). Deterministic: the same bars always give the same outcome, so the tracker
re-simulates every open position from its entry each day (missed runs are caught up automatically).

Execution convention (identical everywhere):
  signal at close t → entry at the OPEN of t+1 (skipped if that open gapped up ≥ GAP_SKIP or has no valid fill)
  each session close is evaluated → an exit DECIDED at close i is executed at the OPEN of i+1
  (deferred to the next bar with a valid fill when i+1 has none, e.g. a suspension)
Exit rules, first hit on a close wins: stop / trailing stop, take-profit, the tuned sell signal, planned
holding sessions reached (held = closes since entry, entry day = 1 → time exit at the open of t+h+1).
All prices are ADJUSTED (aopen/aclose); the latest bar's adjusted price equals the raw price.
"""
import numpy as np

GAP_SKIP = 0.095


def simulate_position(g, f, sells, e, plan_hold, stop_pct, tp_pct=None, sell_rule=None, atr_mult=2.5,
                      gap_skip=GAP_SKIP):
    """g: adjusted bars of one stock (aopen/aclose, optional fill_ok); f: features(g); sells: sell_signals(f);
    e: index of the entry bar (first bar after the signal day). Returns a dict describing the outcome up to
    the last bar of g."""
    n = len(g)
    ao, ac = g["aopen"].to_numpy(float), g["aclose"].to_numpy(float)
    ok = g["fill_ok"].to_numpy(bool) if "fill_ok" in g else np.ones(n, bool)
    atr = f["atr"].to_numpy(float)
    rule = sells.get(sell_rule).to_numpy(bool) if sell_rule and sell_rule in sells else None
    out = {"status": "waiting", "entry_i": None, "exit_i": None, "decided_i": None, "reason": None,
           "held": 0, "stop": None, "tp": None, "ret": None, "pending": None}
    if e >= n:
        return out
    if not ok[e]:
        return {**out, "status": "skipped", "reason": "進場日無有效開盤價（停牌或無成交），未進場", "exit_i": e}
    if e > 0 and np.isfinite(ac[e - 1]) and ao[e] >= ac[e - 1] * (1 + gap_skip):
        return {**out, "status": "skipped", "reason": f"開盤漲幅 {ao[e] / ac[e - 1] - 1:.1%} ≥ {gap_skip:.1%}，不追",
                "exit_i": e}
    entry = ao[e]
    stop0 = entry * (1 + stop_pct)
    tp = entry * (1 + tp_pct) if tp_pct else None
    peak = -np.inf
    pending, decided = None, None
    stop_eff = stop0
    for i in range(e, n):
        if pending is not None and i > decided:
            if ok[i]:
                return {**out, "status": "closed", "entry_i": e, "exit_i": i, "decided_i": decided,
                        "reason": pending, "held": decided - e + 1, "stop": stop_eff, "tp": tp,
                        "ret": ao[i] / entry - 1}
            continue                                   # no valid open: exit deferred to the next tradable bar
        close = ac[i]
        if not np.isfinite(close):
            continue
        peak = max(peak, close)
        held = i - e + 1
        trail = peak - atr_mult * atr[i] if np.isfinite(atr[i]) else np.nan
        stop_eff = max(stop0, trail) if np.isfinite(trail) else stop0
        if close <= stop_eff:
            pending = ("停損" if stop_eff == stop0 else "移動停利／停損") + f"（收盤跌破 {stop_eff:.2f}）"
        elif tp is not None and close >= tp:
            pending = f"停利（收盤達 {tp:.2f}）"
        elif rule is not None and rule[i]:
            pending = "出場訊號"
        elif held >= plan_hold:
            pending = f"持有 {held} 日已達計畫 {plan_hold} 日"
        if pending is not None:
            decided = i
    last = n - 1
    held = (decided if decided is not None else last) - e + 1
    return {**out, "status": "open", "entry_i": e, "decided_i": decided, "pending": pending, "held": held,
            "stop": stop_eff, "tp": tp, "ret": ac[last] / entry - 1}
