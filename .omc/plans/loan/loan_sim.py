"""Pre-registered loan-funded ETF simulation (see preregistration_loan.md). Monthly, 0050 TR as the stock leg."""
import importlib.util
import numpy as np, pandas as pd
spec = importlib.util.spec_from_file_location("r", ".omc/plans/debate/codex_repro_vt.py"); r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
_c = r.load_prices()["close"]
m = _c.resample("ME").last()
if _c.index[-1] < m.index[-1]:            # drop an unfinished last month
    m = m.iloc[:-1]
P, i_m, n = 2_000_000, 0.029 / 12, 120
PAY = P * i_m / (1 - (1 + i_m) ** -n)
FB, FS = 0.001425 * 0.6 + 0.001, 0.001425 * 0.6 + 0.001 + 0.001     # buy / sell cost fractions


def sim(px, cash_rate, w=None, rebalance=False, stock_amt=1_500_000, entry="A"):
    cash, units, high, ruin, low, short = float(P), 0.0, px[0], False, 1e18, 0.0
    target = stock_amt if w is None else P * w
    plan = {"A": [(0, 1.0)], "B": [(k, 1 / 6) for k in range(6)], "C": [(k, 1 / 12) for k in range(12)]}.get(entry)
    invested = 0.0
    for k in range(n + 1):
        if k:
            cash *= 1 + cash_rate / 12
        high = max(high, px[k])
        # entry tranches
        buy = 0.0
        if plan is not None:
            buy = sum(f for t, f in plan if t == k) * target
        else:   # staged D
            want = 1 / 3 + (1 / 3 if px[k] <= 0.90 * high else 0) + (1 / 3 if px[k] <= 0.80 * high else 0)
            if k >= 12:
                want = 1.0
            buy = max(0.0, want * target - invested)
        if buy > 0 and k <= 12:
            buy = min(buy, cash)
            units += buy * (1 - FB) / px[k]; cash -= buy; invested += buy
        if k:
            need = PAY
            use = min(cash, need); cash -= use; need -= use
            if need > 0:
                avail = units * px[k] * (1 - FS)          # never sell more than is held
                got = min(avail, need)
                units -= got / (px[k] * (1 - FS)); need -= got
                if need > 1e-6:
                    short += need                         # paid out of pocket
                    if k < n:
                        ruin = True
        if rebalance and k and k % 3 == 0 and units > 0:
            sv = units * px[k]; tot = sv + cash; gap = w * tot - sv
            if gap > 0:   # buy x so that post-cost stock = w × post-cost total
                x = min(gap / (1 - FB + w * FB), cash); units += x * (1 - FB) / px[k]; cash -= x
            else:         # sell x of stock value
                x = -gap / (1 - w * FS); units -= x / px[k]; cash += x * (1 - FS)
        low = min(low, units * px[k] + cash)
    return units * px[n] + cash - short, low, ruin, short


VARIANTS = {"U (20/30/150, no rebal)": dict(), "W40 quarterly": dict(w=.40, rebalance=True),
            "W50 quarterly": dict(w=.50, rebalance=True), "W60 quarterly": dict(w=.60, rebalance=True),
            "W75 quarterly": dict(w=.75, rebalance=True), "B 6-month DCA": dict(entry="B"),
            "C 12-month DCA": dict(entry="C"), "D staged on drawdown": dict(entry="D")}
starts = [i for i in range(len(m) - n) if m.index[i] >= pd.Timestamp("2004-01-01")]
for cr in (0.015, 0.0):
    print(f"\n## cash {cr:.1%}  ({len(starts)} windows {m.index[starts[0]]:%Y-%m}–{m.index[starts[-1]]:%Y-%m}), payment {PAY:,.0f}/mo\n")
    print("| variant | min | p10 | median | share<0 | share<500k | ruin | worst mid-way | worst start |\n|---|---|---|---|---|---|---|---|---|")
    for name, kw in VARIANTS.items():
        res = [sim(m.iloc[i:i + n + 1].to_numpy(), cr, **kw) for i in starts]
        e = np.array([x[0] for x in res]); lo = np.array([x[1] for x in res]); ru = sum(x[2] for x in res); sh = np.array([x[3] for x in res])
        print(f"| {name} | {e.min():,.0f} | {np.quantile(e, .1):,.0f} | {np.median(e):,.0f} | {(e < 0).mean():.1%} | "
              f"{(e < 5e5).mean():.1%} | {ru} | {sh.max():,.0f} | {m.index[starts[int(e.argmin())]]:%Y-%m} |")
