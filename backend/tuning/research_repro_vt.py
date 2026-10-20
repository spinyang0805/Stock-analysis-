"""Independent 0050 reproduction; run from any directory (pandas + numpy).

Cached pre-2004 prices warm up volatility; signals begin on 2004-01-02.
Weekly/monthly decisions execute only at the following session's open.
Fractional adjusted units represent dividend reinvestment; cash yields zero.
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
START = pd.Timestamp("2004-01-02")
COMMISSION = 0.001425 * 0.6
BUY_COST = (1 + 0.001) * (1 + COMMISSION) - 1
SELL_COST = 1 - (1 - 0.001) * (1 - COMMISSION - 0.001)


def load_prices():
    cache = ROOT / "history" / "cache"
    p = pd.read_json(cache / "finmind_TaiwanStockPrice_0050.json")
    p["date"] = pd.to_datetime(p["date"])
    p = p.set_index("date").sort_index()[["open", "close"]].astype(float)
    if not p.index.is_unique or not np.isfinite(p.to_numpy()).all():
        raise ValueError("Duplicate dates or nonfinite prices")
    if (p <= 0).any().any() or START not in p.index:
        raise ValueError("Nonpositive prices or missing start session")
    if p.index[-1] < pd.Timestamp("2026-10-16"):
        raise ValueError("Price cache ends before 2026-10-16")

    d = pd.read_json(cache / "finmind_TaiwanStockDividend_0050.json")
    d["ex"] = pd.to_datetime(d["CashExDividendTradingDate"], errors="coerce")
    d["cash"] = pd.to_numeric(d["CashEarningsDistribution"], errors="raise")
    # Remove repeated event records, then sum distinct cash amounts per ex-date.
    div = d.dropna(subset=["ex"]).drop_duplicates(["ex", "cash"])
    div = div.groupby("ex")["cash"].sum()
    relevant = div[(div.index > p.index[0]) & (div.index <= p.index[-1])]
    if not relevant.index.isin(p.index).all():
        raise ValueError("Dividend ex-date missing from trading calendar")
    cash = div.reindex(p.index, fill_value=0.0)
    prev = p["close"].shift()
    ratio = prev / p["close"]
    integer = ratio.round()
    # Within 10% of a whole-number split >= 2; never infer on an ex-date.
    split = (integer >= 2) & ((ratio / integer - 1).abs() <= 0.10)
    split &= ~p.index.isin(div.index)
    event = pd.Series(1.0, index=p.index)
    event.loc[split] = 1.0 / integer.loc[split]
    ex = cash.ne(0) & prev.notna()
    event.loc[ex] = (prev.loc[ex] - cash.loc[ex]) / prev.loc[ex]
    if (event <= 0).any():
        raise ValueError("Invalid corporate-action adjustment factor")
    # Event-day prices stay unchanged; only strictly earlier prices adjust.
    adjustment = event.iloc[::-1].cumprod().iloc[::-1] / event
    adjusted = p.mul(adjustment, axis=0)
    print("Detected splits:", ", ".join(
        f"{dt.date()} x{int(integer.loc[dt])}" for dt in p.index[split]
    ) or "none")
    return adjusted


def simulate(prices, strategy):
    returns = prices["close"].pct_change(fill_method=None)
    sigma = returns.rolling(60, min_periods=60).std(ddof=1) * np.sqrt(252)
    target = (0.15 / sigma).clip(upper=1).round(2)
    target = target.where(sigma.notna(), 0.0)
    p = prices.loc[START:]
    # Monday-Sunday calendar weeks include any Saturday/holiday sessions.
    week = p.index.to_period("W-SUN")
    month = p.index.to_period("M")
    cash, units, pending, trades = 1.0, 0.0, None, 0
    equity, exposure = [], []
    for i, (day, row) in enumerate(p.iterrows()):
        if strategy == "Buy & hold" and i == 0:
            pending = 1.0
        if pending is not None:
            held = units * row.open
            wealth = cash + held
            # Solve for target fraction AFTER fees, with fees on traded value.
            gap = pending * wealth - held
            cost = BUY_COST if gap >= 0 else SELL_COST
            denominator = 1 + pending * cost if gap >= 0 else 1 - pending * cost
            delta = gap / denominator
            if abs(delta) > 1e-12 * wealth:
                units += delta / row.open
                cash -= delta + abs(delta) * cost
                trades += 1
            pending = None
        held = units * row.close
        wealth = cash + held
        equity.append(wealth)
        exposure.append(held / wealth)
        if i + 1 < len(p):
            if strategy == "VT15" and week[i] != week[i + 1]:
                pending = float(target.loc[day])
            elif strategy == "Fixed 50%" and month[i] != month[i + 1]:
                pending = 0.5
    # Liquidate at the final close; include its costs in the final daily NAV.
    if units > 1e-12:
        equity[-1] -= units * p.iloc[-1]["close"] * SELL_COST
        trades += 1
    return pd.Series(equity, index=p.index), trades, np.mean(exposure)


def metrics(nav, baseline, baseline_date):
    years = (nav.index[-1] - baseline_date).days / 365.25
    cagr = (nav.iloc[-1] / baseline) ** (1 / years) - 1
    values = np.r_[baseline, nav.to_numpy()]
    mdd = float(-(values / np.maximum.accumulate(values) - 1).min())
    ratio = cagr / mdd if mdd > 0 else np.nan
    return cagr, mdd, ratio


def main():
    prices = load_prices()
    print(f"Period: {START.date()} to {prices.index[-1].date()}")
    print("MDD is a positive loss magnitude; trades count executed orders incl. exit.")
    print("Subperiods retain holdings; only the full backtest has terminal liquidation.")
    for strategy in ("Buy & hold", "VT15", "Fixed 50%"):
        nav, trades, average_exposure = simulate(prices, strategy)
        year_end = nav.groupby(nav.index.year).last()
        annual = year_end / year_end.shift(1, fill_value=1.0) - 1
        worst_year = annual.idxmin()
        print(f"\n{strategy}: trades={trades}; worst calendar year="
              f"{worst_year} {annual.loc[worst_year]:.2%} (last year may be partial)")
        before = nav.loc[:"2014-12-31"]
        after = nav.loc["2015-01-01":]
        periods = [
            ("Full", nav, 1.0, START),
            ("2004-2014", before, 1.0, START),
            ("2015-end", after, before.iloc[-1], before.index[-1]),
        ]
        for label, segment, baseline, baseline_date in periods:
            cagr, mdd, ratio = metrics(segment, baseline, baseline_date)
            print(f"  {label:10s} CAGR={cagr:8.2%}  MDD={mdd:8.2%}"
                  f"  CAGR/MDD={ratio:.3f}")
        if strategy == "VT15":
            print(f"  Average daily-close exposure={average_exposure:.2%}"
                  " (before terminal liquidation)")


if __name__ == "__main__":
    main()
