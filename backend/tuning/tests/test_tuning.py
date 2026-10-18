"""pytest backend/tuning/tests -q   — no-lookahead, cost math, adjustment, BH."""
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tuning import adjust, evaluate, indicators  # noqa: E402


def synth(n=400, seed=0):
    rng = np.random.default_rng(seed)
    c = 100 * np.exp(np.cumsum(rng.normal(0, 0.02, n)))
    o = c * (1 + rng.normal(0, 0.005, n))
    h = np.maximum(o, c) * (1 + rng.uniform(0, 0.01, n))
    lo = np.minimum(o, c) * (1 - rng.uniform(0, 0.01, n))
    dates = pd.bdate_range("2020-01-01", periods=n).strftime("%Y%m%d")
    g = pd.DataFrame({"date": dates, "aopen": o, "ahigh": h, "alow": lo, "aclose": c,
                      "volume": rng.integers(1000, 5000, n).astype(float),
                      "net_buy": rng.normal(0, 300, n)})
    return g


def test_signals_have_no_lookahead():
    """Signals/features at day t must not change when future rows are appended."""
    g = synth()
    full_f = indicators.features(g)
    full_b, full_s = indicators.buy_signals(full_f), indicators.sell_signals(full_f)
    for cut in (150, 260, 333):
        part = g.iloc[:cut].copy()
        pf = indicators.features(part)
        pb, ps = indicators.buy_signals(pf), indicators.sell_signals(pf)
        for name, s in {**pb, **ps}.items():
            ref = {**full_b, **full_s}[name].iloc[:cut]
            assert (s.to_numpy() == ref.to_numpy()).all(), f"{name} changed with future data (cut={cut})"
        for col in ("rsi", "kd_k", "macd", "atr", "bb_lower", "hh20", "dev20"):
            np.testing.assert_allclose(pf[col].to_numpy(), full_f[col].iloc[:cut].to_numpy(), equal_nan=True)


def test_past_percentile_excludes_today():
    x = pd.Series(np.arange(100, dtype=float))
    q = indicators.past_q(x, 0.5, window=10, minp=10)
    assert np.isnan(q.iloc[9])                       # only 9 past values at t=9
    assert q.iloc[10] == pytest.approx(np.median(np.arange(0, 10)))   # uses x[0..9], not x[10]


def test_forward_return_entry_next_open_exit_close():
    g = synth(30)
    fr = evaluate.forward_returns(g, 3)
    t = 5
    exp = g["aclose"].iloc[t + 3] / g["aopen"].iloc[t + 1] - 1
    assert fr["gross"].iloc[t] == pytest.approx(exp)
    exp_mae = g["alow"].iloc[t + 1:t + 4].min() / g["aopen"].iloc[t + 1] - 1
    assert fr["mae"].iloc[t] == pytest.approx(exp_mae)
    assert np.isnan(fr["gross"].iloc[-3])           # exit beyond data → no label


def test_cost_math():
    c = evaluate.Costs(fee=0.001425, discount=0.6, tax_stock=0.003, tax_etf=0.001)
    buy = 0.001425 * 0.6
    sell = 0.001425 * 0.6 + 0.003
    assert c.net(0.0, etf=False) == pytest.approx((1 - sell) / (1 + buy) - 1)
    assert c.net(0.0, etf=False) == pytest.approx(-0.004699, abs=1e-5)  # ≈ 0.47% round trip
    assert c.net(0.10, etf=True) == pytest.approx(1.10 * (1 - (0.001425 * 0.6 + 0.001)) / (1 + buy) - 1)


def test_limit_up_open_is_skipped():
    g = synth(40)
    g.loc[11, "aopen"] = g.loc[10, "aclose"] * 1.10
    fr = evaluate.forward_returns(g, 3)
    assert bool(fr["limit_up"].iloc[10])


def test_thin_non_overlapping():
    assert evaluate.thin([1, 2, 3, 7, 8, 12], 5) == [1, 7, 12]


def test_adjust_backward_factor_and_gap_flag():
    dates = ["20250102", "20250103", "20250106", "20250107", "20250108"]
    g = pd.DataFrame({"date": dates, "open": [100, 101, 98, 99, 60.0], "high": [101, 102, 99, 100, 61.0],
                      "low": [99, 100, 97, 98, 59.0], "close": [100, 101, 98, 99, 60.0],
                      "change": [np.nan, 1, np.nan, 1, -39.0], "yf": False, "yf_rescaled": False,
                      "yf_unanchored": False, "yf_anchor": ""})
    fin = pd.DataFrame({"date": ["20250106"], "factor": [97.0 / 101.0]})   # ex-dividend 4 on 2025-01-06
    a, cov = adjust.adjust_stock(g, fin)
    assert a["adj_factor"].iloc[-1] == 1.0
    assert a["aclose"].iloc[1] == pytest.approx(101 * 97 / 101)
    assert a["aclose"].iloc[2] == 98                      # rows on/after the ex-date unchanged
    assert cov["unexplained_gaps"] == 1                   # -39% with no event is flagged, not patched
    assert cov["markers_unexplained"] == 0


def test_bh():
    rej = evaluate.bh_reject([0.001, 0.02, 0.03, 0.5], q=0.1)
    assert rej.tolist() == [True, True, True, False]
