"""pytest backend/tuning/tests -q   — no-lookahead, costs, adjustment, BH, PIT universe, nested choice, JSON."""
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tuning import adjust, data, evaluate, indicators, run_tuning, score_today  # noqa: E402


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
    g["foreign"], g["trust"] = rng.normal(0, 200, n), rng.normal(0, 100, n)
    g["margin"] = 5000 + np.cumsum(rng.normal(0, 50, n))
    g["short"] = 800 + np.cumsum(rng.normal(0, 10, n))
    g["pe"] = 15 * c / c[0] * (1 + rng.normal(0, 0.01, n))
    g["pb"], g["dy"] = 2 * c / c[0], 300 / c
    g["mkt"] = 10000 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
    month = np.arange(n) // 21                       # revenue changes once per "month"
    g["rev"] = (1e6 * (1 + 0.05 * rng.normal(size=month.max() + 1)).cumprod())[month]
    g["rev_yoy"] = rng.normal(10, 20, month.max() + 1)[month]
    g["rev_ym"] = np.array([202001 + (m // 12) * 100 + m % 12 for m in range(month.max() + 1)], float)[month]
    g["sbl"] = np.abs(1e5 + np.cumsum(rng.normal(0, 3e3, n)))
    week = np.arange(n) // 5
    g["p1000"] = (60 + np.cumsum(rng.normal(0, 0.3, week.max() + 1)))[week]
    g["p400"] = g["p1000"] + 5
    g["n_total"] = (50000 + np.cumsum(rng.normal(0, 300, week.max() + 1)))[week]
    g["td_d"] = pd.to_datetime(dates).to_series().groupby(week).transform("first").dt.strftime("%Y%m%d") \
        .astype(int).astype(float).to_numpy()
    return g


def test_signals_have_no_lookahead():
    """Signals/features at day t must not change when future rows are appended."""
    g = synth()
    full_f = indicators.features(g)
    full_b, full_s = indicators.buy_signals(full_f), indicators.sell_signals(full_f)
    for cut in (100, 119, 121, 150, 260, 333):        # 100/119/121: around the CHIP_MIN_ROWS=120 coverage gate
        part = g.iloc[:cut].copy()
        pf = indicators.features(part)
        pb, ps = indicators.buy_signals(pf), indicators.sell_signals(pf)
        allsig = {**full_b, **full_s}
        for name, s in {**pb, **ps}.items():
            ref = allsig[name].iloc[:cut]
            assert (s.to_numpy() == ref.to_numpy()).all(), f"{name} changed with future data (cut={cut})"
        for name in set(allsig) - set(pb) - set(ps):   # signal absent on the prefix ⇒ must not fire there in full
            assert not allsig[name].iloc[:cut].any(), f"{name} fires before its data coverage (cut={cut})"
        for col in ("rsi", "kd_k", "macd", "atr", "bb_lower", "hh20", "dev20"):
            np.testing.assert_allclose(pf[col].to_numpy(), full_f[col].iloc[:cut].to_numpy(), equal_nan=True)


def test_attach_extras_point_in_time():
    dates = ["20250610", "20250611", "20250613", "20250616"]
    g = pd.DataFrame({"date": dates, "volume": 1.0})
    rev = pd.DataFrame({"stock_id": "1111", "ym": ["202505"], "revenue": [100.0], "yoy": [12.0],
                        "avail_date": ["20250611"]})
    td = pd.DataFrame({"stock_id": "1111", "date": ["20250613"], "p400": [50.0], "p1000": [40.0],
                       "n_total": [1000.0]})
    mkt = {"TAIEX": pd.Series([1.0, 2.0, 3.0, 4.0], index=dates)}
    run_tuning.attach_extras(g, "1111", {"1111": {"market": "TWSE"}}, mkt, rev, None, td)
    assert np.isnan(g["rev"].iloc[0]) and g["rev"].iloc[1] == 100.0      # usable from avail_date on
    assert np.isnan(g["p1000"].iloc[2]) and g["p1000"].iloc[3] == 40.0   # strictly after the TDCC data date
    assert g["mkt"].tolist() == [1.0, 2.0, 3.0, 4.0]


def test_past_percentile_excludes_today():
    x = pd.Series(np.arange(100, dtype=float))
    q = indicators.past_q(x, 0.5, window=10, minp=10)
    assert np.isnan(q.iloc[9])                       # only 9 past values at t=9
    assert q.iloc[10] == pytest.approx(np.median(np.arange(0, 10)))   # uses x[0..9], not x[10]


def test_forward_return_entry_next_open_exit_open_after_h():
    g = synth(30)
    fr = evaluate.forward_returns(g, 3)
    t = 5
    exp = g["aopen"].iloc[t + 4] / g["aopen"].iloc[t + 1] - 1      # exit at the open of t+h+1
    assert fr["gross"].iloc[t] == pytest.approx(exp)
    assert fr["exit_date"].iloc[t] == g["date"].iloc[t + 4]
    exp_mae = g["alow"].iloc[t + 1:t + 4].min() / g["aopen"].iloc[t + 1] - 1
    assert fr["mae"].iloc[t] == pytest.approx(exp_mae)
    assert np.isnan(fr["gross"].iloc[-4])           # exit beyond data → no label


def test_forward_return_requires_valid_fill():
    g = synth(30)
    g["fill_ok"] = True
    g.loc[9, "fill_ok"] = False                      # no traded open on day 9
    fr = evaluate.forward_returns(g, 3)
    assert np.isnan(fr["gross"].iloc[8])             # entry bar invalid → no trade
    # planned exit bar (5+3+1 = 9) invalid → deferred to the next valid open (10), like simulate_position
    assert fr["exit_date"].iloc[5] == g["date"].iloc[10]
    assert fr["gross"].iloc[5] == pytest.approx(g["aopen"].iloc[10] / g["aopen"].iloc[6] - 1)


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


def test_position_state_carried_across_folds():
    def ev_rows(ps, xps):
        return pd.DataFrame({"stock_id": "1", "side": "buy", "signal": "a", "h": 5, "p": ps, "xp": xps,
                             "date": [f"d{p:03d}" for p in ps]})
    busy = {}
    f0 = evaluate.strategy_trades(ev_rows([10, 60], [21, 66]), {"1": [("a", 5)]}, "buy", busy)
    assert f0["p"].tolist() == [10, 60] and busy["1"] == 66
    # fold 1: p=62 starts while the fold-0 trade (exit 66) is open → dropped; p=64 too; p=70 is kept.
    # Without carried state, 62 would be taken and would then wrongly suppress 64…69.
    f1 = evaluate.strategy_trades(ev_rows([62, 64, 70], [68, 70, 76]), {"1": [("a", 5)]}, "buy", busy)
    assert f1["p"].tolist() == [70]


def test_block_bootstrap_calibration_under_dependent_null():
    """AR(1) (rho 0.4), zero-mean, heavy-tailed trade returns: rejection rate at 5% must stay near 5%."""
    rng = np.random.default_rng(42)
    rej, sims = 0, 300
    for s in range(sims):
        n = 60
        e = rng.standard_t(4, n) * 0.03
        x = np.zeros(n)
        for i in range(1, n):
            x[i] = 0.4 * x[i - 1] + e[i]
        rej += evaluate.block_boot_p(x, n_boot=400, seed=s) < 0.05
    assert rej / sims <= 0.10, rej / sims
    # and it has power against a real positive mean
    x = rng.standard_t(4, 60) * 0.03 + 0.02
    assert evaluate.block_boot_p(x) < 0.05


def test_select_single_stock_group_has_no_loo_prior():
    rows = [{"stock_id": "1111", "side": "buy", "signal": "brk_20", "h": 5, "date": f"2024{i:04d}",
             "xpd": 0.01, "apd": 0.0, "stat": True} for i in range(1, 40)]
    tr = pd.DataFrame(rows)
    cfg = run_tuning.Cfg()
    sel = evaluate.select(tr, {"1111": "TWSE-midvol"}, cfg, "buy")
    c = sel["cand"]
    assert c["g_mean_loo"].isna().all() and np.isfinite(c["score"]).all()


def test_bh():
    rej = evaluate.bh_reject([0.001, 0.02, 0.03, 0.5], q=0.1)
    assert rej.tolist() == [True, True, True, False]


def test_future_perturbation_does_not_change_past_signals():
    g = synth(300, seed=3)
    base_f = indicators.features(g)
    base = {**indicators.buy_signals(base_f), **indicators.sell_signals(base_f)}
    t = 220
    g2 = g.copy()
    rng = np.random.default_rng(9)
    for c in ("aopen", "ahigh", "alow", "aclose"):
        g2.loc[t + 1:, c] = g2.loc[t + 1:, c] * rng.uniform(0.5, 1.5, len(g2) - t - 1)
    g2.loc[t + 1:, "volume"] *= 10
    g2.loc[t + 1:, "net_buy"] *= -5
    for c in ("foreign", "trust", "margin", "short", "pe", "pb", "dy", "mkt", "rev", "rev_yoy", "sbl", "p400", "p1000", "n_total"):
        g2.loc[t + 1:, c] = g2.loc[t + 1:, c] * -3
    f2 = indicators.features(g2)
    pert = {**indicators.buy_signals(f2), **indicators.sell_signals(f2)}
    for k in base:
        assert (base[k].iloc[:t + 1].to_numpy() == pert[k].iloc[:t + 1].to_numpy()).all(), k


def test_non_overlap_and_cluster_t():
    d = pd.DataFrame({"p": [0, 2, 5, 6, 12], "xp": [5, 7, 10, 11, 17]})
    assert evaluate.non_overlap(d)["p"].tolist() == [0, 5, 12]
    t, g = evaluate.t_cluster([1.0, 1.1, 0.9, 1.2], [0, 0, 1, 1])
    assert g == 2 and t > 0


def test_pit_membership_uses_only_past_turnover():
    dates = pd.bdate_range("2025-01-01", periods=120).strftime("%Y%m%d").tolist()
    rows = []
    for i, d in enumerate(dates):
        rows.append({"stock_id": "1111", "date": d, "turnover": 100.0 if i < 60 else 1.0, "close": 10, "volume": 1})
        rows.append({"stock_id": "2222", "date": d, "turnover": 1.0 if i < 60 else 100.0, "close": 10, "volume": 1})
        rows.append({"stock_id": "0050", "date": d, "turnover": 1e9, "close": 10, "volume": 1})  # ETF excluded
    clean = pd.DataFrame(rows)
    inu, sched, cur = data.pit_membership(clean, dates, 1, lookback=20, every=20)
    first = dict(sched)[dates[20]]
    assert first == ["1111"]                       # ranked on days 0..19 only
    assert dict(sched)[dates[100]] == ["2222"]
    assert cur == ["2222"] and "0050" not in set(inu["stock_id"])
    # changing turnover AFTER a rebalance date must not change that rebalance
    clean2 = clean.copy()
    clean2.loc[(clean2["stock_id"] == "2222") & (clean2["date"] >= dates[20]), "turnover"] = 1e6
    _, sched2, _ = data.pit_membership(clean2, dates, 1, lookback=20, every=20)
    assert dict(sched2)[dates[20]] == ["1111"]


def _toy_events(seed=0, n_days=200):
    rng = np.random.default_rng(seed)
    rows = []
    for sid in ("1111", "2222", "3333"):
        for sig in ("brk_20", "rsi_p10"):
            for p in range(0, n_days - 5, 3):
                x = rng.normal(0.002 if sig == "brk_20" else -0.001, 0.01)
                rows.append({"stock_id": sid, "side": "buy", "signal": sig, "h": 5, "p": p, "xp": p + 5,
                             "date": f"{p:08d}", "net": x, "gross": x, "excess": x, "xnet": x, "xpd": x / 5,
                             "apd": -x / 5, "mae": -0.01, "mfe": 0.02, "in_u": True})
    return pd.DataFrame(rows)


def test_nested_choice_ignores_test_window():
    ev = _toy_events()
    cfg = run_tuning.Cfg(test_days=40, n_min=5, min_train_events=3)
    groups_at = lambda p: {s: "G" for s in ("1111", "2222", "3333")}  # noqa: E731
    lv1, ch1, *_ = run_tuning.nested_choose(ev, 0, 150, groups_at, cfg, ["1111", "2222", "3333"])
    ev2 = ev.copy()
    fut = ev2["p"] >= 150
    ev2.loc[fut, ["net", "xnet", "xpd", "gross", "excess"]] *= -100   # wreck the test window
    lv2, ch2, *_ = run_tuning.nested_choose(ev2, 0, 150, groups_at, cfg, ["1111", "2222", "3333"])
    assert lv1 == lv2 and ch1 == ch2
    assert all(c and c[0][0] == "brk_20" for c in ch1.values())


def test_make_folds_purged_and_ordered():
    cal = pd.bdate_range("2024-01-01", periods=400).strftime("%Y%m%d").tolist()
    prep = {"calendar": cal, "adjusted": {"1111": pd.DataFrame({"date": cal})}}
    cfg = run_tuning.Cfg(train_days=150, min_train_days=100, test_days=50, holdout_days=0)
    _, _, folds, train, _ = run_tuning.make_folds(prep, cfg)
    assert train == 150 and folds
    for f in folds:
        assert f["train_lo"] < f["test_lo"] < f["test_hi"] and f["test_lo"] - f["train_lo"] == train
    assert all(b["test_lo"] == a["test_hi"] for a, b in zip(folds, folds[1:]))


def test_folds_never_touch_the_holdout():
    cal = pd.bdate_range("2023-01-01", periods=700).strftime("%Y%m%d").tolist()
    prep = {"calendar": cal, "adjusted": {"1111": pd.DataFrame({"date": cal})}}
    cfg = run_tuning.Cfg(train_days=200, min_train_days=100, test_days=50, holdout_days=126)
    _, _, folds, _, _ = run_tuning.make_folds(prep, cfg)
    assert folds and all(f["test_hi"] <= 700 - 126 and f["end"] == 700 - 126 for f in folds)


def test_json_safe_and_fin():
    import json
    obj = {"a": float("nan"), "b": [np.float64("inf"), np.int64(3)], "c": np.bool_(True)}
    s = json.dumps(run_tuning.json_safe(obj), allow_nan=False)
    assert json.loads(s) == {"a": None, "b": [None, 3], "c": True}
    assert score_today.fin(float("nan")) is None and score_today.fin("x") is None and score_today.fin(0.5) == 0.5


def test_exchange_roc_date():
    assert adjust.roc_to_ymd("114年06月02日") == "20250602"
    assert adjust.roc_to_ymd("114/06/02") == "20250602"


def test_cache_is_atomic_and_tolerates_corruption(tmp_path):
    p = str(tmp_path / "x" / "c.json")
    adjust.write_cache(p, [{"a": 1}])
    assert adjust.read_cache(p)["data"] == [{"a": 1}]
    with open(p, "w", encoding="utf-8") as f:
        f.write("{broken")
    assert adjust.read_cache(p) is None
