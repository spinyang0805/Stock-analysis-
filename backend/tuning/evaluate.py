"""evaluate.py — event study, walk-forward selection, multiple-testing guard, shrinkage.

Event (signal known at close t):  entry = adjusted OPEN of t+1, exit = adjusted CLOSE of t+h.
Skipped when open(t+1) >= close(t) * (1 + LIMIT_SKIP) (≈ limit-up, cannot be filled).
Events of one (stock, signal, h) are thinned to be >= h trading days apart (non-overlapping) so the
t-stat is not inflated by overlapping holding periods.
Net return = (1+gross)(1-sell_cost)/(1+buy_cost) - 1, buy_cost = fee*discount,
sell_cost = fee*discount + tax (0.3% stocks / 0.1% ETFs).
"""
import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

from . import indicators as ind

LIMIT_SKIP = 0.095


@dataclass
class Costs:
    fee: float = 0.001425
    discount: float = 0.6
    tax_stock: float = 0.003
    tax_etf: float = 0.001

    def buy(self):
        return self.fee * self.discount

    def sell(self, etf):
        return self.fee * self.discount + (self.tax_etf if etf else self.tax_stock)

    def net(self, gross, etf):
        return (1 + gross) * (1 - self.sell(etf)) / (1 + self.buy()) - 1


def forward_returns(g, h):
    """Per-row forward outcome for horizon h (entry next open, exit close t+h). No lookahead in the
    signal: everything here is strictly after t and is only used as the label."""
    entry = g["aopen"].shift(-1)
    exit_ = g["aclose"].shift(-h)
    gross = exit_ / entry - 1
    mae = g["alow"].rolling(h, min_periods=h).min().shift(-h) / entry - 1
    mfe = g["ahigh"].rolling(h, min_periods=h).max().shift(-h) / entry - 1
    limit_up = (entry / g["aclose"] - 1) >= LIMIT_SKIP
    return pd.DataFrame({"gross": gross, "mae": mae, "mfe": mfe, "limit_up": limit_up,
                         "exit_date": g["date"].shift(-h)})


def thin(idx, h):
    """Keep event positions at least h apart (greedy, chronological)."""
    keep, last = [], -10 ** 9
    for i in idx:
        if i - last >= h:
            keep.append(i)
            last = i
    return keep


def stock_events(sid, g, horizons, costs, etf):
    """Returns (events DataFrame, fwd-gross DataFrame[date, h, gross], features, signals dicts)."""
    f = ind.features(g)
    buys, sells = ind.buy_signals(f), ind.sell_signals(f)
    dates = g["date"].to_numpy()
    rows, fwd_all = [], []
    for h in horizons:
        fr = forward_returns(g, h)
        ok = fr["gross"].notna() & ~fr["limit_up"]
        fwd_all.append(pd.DataFrame({"date": dates, "h": h, "gross": fr["gross"].where(ok).to_numpy()}))
        for side, sigs in (("buy", buys), ("sell", sells)):
            for name, s in sigs.items():
                pos = np.flatnonzero((s & (ok if side == "buy" else fr["gross"].notna())).to_numpy())
                if not len(pos):
                    continue
                pos = thin(pos, h)
                sub = fr.iloc[pos]
                rows.append(pd.DataFrame({
                    "stock_id": sid, "side": side, "signal": name, "h": h, "date": dates[pos],
                    "exit_date": sub["exit_date"].to_numpy(), "gross": sub["gross"].to_numpy(),
                    "mae": sub["mae"].to_numpy(), "mfe": sub["mfe"].to_numpy()}))
    ev = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=["stock_id", "side", "signal", "h", "date", "exit_date", "gross", "mae", "mfe"])
    ev["net"] = np.where(ev["side"] == "buy", costs.net(ev["gross"].astype(float), etf), np.nan)
    return ev, pd.concat(fwd_all, ignore_index=True), f, buys, sells


# ── statistics ───────────────────────────────────────────────────────────────
def pvalue(t):
    return math.erfc(abs(t) / math.sqrt(2)) if np.isfinite(t) else 1.0


def agg_stats(df, keys, col="net"):
    g = df.groupby(keys, observed=True)[col]
    st = g.agg(n="size", mean="mean", sd="std", median="median")
    st["win"] = df.assign(_w=df[col] > 0).groupby(keys, observed=True)["_w"].mean()
    st["t"] = st["mean"] / (st["sd"] / np.sqrt(st["n"])).replace(0, np.nan)
    st["t"] = st["t"].fillna(0.0)
    return st


def bh_reject(p, q=0.10):
    """Benjamini–Hochberg: boolean array of rejections."""
    p = np.asarray(p, dtype=float)
    m = len(p)
    if not m:
        return np.zeros(0, bool)
    order = np.argsort(p)
    thr = q * np.arange(1, m + 1) / m
    passed = p[order] <= thr
    k = np.max(np.nonzero(passed)[0]) + 1 if passed.any() else 0
    rej = np.zeros(m, bool)
    rej[order[:k]] = True
    return rej


def assign_groups(ret_panel, meta, end_pos, lookback):
    """Volatility tercile × market (ETFs pooled separately) using returns BEFORE end_pos only."""
    win = ret_panel.iloc[max(0, end_pos - lookback):end_pos]
    vol = win.std()
    stocks = [c for c in vol.index if not meta[c]["etf"]]
    terc = pd.qcut(vol[stocks].rank(method="first"), 3, labels=["lowvol", "midvol", "highvol"]) \
        if len(stocks) >= 3 else pd.Series("midvol", index=stocks)
    out = {}
    for c in vol.index:
        if meta[c]["etf"]:
            out[c] = "ETF"
        else:
            mk = "TWSE" if meta[c]["market"] in ("TWSE", "上市") else "TPEx"
            out[c] = f"{mk}-{terc.get(c, 'midvol')}"
    return out


def add_scores(ev):
    """xnet = net return minus the equal-weight universe gross return over the same window (timing value,
    not market beta); xpd = xnet per holding day (so 3-day and 20-day horizons compete fairly).
    Sell side: apd = -(gross - bench) / h = market-relative return AVOIDED per day by exiting."""
    ev["xnet"] = ev["net"] - ev["bench"]
    ev["xpd"] = ev["xnet"] / ev["h"]
    ev["apd"] = -(ev["gross"] - ev["bench"]) / ev["h"]
    return ev


def select(train, groups, cfg, side="buy"):
    """Selection on a training slice of events. Returns dict with per-stock, per-group, uniform choices
    and the scored candidate table. Ranking column: xpd (buy) / apd (sell).
    Score = shrunk mean = w*stock + (1-w)*group, w = n/(n+k)."""
    col = "xpd" if side == "buy" else "apd"
    tr = train[train["side"] == side].copy()
    if tr.empty:
        return {"stock": {}, "group": {}, "uniform": [], "cand": pd.DataFrame()}
    tr["group"] = tr["stock_id"].map(groups)
    st = agg_stats(tr, ["stock_id", "signal", "h"], col).reset_index()
    gs = agg_stats(tr, ["group", "signal", "h"], col).reset_index()
    us = agg_stats(tr, ["signal", "h"], col).reset_index()
    st["group"] = st["stock_id"].map(groups)
    st = st.merge(gs[["group", "signal", "h", "mean", "n", "t"]].rename(
        columns={"mean": "g_mean", "n": "g_n", "t": "g_t"}), on=["group", "signal", "h"], how="left")
    w = st["n"] / (st["n"] + cfg.shrink_k)
    st["score"] = w * st["mean"] + (1 - w) * st["g_mean"].fillna(0)
    st["p"] = [pvalue(t) for t in st["t"]]

    def top3(df, score_col, min_n):
        df = df[(df["n"] >= min_n) & (df[score_col] > 0)].sort_values(score_col, ascending=False)
        df = df.drop_duplicates("signal")
        return list(zip(df["signal"].head(cfg.max_signals), df["h"].head(cfg.max_signals)))

    stock_choice = {sid: top3(d, "score", cfg.min_train_events) for sid, d in st.groupby("stock_id")}
    gs["ok"] = gs["t"] > 0
    group_choice = {grp: top3(d[d["ok"]], "mean", cfg.n_min) for grp, d in gs.groupby("group")}
    uniform = top3(us[us["t"] > 0], "mean", cfg.n_min)
    return {"stock": stock_choice, "group": group_choice, "uniform": uniform, "cand": st,
            "group_stats": gs, "uniform_stats": us}


def strategy_trades(test, choices_by_stock, side="buy"):
    """Simulated trades: on each stock-day take the highest-ranked chosen signal that fired."""
    te = test[test["side"] == side]
    rows = []
    for sid, chosen in choices_by_stock.items():
        if not chosen:
            continue
        rank = {(s, h): i for i, (s, h) in enumerate(chosen)}
        d = te[te["stock_id"] == sid]
        if d.empty:
            continue
        key = list(zip(d["signal"], d["h"]))
        r = np.array([rank.get(k, -1) for k in key])
        d = d[r >= 0].assign(_r=r[r >= 0]).sort_values(["date", "_r"]).drop_duplicates("date")
        rows.append(d)
    return pd.concat(rows, ignore_index=True) if rows else te.iloc[:0]


def _t(x):
    sd = x.std()
    return float(x.mean() / (sd / math.sqrt(len(x)))) if len(x) > 1 and sd > 0 else 0.0


def summarize(tr, col="net"):
    """OOS trade summary. mean/median/win/t on net return; xnet/xt on market-relative net return."""
    if tr.empty:
        return {"n": 0, "mean": np.nan, "median": np.nan, "win": np.nan, "t": 0.0, "excess": np.nan,
                "xnet": np.nan, "xt": 0.0, "mae_med": np.nan, "mfe_med": np.nan}
    x = tr[col].astype(float)
    xn = tr["xnet"].astype(float) if "xnet" in tr else x * np.nan
    return {"n": int(len(x)), "mean": float(x.mean()), "median": float(x.median()), "win": float((x > 0).mean()),
            "t": _t(x), "excess": float(tr["excess"].mean()) if "excess" in tr else np.nan,
            "xnet": float(xn.mean()), "xt": _t(xn),
            "mae_med": float(tr["mae"].median()), "mfe_med": float(tr["mfe"].median())}
