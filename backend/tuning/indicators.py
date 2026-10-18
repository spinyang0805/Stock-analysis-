"""indicators.py — vectorized indicators + the fixed candidate signal library.

Indicator conventions follow src/lib/indicators.js (MA 5/10/20/60, BB(20,2) population sd, Wilder RSI14,
KD(9,3,3) seeded at 50, MACD(12,26,9), Wilder ATR14). Wilder smoothing is done with ewm(alpha=1/n),
which differs from the JS SMA seed only in the first ~3n bars.

Every signal is computed from data up to and including day t (known at the close of t).
Thresholds that are "percentiles of the stock's own history" use ONLY t-1 and earlier
(past_q = x.shift(1).rolling(W).quantile(q)) → no lookahead. Level conditions are turned into
events on the first day the condition becomes true (rising edge), crossings are events by nature.
All prices are ADJUSTED (aopen/ahigh/alow/aclose).
"""
import numpy as np
import pandas as pd

PCT_WINDOW = 250     # rolling window (trading days) for own-history percentiles
PCT_MIN = 60         # minimum past observations before a percentile threshold exists
CHIP_MIN_ROWS = 120  # chip signals only when this many valid chip rows exist


def sma(x, n):
    return x.rolling(n, min_periods=n).mean()


def ema(x, n):
    return x.ewm(span=n, adjust=False).mean()


def past_q(x, q, window=PCT_WINDOW, minp=PCT_MIN):
    return x.shift(1).rolling(window, min_periods=minp).quantile(q)


def edge(cond):
    cond = cond.fillna(False).astype(bool)
    return cond & ~cond.shift(1, fill_value=False)


def cross_up(a, b):
    return ((a > b) & (a.shift(1) <= b.shift(1))).fillna(False)


def cross_down(a, b):
    return ((a < b) & (a.shift(1) >= b.shift(1))).fillna(False)


def features(g):
    """g: adjusted rows of one stock (aopen..aclose, volume, optional net_buy). Returns feature frame."""
    c, h, l, v = g["aclose"], g["ahigh"], g["alow"], g["volume"].fillna(0)
    f = pd.DataFrame(index=g.index)
    f["close"], f["high"], f["low"], f["open"], f["volume"] = c, h, l, g["aopen"], v
    for n in (5, 10, 20, 60):
        f[f"ma{n}"] = sma(c, n)
    sd = c.rolling(20, min_periods=20).std(ddof=0)
    f["bb_lower"], f["bb_upper"] = f["ma20"] - 2 * sd, f["ma20"] + 2 * sd
    d = c.diff()
    ag = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    al = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi = 100 - 100 / (1 + ag / al.replace(0, np.nan))
    f["rsi"] = rsi.where(al > 0, 100.0).where(np.arange(len(c)) >= 14)
    lo9, hi9 = l.rolling(9, min_periods=9).min(), h.rolling(9, min_periods=9).max()
    rsv = ((c - lo9) / (hi9 - lo9) * 100).where(hi9 > lo9, 50.0).where(lo9.notna())
    k = pd.Series(np.nan, index=c.index)
    dd = pd.Series(np.nan, index=c.index)
    valid = rsv.dropna()
    if len(valid):
        kk = pd.concat([pd.Series([50.0]), valid.reset_index(drop=True)]).ewm(alpha=1 / 3, adjust=False).mean()
        dk = kk.ewm(alpha=1 / 3, adjust=False).mean()
        k.loc[valid.index], dd.loc[valid.index] = kk.iloc[1:].to_numpy(), dk.iloc[1:].to_numpy()
    f["kd_k"], f["kd_d"] = k, dd
    dif = (ema(c, 12) - ema(c, 26)).where(np.arange(len(c)) >= 25)
    f["macd"], f["macd_sig"] = dif, ema(dif.dropna(), 9).reindex(c.index)
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    f["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean().where(np.arange(len(c)) >= 14)
    f["vma20_prev"] = sma(v, 20).shift(1)
    f["vr"] = v / f["vma20_prev"].replace(0, np.nan)
    f["obv"] = (np.sign(d.fillna(0)) * v).cumsum()
    f["dev20"] = c / f["ma20"] - 1
    f["hh20"] = h.shift(1).rolling(20, min_periods=20).max()
    f["hh55"] = h.shift(1).rolling(55, min_periods=55).max()
    f["hc22"] = c.rolling(22, min_periods=10).max()
    f["up"] = d > 0
    if "net_buy" in g:
        f["net_buy"] = g["net_buy"]
    return f


def buy_signals(f):
    """dict name → boolean Series (event on day t)."""
    s = {}
    for a, b in ((5, 20), (10, 60), (20, 60)):
        s[f"ma_x_{a}_{b}"] = cross_up(f[f"ma{a}"], f[f"ma{b}"])
    s["brk_20"] = edge(f["close"] > f["hh20"])
    s["brk_55"] = edge(f["close"] > f["hh55"])
    mx = cross_up(f["macd"], f["macd_sig"])
    s["macd_x"] = mx
    s["macd_x_neg"] = mx & (f["macd"] < 0)
    for q in (10, 20):
        s[f"rsi_p{q}"] = edge(f["rsi"] < past_q(f["rsi"], q / 100))
        s[f"dev_p{q}"] = edge(f["dev20"] < past_q(f["dev20"], q / 100))
    kx = cross_up(f["kd_k"], f["kd_d"])
    s["kd_x_20"] = kx & (f["kd_k"].shift(1) < 20)
    s["kd_x_30"] = kx & (f["kd_k"].shift(1) < 30)
    s["bb_lower"] = edge(f["close"] < f["bb_lower"])
    s["vol_15_up"] = edge((f["vr"] > 1.5) & f["up"])
    s["vol_20_up"] = edge((f["vr"] > 2.0) & f["up"])
    s["obv_high"] = edge((f["obv"] > f["obv"].shift(1).rolling(60, min_periods=60).max()) & f["up"])
    s["dry_expand"] = (f["vr"].shift(1).rolling(5, min_periods=5).min() < 0.6) & (f["vr"] > 1.5) & f["up"]
    nb = f["net_buy"] if "net_buy" in f else None
    if nb is not None and nb.notna().sum() >= CHIP_MIN_ROWS:
        pos = (nb > 0).astype(int)
        streak = pos.groupby((pos == 0).cumsum()).cumsum()
        s["chip_streak_3"] = streak == 3
        s["chip_streak_5"] = streak == 5
        ratio = nb.rolling(5, min_periods=4).sum() / f["volume"].rolling(5, min_periods=4).sum().replace(0, np.nan)
        s["chip_p80"] = edge(ratio > past_q(ratio, 0.8))
        s["chip_p90"] = edge(ratio > past_q(ratio, 0.9))
    return {k: v.fillna(False).astype(bool) for k, v in s.items()}


def sell_signals(f):
    s = {"s_ma20_break": cross_down(f["close"], f["ma20"]),
         "s_macd_down": cross_down(f["macd"], f["macd_sig"])}
    for q in (80, 90):
        s[f"s_rsi_p{q}"] = edge(f["rsi"] > past_q(f["rsi"], q / 100))
    for k, tag in ((2.0, "20"), (2.5, "25"), (3.0, "30")):
        s[f"s_atr_{tag}"] = edge(f["close"] < f["hc22"] - k * f["atr"])
    return {k: v.fillna(False).astype(bool) for k, v in s.items()}


BUY_DESC = {
    "ma_x_5_20": "MA5 上穿 MA20", "ma_x_10_60": "MA10 上穿 MA60", "ma_x_20_60": "MA20 上穿 MA60",
    "brk_20": "收盤突破前 20 日高", "brk_55": "收盤突破前 55 日高",
    "macd_x": "MACD 黃金交叉", "macd_x_neg": "MACD 零軸下黃金交叉",
    "rsi_p10": "RSI14 低於自身歷史 P10", "rsi_p20": "RSI14 低於自身歷史 P20",
    "dev_p10": "MA20 乖離低於自身 P10", "dev_p20": "MA20 乖離低於自身 P20",
    "kd_x_20": "KD 低檔(<20)黃金交叉", "kd_x_30": "KD 低檔(<30)黃金交叉",
    "bb_lower": "收盤跌破布林下軌", "vol_15_up": "量比>1.5 且收紅", "vol_20_up": "量比>2.0 且收紅",
    "obv_high": "OBV 創 60 日新高且收紅", "dry_expand": "量縮後放量收紅",
    "chip_streak_3": "外資+投信連 3 日買超", "chip_streak_5": "外資+投信連 5 日買超",
    "chip_p80": "法人 5 日買超比 > 自身 P80", "chip_p90": "法人 5 日買超比 > 自身 P90",
}
SELL_DESC = {
    "s_ma20_break": "收盤跌破 MA20", "s_macd_down": "MACD 死亡交叉",
    "s_rsi_p80": "RSI14 高於自身 P80", "s_rsi_p90": "RSI14 高於自身 P90",
    "s_atr_20": "跌破 22 日高點 − 2×ATR", "s_atr_25": "跌破 22 日高點 − 2.5×ATR",
    "s_atr_30": "跌破 22 日高點 − 3×ATR",
}
SIGNAL_GROUP = {k: ("trend" if k.startswith(("ma_x", "brk", "macd")) else
                    "pullback" if k.startswith(("rsi", "dev", "kd", "bb")) else
                    "chips" if k.startswith("chip") else "volume") for k in BUY_DESC}
ATR_MULT = {"s_atr_20": 2.0, "s_atr_25": 2.5, "s_atr_30": 3.0}


# ── next-day intraday trigger levels (price the NEXT session must reach) ─────
def ma_cross_level(closes, fast, slow):
    """Price x tomorrow s.t. MA_fast(x) = MA_slow(x): above it the fast MA is above the slow MA."""
    if len(closes) < slow - 1:
        return np.nan
    sf, ss = closes[-(fast - 1):].sum(), closes[-(slow - 1):].sum()
    return (ss / slow - sf / fast) / (1 / fast - 1 / slow)


def bb_lower_level(closes, n=20, k=2.0):
    """Highest price x tomorrow with x <= lower Bollinger band computed including x (bisection)."""
    w = np.asarray(closes[-(n - 1):], dtype=float)
    if len(w) < n - 1:
        return np.nan

    def gap(x):
        a = np.append(w, x)
        return x - (a.mean() - k * a.std())

    lo, hi = 1e-6, w.mean()
    if gap(lo) > 0 or gap(hi) < 0:
        return np.nan
    for _ in range(60):
        mid = (lo + hi) / 2
        if gap(mid) <= 0:
            lo = mid
        else:
            hi = mid
    return lo
