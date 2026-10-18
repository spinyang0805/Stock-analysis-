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
TOUCH_BAND = 0.01    # support-touch tolerance around the MA (±1%)
VAL_WINDOW = 750     # valuation percentiles use ~3 years of the stock's own history
VAL_MIN = 120


def val_q(x, q):
    return past_q(x, q, window=VAL_WINDOW, minp=VAL_MIN)


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
    for n in (5, 10, 20, 60, 120, 240):
        f[f"ma{n}"] = sma(c, n)
    sd = c.rolling(20, min_periods=20).std(ddof=0)
    f["bb_lower"], f["bb_upper"] = f["ma20"] - 2 * sd, f["ma20"] + 2 * sd
    f["bb_width"] = (4 * sd) / f["ma20"]          # 通道寬度（相對中軌）
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
    # RSI6：短週期動能（與 RSI14 並列，由資料決定哪個週期適合該股）
    ag6 = d.clip(lower=0).ewm(alpha=1 / 6, adjust=False).mean()
    al6 = (-d.clip(upper=0)).ewm(alpha=1 / 6, adjust=False).mean()
    f["rsi6"] = (100 - 100 / (1 + ag6 / al6.replace(0, np.nan))).where(al6 > 0, 100.0).where(np.arange(len(c)) >= 6)
    # K 棒型態：實體、上下影線佔全幅比例；位置 = 收盤在近 20 日高低區間的百分位（0 低檔 → 1 高檔）
    o = g["aopen"]
    rng = (h - l).replace(0, np.nan)
    f["body"] = (c - o).abs() / rng
    f["ushadow"] = (h - pd.concat([c, o], axis=1).max(axis=1)) / rng
    f["lshadow"] = (pd.concat([c, o], axis=1).min(axis=1) - l) / rng
    lo20, hi20 = l.rolling(20, min_periods=20).min(), h.rolling(20, min_periods=20).max()
    f["pos20"] = ((c - lo20) / (hi20 - lo20).replace(0, np.nan))
    f["red"] = c > o
    for col in ("net_buy", "foreign", "trust", "margin", "short", "pe", "pb", "dy", "mkt", "rev", "rev_yoy", "sbl",
                "p400", "p1000", "n_total"):
        if col in g:
            f[col] = pd.to_numeric(g[col], errors="coerce").astype(float)
    if "mkt" in f and f["mkt"].notna().sum() >= 60:
        m = f["mkt"]
        mma = sma(m, 60)
        up, dn = (m > mma) & (mma > mma.shift(5)), (m < mma) & (mma < mma.shift(5))
        f["mkt_regime"] = np.where(up, 1, np.where(dn, -1, 0)).astype(float)
        f.loc[mma.isna(), "mkt_regime"] = np.nan
        f["rs"] = c / m
    return f


PAT_BOTTOM_TOL = 0.03  # W 底 / M 頭：兩個低點（高點）相差 3% 內
V_DROP, V_WIN, V_REC = 0.15, 40, 0.5


def patterns(f):
    """Chart patterns from a causal zigzag. A swing high/low is only CONFIRMED once price has reversed from it
    by thr = max(4%, 2×ATR/close) — the pivot is used from the confirmation day on, never before (no lookahead).
    Returns dict name → bool Series (event day = the breakout / breakdown day)."""
    c, h, l = f["close"].to_numpy(), f["high"].to_numpy(), f["low"].to_numpy()
    atr = f["atr"].to_numpy()
    n = len(c)
    out = {k: np.zeros(n, bool) for k in ("pat_w_bottom", "pat_n_up", "pat_v_rev", "s_pat_m_top", "s_pat_n_down",
                                          "pat_hl", "pat_bull_struct", "s_pat_lh", "s_pat_ll", "s_pat_bear_struct",
                                          "tl_down_break", "s_tl_up_break")}
    tl_used = set()           # each trendline (pair of pivot indices) fires at most once
    struct = np.zeros(n, dtype=np.int8)  # 1 頭頭高+底底高（多頭結構），-1 頭頭低+底底低（空頭結構），0 其他
    piv = []                  # confirmed pivots: (kind 'H'/'L', price)
    trend, ext, ext_i = 0, None, 0   # 1 up-leg (tracking high), -1 down-leg (tracking low); ext_i = bar of extreme
    used = set()              # one event per pivot configuration
    for t in range(n):
        if not np.isfinite(c[t]):
            continue
        thr = max(0.04, 2 * atr[t] / c[t]) if np.isfinite(atr[t]) and c[t] > 0 else 0.04
        new = None
        if trend == 0:
            trend, ext, ext_i = 1, h[t], t
        elif trend == 1:
            if h[t] > ext:
                ext, ext_i = h[t], t
            elif c[t] <= ext * (1 - thr):
                piv.append(("H", ext, ext_i))
                new = "H"
                trend, ext, ext_i = -1, l[t], t
        else:
            if l[t] < ext:
                ext, ext_i = l[t], t
            elif c[t] >= ext * (1 + thr):
                piv.append(("L", ext, ext_i))
                new = "L"
                trend, ext, ext_i = 1, h[t], t
        # 波段結構：比較最近兩個已確認高點、兩個已確認低點
        hpv = [(p, i) for k, p, i in piv[-4:] if k == "H"][-2:]
        lpv = [(p, i) for k, p, i in piv[-4:] if k == "L"][-2:]
        hs, ls = [p for p, _ in hpv], [p for p, _ in lpv]
        # 趨勢線：兩個墊高的低點連成上升線，收盤跌破 → 賣；兩個降低的高點連成下降線，收盤突破 → 買
        if len(lpv) == 2 and ls[1] > ls[0] and (lpv[0][1], lpv[1][1]) not in tl_used:
            (p0, i0), (p1, i1) = lpv
            line = p1 + (p1 - p0) / (i1 - i0) * (t - i1)
            if c[t] < line:
                out["s_tl_up_break"][t] = True
                tl_used.add((i0, i1))
        if len(hpv) == 2 and hs[1] < hs[0] and (hpv[0][1], hpv[1][1]) not in tl_used:
            (p0, i0), (p1, i1) = hpv
            line = p1 + (p1 - p0) / (i1 - i0) * (t - i1)
            if c[t] > line:
                out["tl_down_break"][t] = True
                tl_used.add((i0, i1))
        if len(hs) == 2 and len(ls) == 2:
            struct[t] = 1 if hs[1] > hs[0] and ls[1] > ls[0] else -1 if hs[1] < hs[0] and ls[1] < ls[0] else 0
        if new == "L" and len(ls) == 2:
            out["pat_hl" if ls[1] > ls[0] else "s_pat_ll"][t] = True   # 底底高 / 底底低 確認
        if new == "H" and len(hs) == 2 and hs[1] < hs[0]:
            out["s_pat_lh"][t] = True                                  # 頭頭低 確認
        if t > 0 and struct[t] != struct[t - 1]:
            if struct[t] == 1:
                out["pat_bull_struct"][t] = True
            elif struct[t] == -1:
                out["s_pat_bear_struct"][t] = True
        if len(piv) >= 3:
            (k1, p1, _), (k2, p2, _), (k3, p3, _) = piv[-3:]
            key = (len(piv), )
            if (k1, k2, k3) == ("L", "H", "L") and key + ("up",) not in used and c[t] > p2:
                used.add(key + ("up",))
                if abs(p3 / p1 - 1) <= PAT_BOTTOM_TOL:
                    out["pat_w_bottom"][t] = True          # 兩底相近，突破頸線
                elif p3 > p1:
                    out["pat_n_up"][t] = True              # 低點墊高後突破前高
            if (k1, k2, k3) == ("H", "L", "H") and key + ("dn",) not in used and c[t] < p2:
                used.add(key + ("dn",))
                if abs(p3 / p1 - 1) <= PAT_BOTTOM_TOL:
                    out["s_pat_m_top"][t] = True           # 兩頭相近，跌破頸線
                elif p3 < p1:
                    out["s_pat_n_down"][t] = True          # 高點下降後跌破前低
    # V 轉：近 40 日自高點急跌 ≥15%，低點在最近 10 日內，已收復跌幅一半以上
    hs_, lo_s = pd.Series(h), pd.Series(l)
    hi = hs_.rolling(V_WIN, min_periods=V_WIN).max().to_numpy()
    lo10 = lo_s.rolling(10, min_periods=10).min().to_numpy()
    lo40 = lo_s.rolling(V_WIN, min_periods=V_WIN).min().to_numpy()
    # 高點必須出現在低點之前（先急跌、後反彈），否則只是低檔起漲
    hi_at = hs_.rolling(V_WIN, min_periods=V_WIN).apply(np.argmax, raw=True).to_numpy()
    lo_at = lo_s.rolling(V_WIN, min_periods=V_WIN).apply(np.argmin, raw=True).to_numpy()
    with np.errstate(invalid="ignore", divide="ignore"):
        v = (lo10 <= lo40) & (lo40 <= hi * (1 - V_DROP)) & (c >= lo10 + V_REC * (hi - lo10)) & (hi_at < lo_at)
    out["pat_v_rev"] = edge(pd.Series(v, index=f.index)).to_numpy()
    res = {k: pd.Series(a, index=f.index) for k, a in out.items()}
    res["struct"] = pd.Series(struct, index=f.index)
    return res


def _monthly(f):
    """Revenue figures arrive once a month: (new_flag, monthly-series helpers mapped back to days)."""
    rev = f["rev"]
    new = rev.notna() & (rev != rev.shift(1))
    return new


def extra_buy(f):
    s = {}
    if "rs" in f:
        rs = f["rs"]
        rs_hi = rs > rs.shift(1).rolling(60, min_periods=60).max()
        s["rs_high"] = edge(rs_hi)                                   # 相對大盤強度創 60 日新高
        s["rs_lead"] = edge(rs_hi & (f["close"] <= f["hh55"]))       # 股價未創高、相對強度先創高（領先）
        m20, c20 = f["mkt"].pct_change(20, fill_method=None), f["close"].pct_change(20, fill_method=None)
        s["rs_resilient"] = edge((m20 < -0.05) & (c20 > 0))          # 大盤 20 日跌 >5% 時逆勢上漲（抗跌）
        s["mkt_turn_bull"] = edge(f["mkt_regime"] == 1)              # 大盤轉多
    if _has(f, "rev"):
        new = _monthly(f)
        rv = f["rev"][new]
        hi12 = rv > rv.shift(1).rolling(12, min_periods=12).max()
        s["rev_high12"] = new & hi12.reindex(f.index).fillna(False)  # 月營收創 12 個月新高
        y = f["rev_yoy"][new]
        s["rev_yoy_accel"] = new & ((y > 20) & (y > y.shift(1))).reindex(f.index).fillna(False)  # 年增 >20% 且加速
        st3 = (y > 0).astype(float).rolling(3, min_periods=3).min() == 1
        s["rev_yoy_3up"] = new & (st3 & ~st3.shift(1, fill_value=False)).reindex(f.index).fillna(False)
    # 缺口：向上跳空（整日最低 > 前日最高）；之後 20 日內回測缺口上緣守住
    gap_up = f["low"] > f["high"].shift(1)
    s["gap_up"] = gap_up & f["up"]
    lvl = f["high"].shift(1).where(gap_up).ffill(limit=20)
    s["gap_support"] = edge(~gap_up & (f["low"] <= lvl * (1 + TOUCH_BAND)) & (f["close"] >= lvl))
    if _has(f, "sbl"):
        chg = f["sbl"].pct_change(5, fill_method=None).replace([np.inf, -np.inf], np.nan)
        s["sbl_cover"] = edge((chg < past_q(chg, 0.1)) & (f["close"] > f["ma20"]))  # 借券大量回補且站上月線
    s.update(_whale(f, "buy"))
    return s


def _whale(f, side):
    """集保大戶：週資料，只在新一週數字出現的那天判斷。集中 = 千張(或 400 張)大戶比例連 2 週上升且總人數下降。"""
    if not _has(f, "p1000"):
        return {}
    new = f["p1000"].notna() & ((f["p1000"] != f["p1000"].shift(1)) | (f["n_total"] != f["n_total"].shift(1)))
    w = f.loc[new, ["p400", "p1000", "n_total"]]
    d1000, d400, dn = w["p1000"].diff(), w["p400"].diff(), w["n_total"].diff()
    out = {}
    if side == "buy":
        conc = (d1000 > 0) & (d1000.shift(1) > 0) & (dn < 0)
        big = d400 > d400.shift(1).rolling(26, min_periods=8).quantile(0.8)
        out["whale_conc"] = conc.reindex(f.index).fillna(False) & new          # 千張大戶連 2 週增、散戶減
        out["whale_400_jump"] = (big & (d400 > 0)).reindex(f.index).fillna(False) & new  # 400 張大戶週增幅創高
    else:
        disp = (d1000 < 0) & (d1000.shift(1) < 0) & (dn > 0)
        out["s_whale_disp"] = disp.reindex(f.index).fillna(False) & new        # 千張大戶連 2 週減、散戶增
    return out


def extra_sell(f):
    s = {}
    if "rs" in f:
        rs = f["rs"]
        s["s_rs_low"] = edge(rs < rs.shift(1).rolling(60, min_periods=60).min())  # 相對強度創 60 日新低
        s["s_mkt_turn_bear"] = edge(f["mkt_regime"] == -1)                        # 大盤轉空
    if _has(f, "rev"):
        new = _monthly(f)
        y = f["rev_yoy"][new]
        s["s_rev_yoy_neg"] = new & ((y < 0) & (y.shift(1) > 0)).reindex(f.index).fillna(False)  # 年增轉負
    gap_dn = f["high"] < f["low"].shift(1)
    s["s_gap_down"] = gap_dn                                                       # 向下跳空缺口
    gap_up = f["low"] > f["high"].shift(1)
    lvl = f["high"].shift(1).where(gap_up).ffill(limit=20)
    s["s_gap_fill"] = edge(f["close"] < lvl)                                      # 向上缺口被回補（跌破缺口）
    if _has(f, "sbl"):
        chg = f["sbl"].pct_change(5, fill_method=None).replace([np.inf, -np.inf], np.nan)
        s["s_sbl_surge"] = edge((chg > past_q(chg, 0.9)) & (f["sbl"] > 0))       # 借券賣出 5 日大增
    s.update(_whale(f, "sell"))
    return s


def _pct5(x):
    """5-day % change; a 0 → x move is undefined (inf) and becomes NaN so it can't fire or poison past_q."""
    return x.pct_change(5, fill_method=None).replace([np.inf, -np.inf], np.nan)


def _streak(x):
    pos = (x > 0).astype(int)
    return pos.groupby((pos == 0).cumsum()).cumsum()


def _ratio5(x, vol):
    return x.rolling(5, min_periods=4).sum() / vol.rolling(5, min_periods=4).sum().replace(0, np.nan)


def _has(f, col):
    return col in f and f[col].notna().sum() >= CHIP_MIN_ROWS


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
    # 布林：跌破下軌後收回（止跌確認）、通道壓縮到自身低 10% 後收盤突破上軌（盤整噴出）、
    # 中軌上揚時收盤站上上軌（沿上軌走強）
    s["bb_lower_back"] = cross_up(f["close"], f["bb_lower"])
    squeeze = f["bb_width"].shift(1) < past_q(f["bb_width"], 0.1)
    s["bb_squeeze_brk"] = edge(squeeze.rolling(5, min_periods=1).max().astype(bool) & (f["close"] > f["bb_upper"]))
    s["bb_upper_ride"] = edge((f["close"] > f["bb_upper"]) & (f["ma20"] > f["ma20"].shift(5)))
    s["vol_15_up"] = edge((f["vr"] > 1.5) & f["up"])
    s["vol_20_up"] = edge((f["vr"] > 2.0) & f["up"])
    s["obv_high"] = edge((f["obv"] > f["obv"].shift(1).rolling(60, min_periods=60).max()) & f["up"])
    s["dry_expand"] = (f["vr"].shift(1).rolling(5, min_periods=5).min() < 0.6) & (f["vr"] > 1.5) & f["up"]
    # 回測支撐：均線上揚、前 3 日收盤都在均線上，今日最低觸及均線 ±TOUCH_BAND 且收盤守住
    for n in (10, 20, 60, 120, 240):
        ma = f[f"ma{n}"]
        rising = ma > ma.shift(5)
        above = (f["close"].shift(1) > ma.shift(1)).astype(float).rolling(3, min_periods=3).min() == 1
        touch = (f["low"] <= ma * (1 + TOUCH_BAND)) & (f["close"] >= ma * (1 - TOUCH_BAND))
        s[f"touch_ma{n}"] = edge(rising & above & touch)
        s[f"touch_ma{n}_up"] = edge(rising & above & touch & f["up"])  # 觸及且收紅（確認止跌）
    # 動能：RSI6 低於自身歷史分位、RSI14 站上 50
    for q in (10, 20):
        s[f"rsi6_p{q}"] = edge(f["rsi6"] < past_q(f["rsi6"], q / 100))
    s["rsi_x50"] = cross_up(f["rsi"], pd.Series(50.0, index=f.index))
    # K 棒：長下影線的意義依位置而定 —— 低檔（近 20 日區間下緣）或剛好落在月線支撐
    long_lower = (f["lshadow"] >= 0.5) & (f["body"] <= 0.35)
    s["hammer_low"] = long_lower & (f["pos20"] <= 0.25)
    s["lshadow_ma20"] = long_lower & (f["low"] <= f["ma20"] * (1 + TOUCH_BAND)) & (f["close"] >= f["ma20"]) \
        & (f["ma20"] > f["ma20"].shift(5))
    prev_black = f["close"].shift(1) < f["open"].shift(1)
    s["engulf_low"] = f["red"] & prev_black & (f["close"] >= f["open"].shift(1)) \
        & (f["open"] <= f["close"].shift(1)) & (f["pos20"].shift(1) <= 0.3)
    # 均線型態：糾結後帶量突破、多頭排列成形
    mas = f[["ma5", "ma10", "ma20"]]
    spread = mas.max(axis=1) / mas.min(axis=1) - 1
    tangled = spread.shift(1).rolling(5, min_periods=5).max() < 0.015
    s["ma_squeeze_brk"] = edge(tangled & (f["close"] > mas.max(axis=1)) & f["up"] & (f["vr"] > 1.2))
    s["ma_bull_align"] = edge((f["ma5"] > f["ma10"]) & (f["ma10"] > f["ma20"]) & (f["ma20"] > f["ma60"])
                              & (f["ma20"] > f["ma20"].shift(5)) & (f["ma60"] > f["ma60"].shift(5)))
    pat = patterns(f)
    for k in ("pat_w_bottom", "pat_n_up", "pat_v_rev", "pat_hl", "pat_bull_struct", "tl_down_break"):
        s[k] = pat[k]
    # 量價：量縮回測月線（賣壓輕）
    s["dry_touch_ma20"] = edge((f["vr"] < 0.7) & (f["low"] <= f["ma20"] * (1 + TOUCH_BAND))
                               & (f["close"] >= f["ma20"]) & (f["ma20"] > f["ma20"].shift(5)))
    nb = f["net_buy"] if "net_buy" in f else None
    if nb is not None and nb.notna().sum() >= CHIP_MIN_ROWS:
        streak = _streak(nb)
        s["chip_streak_3"] = streak == 3
        s["chip_streak_5"] = streak == 5
        ratio = _ratio5(nb, f["volume"])
        s["chip_p80"] = edge(ratio > past_q(ratio, 0.8))
        s["chip_p90"] = edge(ratio > past_q(ratio, 0.9))
    # 法人分開看：外資、投信各自連買 / 買超比創自身高分位；投信首買（前 10 日皆未買超）
    for col in ("foreign", "trust"):
        if _has(f, col):
            x = f[col]
            st = _streak(x)
            s[f"{col}_streak_3"] = st == 3
            s[f"{col}_streak_5"] = st == 5
            r = _ratio5(x, f["volume"])
            s[f"{col}_p90"] = edge(r > past_q(r, 0.9))
    if _has(f, "trust"):
        t = f["trust"]
        s["trust_first"] = (t > 0) & ((t.shift(1) > 0).astype(float).rolling(10, min_periods=10).max() == 0)
    # 融資融券：融資減少但股價站穩上揚月線（籌碼沉澱）；融券增加中突破 20 日高（軋空）
    if _has(f, "margin"):
        mchg = _pct5(f["margin"])
        s["margin_drop_up"] = edge((mchg < past_q(mchg, 0.2)) & (f["close"] > f["ma20"])
                                   & (f["ma20"] > f["ma20"].shift(5)))
    if _has(f, "short"):
        schg = _pct5(f["short"])
        s["short_squeeze"] = edge((schg > past_q(schg, 0.8)) & (f["close"] > f["hh20"]))
    # 估值：相對自身近 3 年的位置 —— 便宜價 ≈ 本益比/淨值比落在自身低 20%，高殖利率 ≈ 自身高 20%
    cheap = None
    if _has(f, "pe"):
        pe = f["pe"].where(f["pe"] > 0)
        s["val_pe_cheap"] = edge(pe < val_q(pe, 0.2))
        cheap = pe < val_q(pe, 0.2)
    if _has(f, "pb"):
        pb = f["pb"].where(f["pb"] > 0)
        s["val_pb_cheap"] = edge(pb < val_q(pb, 0.2))
        cheap = (pb < val_q(pb, 0.2)) if cheap is None else (cheap | (pb < val_q(pb, 0.2)))
    if _has(f, "dy"):
        dy = f["dy"].where(f["dy"] > 0)
        s["val_dy_high"] = edge(dy > val_q(dy, 0.8))
    if cheap is not None:
        s["val_cheap_turn"] = edge(cheap.fillna(False) & (f["close"] > f["ma20"]) & (f["ma20"] > f["ma20"].shift(5)))
    s.update(extra_buy(f))
    return {k: v.fillna(False).astype(bool) for k, v in s.items()}


def sell_signals(f):
    s = {"s_ma20_break": cross_down(f["close"], f["ma20"]),
         "s_macd_down": cross_down(f["macd"], f["macd_sig"])}
    for q in (80, 90):
        s[f"s_rsi_p{q}"] = edge(f["rsi"] > past_q(f["rsi"], q / 100))
    for k, tag in ((2.0, "20"), (2.5, "25"), (3.0, "30")):
        s[f"s_atr_{tag}"] = edge(f["close"] < f["hc22"] - k * f["atr"])
    # 布林：站上上軌後跌回通道內（漲勢轉弱）、跌破下軌（弱勢加速）
    s["s_bb_upper_fail"] = cross_down(f["close"], f["bb_upper"])
    s["s_bb_lower_break"] = edge(f["close"] < f["bb_lower"])
    pat = patterns(f)
    for k in ("s_pat_m_top", "s_pat_n_down", "s_pat_lh", "s_pat_ll", "s_pat_bear_struct", "s_tl_up_break"):
        s[k] = pat[k]
    s["s_ma_bear_align"] = edge((f["ma5"] < f["ma10"]) & (f["ma10"] < f["ma20"]) & (f["ma20"] < f["ma60"]))
    # K 棒：長上影線在高檔、高檔吞噬黑 K
    s["s_shoot_high"] = (f["ushadow"] >= 0.5) & (f["body"] <= 0.35) & (f["pos20"] >= 0.8)
    prev_red = f["close"].shift(1) > f["open"].shift(1)
    s["s_engulf_high"] = (f["close"] < f["open"]) & prev_red & (f["close"] <= f["open"].shift(1)) \
        & (f["open"] >= f["close"].shift(1)) & (f["pos20"].shift(1) >= 0.7)
    # 動能 / 量價：KD 高檔死叉、價創 20 日高但量縮（量價背離）
    kx = cross_down(f["kd_k"], f["kd_d"])
    s["s_kd_x_80"] = kx & (f["kd_k"].shift(1) > 80)
    s["s_vol_div"] = edge((f["close"] > f["hh20"]) & (f["vr"] < 0.8))
    # 籌碼：外資連 5 日賣超、融資 5 日增幅創自身高分位且在高檔（散戶追高）
    if _has(f, "foreign"):
        s["s_foreign_sell_5"] = _streak(-f["foreign"]) == 5
    if _has(f, "margin"):
        mchg = _pct5(f["margin"])
        s["s_margin_surge"] = edge((mchg > past_q(mchg, 0.9)) & (f["pos20"] >= 0.8))
    # 估值：昂貴價 ≈ 本益比/淨值比進入自身近 3 年高 10%
    if _has(f, "pe"):
        pe = f["pe"].where(f["pe"] > 0)
        s["s_val_pe_exp"] = edge(pe > val_q(pe, 0.9))
    if _has(f, "pb"):
        pb = f["pb"].where(f["pb"] > 0)
        s["s_val_pb_exp"] = edge(pb > val_q(pb, 0.9))
    s.update(extra_sell(f))
    return {k: v.fillna(False).astype(bool) for k, v in s.items()}


BUY_DESC = {
    "ma_x_5_20": "MA5 上穿 MA20", "ma_x_10_60": "MA10 上穿 MA60", "ma_x_20_60": "MA20 上穿 MA60",
    "brk_20": "收盤突破前 20 日高", "brk_55": "收盤突破前 55 日高",
    "macd_x": "MACD 黃金交叉", "macd_x_neg": "MACD 零軸下黃金交叉",
    "rsi_p10": "RSI14 低於自身歷史 P10", "rsi_p20": "RSI14 低於自身歷史 P20",
    "dev_p10": "MA20 乖離低於自身 P10", "dev_p20": "MA20 乖離低於自身 P20",
    "kd_x_20": "KD 低檔(<20)黃金交叉", "kd_x_30": "KD 低檔(<30)黃金交叉",
    "bb_lower": "收盤跌破布林下軌", "bb_lower_back": "跌破布林下軌後收回",
    "bb_squeeze_brk": "布林壓縮後突破上軌", "bb_upper_ride": "中軌上揚、收盤站上布林上軌", "vol_15_up": "量比>1.5 且收紅", "vol_20_up": "量比>2.0 且收紅",
    "obv_high": "OBV 創 60 日新高且收紅", "dry_expand": "量縮後放量收紅",
    "chip_streak_3": "外資+投信連 3 日買超", "chip_streak_5": "外資+投信連 5 日買超",
    "chip_p80": "法人 5 日買超比 > 自身 P80", "chip_p90": "法人 5 日買超比 > 自身 P90",
    "touch_ma10": "上揚 MA10 回測守住", "touch_ma10_up": "上揚 MA10 回測守住且收紅",
    "touch_ma20": "上揚月線回測守住", "touch_ma20_up": "上揚月線回測守住且收紅",
    "touch_ma60": "上揚季線回測守住", "touch_ma60_up": "上揚季線回測守住且收紅",
    "touch_ma120": "上揚半年線回測守住", "touch_ma120_up": "上揚半年線回測守住且收紅",
    "touch_ma240": "上揚年線回測守住", "touch_ma240_up": "上揚年線回測守住且收紅",
    "rsi6_p10": "RSI6 低於自身歷史 P10", "rsi6_p20": "RSI6 低於自身歷史 P20", "rsi_x50": "RSI14 站上 50",
    "hammer_low": "低檔長下影線", "lshadow_ma20": "月線長下影線（支撐有效）", "engulf_low": "低檔紅 K 吞噬",
    "dry_touch_ma20": "量縮回測月線",
    "foreign_streak_3": "外資連 3 日買超", "foreign_streak_5": "外資連 5 日買超",
    "foreign_p90": "外資 5 日買超比 > 自身 P90",
    "trust_streak_3": "投信連 3 日買超", "trust_streak_5": "投信連 5 日買超",
    "trust_p90": "投信 5 日買超比 > 自身 P90", "trust_first": "投信首買（前 10 日未買）",
    "margin_drop_up": "融資減少且站穩上揚月線", "short_squeeze": "融券增加中突破 20 日高（軋空）",
    "val_pe_cheap": "本益比進入自身 3 年低 20%（便宜）", "val_pb_cheap": "淨值比進入自身 3 年低 20%（便宜）",
    "val_dy_high": "殖利率進入自身 3 年高 20%", "val_cheap_turn": "便宜區且站上上揚月線（轉強）",
    "rs_high": "相對大盤強度創 60 日新高", "rs_lead": "股價未創高、相對強度先創高（領先）",
    "rs_resilient": "大盤 20 日跌逾 5% 時逆勢上漲（抗跌）", "mkt_turn_bull": "大盤轉為多頭",
    "rev_high12": "月營收創 12 個月新高", "rev_yoy_accel": "月營收年增 >20% 且加速",
    "rev_yoy_3up": "月營收連 3 個月年增", "gap_up": "向上跳空缺口且收紅",
    "gap_support": "回測向上缺口守住", "sbl_cover": "借券大量回補且站上月線",
    "whale_conc": "千張大戶連 2 週增、股東人數減（籌碼集中）", "whale_400_jump": "400 張大戶週增幅創半年高",
    "ma_squeeze_brk": "均線糾結後帶量突破", "ma_bull_align": "均線多頭排列成形",
    "pat_w_bottom": "W 底突破頸線", "pat_n_up": "N 字上攻（低點墊高突破前高）", "pat_v_rev": "V 型反轉",
    "tl_down_break": "突破下降趨勢線", "pat_hl": "底底高確認", "pat_bull_struct": "轉為多頭結構（頭頭高＋底底高）",
}
SELL_DESC = {
    "s_ma20_break": "收盤跌破 MA20", "s_macd_down": "MACD 死亡交叉",
    "s_rsi_p80": "RSI14 高於自身 P80", "s_rsi_p90": "RSI14 高於自身 P90",
    "s_atr_20": "跌破 22 日高點 − 2×ATR", "s_atr_25": "跌破 22 日高點 − 2.5×ATR",
    "s_atr_30": "跌破 22 日高點 − 3×ATR",
    "s_shoot_high": "高檔長上影線", "s_engulf_high": "高檔黑 K 吞噬", "s_kd_x_80": "KD 高檔(>80)死亡交叉",
    "s_vol_div": "創 20 日高但量縮（量價背離）", "s_foreign_sell_5": "外資連 5 日賣超",
    "s_margin_surge": "高檔融資 5 日增幅 > 自身 P90",
    "s_rs_low": "相對大盤強度創 60 日新低", "s_mkt_turn_bear": "大盤轉為空頭",
    "s_rev_yoy_neg": "月營收年增轉負", "s_gap_down": "向下跳空缺口", "s_gap_fill": "向上缺口被回補",
    "s_sbl_surge": "借券賣出 5 日大增", "s_whale_disp": "千張大戶連 2 週減、股東人數增（籌碼分散）",
    "s_bb_upper_fail": "站上布林上軌後跌回", "s_bb_lower_break": "收盤跌破布林下軌",
    "s_ma_bear_align": "均線空頭排列成形", "s_pat_m_top": "M 頭跌破頸線", "s_pat_n_down": "N 字下跌（高點下降跌破前低）",
    "s_tl_up_break": "跌破上升趨勢線", "s_pat_lh": "頭頭低確認", "s_pat_ll": "底底低確認", "s_pat_bear_struct": "轉為空頭結構（頭頭低＋底底低）",
    "s_val_pe_exp": "本益比進入自身 3 年高 10%（昂貴）", "s_val_pb_exp": "淨值比進入自身 3 年高 10%（昂貴）",
}
SIGNAL_GROUP = {k: ("trend" if k.startswith(("ma_", "brk", "macd", "bb_squeeze", "bb_upper")) else
                    "pullback" if k.startswith(("rsi", "dev", "kd", "bb", "touch", "dry_touch")) else
                    "candle" if k in ("hammer_low", "lshadow_ma20", "engulf_low") else
                    "valuation" if k.startswith("val_") else
                    "fundamental" if k.startswith("rev_") else
                    "relative" if k.startswith(("rs_", "mkt_")) else
                    "gap" if k.startswith("gap_") else
                    "pattern" if k.startswith(("pat_", "tl_")) else
                    "chips" if k.startswith(("chip", "foreign", "trust", "margin", "short", "sbl", "whale")) else "volume")
                for k in BUY_DESC}
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
