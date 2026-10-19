"""obvs.py — the user's TradingView "OBV Strategy" (OBVS, Pine v6) re-implemented bar by bar.

Source: obvs.txt shared by the user (2026-10-19). Long only, one position. Modes:
  1  OBV fast EMA > slow EMA → hold
  2  OBV breaks its N-day high → in; breaks its M-day low → out
  3  RSI(OBV) crosses above `rsi_up` → in; crosses below `rsi_dn` → out
  4  price turns up (rises pu% from a low) → in; out when price turned down (fell pd% from a high) AND OBV fast < slow
  5  as 4, but an OBV fast/slow golden cross also enters
Exits on any mode: stop (close ≤ entry × (1 − sp%)), fixed take-profit (close ≥ entry × (1 + tp%)), the mode
turning off, trailing stop (close < highest close in the trade × (1 − trail%)). Pine fills market orders at the
NEXT bar's open, which is what trades() returns (signal bar t, entry bar t + 1, decision bar d, exit d + 1).
Differences from Pine: EMA seeded with the first value (Pine seeds with an SMA; only the first bars differ);
our costs (fee 60% discount + tax) instead of the script's flat 0.2925% per side.
"""
import numpy as np
import pandas as pd

DEFAULT = {"mode": 5, "fast": 20, "slow": 60, "chN": 20, "chM": 10, "rsi_len": 14, "rsi_up": 55.0, "rsi_dn": 45.0,
           "pu": 10.0, "pd": 8.0, "tp": 20.0, "sp": 0.0, "trail": 0.0}
# the script's own per-stock sets (客製參數 1–13)
CUSTOM = {
    "3036": dict(mode=4, fast=15, slow=40, pu=10, pd=8, tp=0),
    "2308": dict(mode=4, fast=10, slow=50, pu=16, pd=12, tp=0),
    "8069": dict(mode=5, fast=15, slow=60, pu=22, pd=11, tp=25),
    "2330": dict(mode=5, fast=30, slow=100, pu=12, pd=12, tp=0),
    "8150": dict(mode=5, fast=12, slow=36, pu=9, pd=8, tp=17.5),
    "5274": dict(mode=5, fast=50, slow=120, pu=10, pd=8, tp=22),
    "6223": dict(mode=5, fast=40, slow=120, pu=10, pd=8, tp=35),
    "2454": dict(mode=5, fast=30, slow=100, pu=12, pd=13, tp=0),
    "3443": dict(mode=4, fast=20, slow=60, pu=9, pd=8, tp=0),
    "8046": dict(mode=5, fast=20, slow=60, pu=8.5, pd=8, tp=15),
    "3037": dict(mode=5, fast=20, slow=60, pu=9, pd=8, tp=18),
    "2368": dict(mode=5, fast=50, slow=150, pu=10, pd=8, tp=35),
    "1815": dict(mode=5, fast=40, slow=120, pu=12, pd=15, tp=35),
}


def params_for(sid):
    return {**DEFAULT, **CUSTOM.get(sid, {})}


def grid():
    """Fixed candidate grid for per-stock selection (117 sets)."""
    fs = ((10, 40), (20, 60), (30, 100), (50, 150))
    turns = ((8, 8), (10, 8), (12, 12), (16, 12))
    tps = (0.0, 20.0, 35.0)
    out = []
    for tp in tps:
        out += [dict(DEFAULT, mode=1, fast=f, slow=s, tp=tp) for f, s in fs]
        out += [dict(DEFAULT, mode=2, chN=n, chM=m, tp=tp) for n, m in ((20, 10), (55, 20))]
        out.append(dict(DEFAULT, mode=3, tp=tp))
        for mode in (4, 5):
            out += [dict(DEFAULT, mode=mode, fast=f, slow=s, pu=u, pd=d, tp=tp) for f, s in fs for u, d in turns]
    return out


def label(p):
    m = p["mode"]
    core = {1: f"OBV 均線 {p['fast']}/{p['slow']} 交叉", 2: f"OBV 突破 {p['chN']} 日高／跌破 {p['chM']} 日低",
            3: f"OBV-RSI 上穿 {p['rsi_up']:.0f}／下穿 {p['rsi_dn']:.0f}",
            4: f"價格轉折 {p['pu']:g}%/{p['pd']:g}% ＋ OBV {p['fast']}/{p['slow']} 延後出場",
            5: f"價格轉折 {p['pu']:g}%/{p['pd']:g}% 或 OBV {p['fast']}/{p['slow']} 金叉提早進場"}[m]
    return f"OBVS 模式{m}：{core}" + (f"，停利 {p['tp']:g}%" if p["tp"] else "") + (f"，停損 {p['sp']:g}%" if p["sp"] else "")


def _rsi(x, n):
    d = np.diff(x, prepend=x[0])
    up = pd.Series(np.maximum(d, 0)).ewm(alpha=1 / n, adjust=False).mean().to_numpy()
    dn = pd.Series(np.maximum(-d, 0)).ewm(alpha=1 / n, adjust=False).mean().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(dn == 0, 100.0, 100 - 100 / (1 + up / dn))


class Series:
    """Per-stock arrays shared by every parameter set."""

    def __init__(self, g):
        self.o = g["aopen"].to_numpy(float)
        self.c = g["aclose"].to_numpy(float)
        self.h = g["ahigh"].to_numpy(float)
        self.l = g["alow"].to_numpy(float)
        self.ok = (g["fill_ok"] if "fill_ok" in g else pd.Series(True, index=g.index)).to_numpy(bool)
        v = g["volume"].to_numpy(float)
        step = np.sign(np.diff(self.c, prepend=self.c[0]))
        self.obv = np.nancumsum(np.nan_to_num(step) * np.nan_to_num(v))
        self._ema, self._rsi, self._ch = {}, {}, {}

    def ema(self, n):
        if n not in self._ema:
            self._ema[n] = pd.Series(self.obv).ewm(span=n, adjust=False).mean().to_numpy()
        return self._ema[n]

    def rsi(self, n):
        if n not in self._rsi:
            self._rsi[n] = _rsi(self.obv, n)
        return self._rsi[n]

    def channel(self, n, m):
        if (n, m) not in self._ch:
            s = pd.Series(self.obv)
            self._ch[(n, m)] = (s.rolling(n).max().shift(1).to_numpy(), s.rolling(m).min().shift(1).to_numpy())
        return self._ch[(n, m)]


def trades(S, p, start=0):
    """Rows (signal_i, entry_i, exit_i, gross, held, open) for parameter set p; orders allowed from bar `start`."""
    o, c, h, l, ok = S.o, S.c, S.h, S.l, S.ok
    n = len(c)
    mode, pu, pdn = p["mode"], p["pu"] / 100, p["pd"] / 100
    tp, sp, tr = p["tp"] / 100, p["sp"] / 100, p["trail"] / 100
    ef, es = S.ema(p["fast"]), S.ema(p["slow"])
    if mode == 2:
        hi, lo = S.channel(p["chN"], p["chM"])
    if mode == 3:
        rv = S.rsi(p["rsi_len"])
    st_b = st_c = st_h = False
    pdir, ext = 1, np.nan
    bull_prev = False
    obv_bull_prev = False
    pos, entry, e_i, sig_i, hh = False, np.nan, -1, -1, np.nan
    pend_in = pend_out = -1                         # decision bar of a pending order (filled at the next open)
    rows = []
    for i in range(n):
        # fills at this bar's open (orders decided on the previous close)
        if pend_in >= 0 and not pos:
            if ok[i] and np.isfinite(o[i]):
                pos, entry, e_i, sig_i, hh = True, o[i], i, pend_in, np.nan
            pend_in = -1
        if pend_out >= 0 and pos:
            if ok[i] and np.isfinite(o[i]):
                rows.append((sig_i, e_i, i, o[i] / entry - 1, pend_out - e_i + 1, False))
                pos, entry = False, np.nan
                pend_out = -1
            # not fillable → keep the order for the next valid open
        ci = c[i]
        if not np.isfinite(ci):
            continue
        # OBV states
        obv_bull = ef[i] > es[i]
        obv_on = obv_bull and not obv_bull_prev
        if mode == 2 and np.isfinite(hi[i]):
            if S.obv[i] > hi[i]:
                st_b = True
            elif S.obv[i] < lo[i]:
                st_b = False
        if mode == 3 and i > 0:
            if rv[i] > p["rsi_up"] >= rv[i - 1]:
                st_c = True
            elif rv[i] < p["rsi_dn"] <= rv[i - 1]:
                st_c = False
        # price percentage turning points
        prev_dir = pdir
        if not np.isfinite(ext):
            ext = ci
        if pdir == 1:
            ext = max(ext, h[i])
            if ci < ext * (1 - pdn):
                pdir, ext = -1, l[i]
        else:
            ext = min(ext, l[i])
            if ci > ext * (1 + pu):
                pdir, ext = 1, h[i]
        p_up = pdir == 1 and prev_dir != 1
        tp_hit = tp > 0 and pos and ci >= entry * (1 + tp)
        stop_hit = sp > 0 and pos and ci <= entry * (1 - sp)
        if not st_h and (p_up or (mode == 5 and obv_on)):
            st_h = True
        elif st_h and ((pdir == -1 and not obv_bull) or tp_hit or stop_hit):
            st_h = False
        bull = obv_bull if mode == 1 else st_b if mode == 2 else st_c if mode == 3 else st_h
        turn_on, turn_off = bull and not bull_prev, (not bull) and bull_prev
        if pos:
            hh = ci if not np.isfinite(hh) else max(hh, ci)
        trail_hit = tr > 0 and pos and ci < hh * (1 - tr)
        first = start > 0 and i == start and bull          # Pine: first bar of the backtest range while already bullish
        if i >= start and not pos and pend_in < 0 and (turn_on or first) and i + 1 < n:
            pend_in = i
        elif pos and pend_out < 0 and (stop_hit or tp_hit or turn_off or trail_hit):
            pend_out = i
        bull_prev, obv_bull_prev = bull, obv_bull
    if pos:
        rows.append((sig_i, e_i, n - 1, c[n - 1] / entry - 1, n - e_i, True))
    return rows
