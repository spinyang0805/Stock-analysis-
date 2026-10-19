"""research_combo.py — "combination punch": layered buy / sell rules where several kinds of evidence must agree.

User idea (2026-10-19): one signal is not enough. A W bottom without money flowing in just keeps basing; buy
only when the breakout is real and confirmed. Sell only when several sell signals agree the trend has really
turned, so strong stocks can be held for a long time (like OBVS). Rules are FIXED in advance, shared by every
stock (no per-stock tuning), and every layer has a market-theory reason:

BUY = environment AND trigger AND money AND (≥ 1 other confirmation)
  environment  close > MA60 and MA60 rising (Weinstein stage 2 / Dow primary uptrend) and the market is not
               in a downtrend (indicators mkt_regime ≠ −1)
  trigger      a real breakout in the last 3 sessions: 20/55-day high, W bottom neckline, N-shape higher high,
               down-trendline break, MA / Bollinger squeeze breakout, breakaway gap (Edwards & Magee)
  money        volume confirms in the last 5 sessions: OBV 60-day high, OBV 20/60 golden cross, volume ratio
               surge, dry-up then expansion (Granville OBV; "price up on rising volume")
  other        in the last 10 sessions (revenue: 40): institutions / big holders buying (chips), relative
               strength new high (O'Neil RS), monthly revenue at a high / accelerating (CANSLIM C/A)
SELL (decided at a close, executed next open) = any of
  breakdown    structure broken (lower high / lower low, N-shape down, M top, up-trendline break, MA bear
               alignment, chandelier 22-day high − 2.5 ATR) in the last 5 sessions
               AND confirmation in the last 5 sessions: money leaving (OBV 20/60 death cross, price-volume
               divergence, foreign selling 5 days, big holders dispersing, short-sale / margin surge) or the
               environment worsening (market turns bearish, relative strength 60-day low, revenue YoY < 0)
  stage 4      close < MA60 and MA60 falling (Weinstein: never hold in stage 4)
  stop         close ≤ entry × 0.90 (O'Neil-style loss cut; from the ENTRY price only, never trailing)
Overheating (RSI / KD high, Bollinger upper fail, long upper shadow) and short-term momentum (MACD death
cross, close < MA20) are NOT exits: strong trends stay "overbought" for months; they only warn.

Variants (reference, not chosen on results): buy without environment / without 'other'; sell without the
10% stop; sell on breakdown alone. Baselines: the single-signal Donchian breakout (55-day high → 20-day low)
and the OBVS script defaults (mode 5, no take-profit).
Execution: close → next open, one position per stock, fees + tax, cap 500 sessions, excess = net − equal-weight
in-universe (top 500 turnover, point-in-time) index over the same days. Optional monthly top-100 ranking filter.

  python backend/tuning/research_combo.py --source parquet --universe 500 --years 8 --adjust-source exchange
"""
import argparse
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
if __package__ in (None, ""):
    __package__ = "tuning"

from tuning import evaluate as ev_mod  # noqa: E402
from tuning import indicators as ind  # noqa: E402
from tuning import obvs  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402
from tuning.evaluate import Costs  # noqa: E402
from tuning.research_rank import EVERY, LOOK, scores_at, wide  # noqa: E402
from tuning.research_style import trades  # noqa: E402
from tuning.run_tuning import add_common_args, prepare  # noqa: E402

CAP = 500
RANK_TOP = 100
STOP = 0.10
TRIGGER = ("brk_20", "brk_55", "pat_w_bottom", "pat_n_up", "tl_down_break", "ma_squeeze_brk", "bb_squeeze_brk",
           "gap_up")
MONEY = ("obv_high", "vol_20_up", "vol_15_up", "dry_expand")                      # + OBV golden cross
CHIPS = ("foreign_streak_3", "foreign_streak_5", "trust_streak_3", "trust_streak_5", "trust_first",
         "chip_streak_3", "chip_streak_5", "chip_p80", "chip_p90", "whale_conc", "whale_400_jump", "margin_drop_up")
RS = ("rs_high", "rs_lead", "rs_resilient")
REV = ("rev_high12", "rev_yoy_accel", "rev_yoy_3up")
BREAK = ("s_pat_lh", "s_pat_ll", "s_pat_bear_struct", "s_pat_n_down", "s_pat_m_top", "s_tl_up_break",
         "s_ma_bear_align", "s_atr_25")
OUTFLOW = ("s_vol_div", "s_foreign_sell_5", "s_whale_disp", "s_sbl_surge", "s_margin_surge")  # + OBV death cross
WORSE = ("s_mkt_turn_bear", "s_rs_low", "s_rev_yoy_neg")
PERIODS = (("2020–2021", "20200101", "20211231"), ("2022–2023", "20220101", "20231231"),
           ("2024–2026", "20240101", "20991231"))
BUYS = {"main": "環境＋突破＋資金＋另一項確認", "no_env": "不看環境（突破＋資金＋另一項確認）",
        "no_other": "環境＋突破＋資金（不要求籌碼／強度／營收）", "trigger_only": "只要突破（單一訊號，對照組）"}
SELLS = {"main": "結構破壞＋（資金流出或環境轉壞）／第四階段／停損 10%", "no_stop": "同左但不設 10% 停損",
         "break_only": "結構破壞就賣（不等確認）"}


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--universe", type=int, default=500)
    ap.add_argument("--years", type=float, default=8.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--report", default=os.path.join(OUT_DIR, "research_combo.md"))
    add_common_args(ap)
    ap.set_defaults(no_holdings=True)
    return ap.parse_args(argv)


def recent(mask, w):
    return pd.Series(np.asarray(mask, float)).rolling(w, min_periods=1).max().to_numpy() > 0


def any_of(sigs, keys, n):
    m = np.zeros(n, bool)
    for k in keys:
        if k in sigs:
            m |= sigs[k].to_numpy(bool)
    return m


def masks(g):
    """Buy / sell masks for every variant on one stock."""
    f = ind.features(g)
    buys, sells = ind.buy_signals(f), ind.sell_signals(f)
    n = len(g)
    c = g["aclose"].to_numpy(float)
    ma60s = pd.Series(c).rolling(60, min_periods=60).mean()
    ma60, ma60_prev = ma60s.to_numpy(), ma60s.shift(20).to_numpy()
    with np.errstate(invalid="ignore"):
        stage2 = (c > ma60) & (ma60 > ma60_prev)
        stage4 = (c < ma60) & (ma60 < ma60_prev)
    mkt = f["mkt_regime"].fillna(0).to_numpy() if "mkt_regime" in f else np.zeros(n)
    env = stage2 & (mkt != -1)
    S = obvs.Series(g)
    bull = S.ema(20) > S.ema(60)
    prev = np.r_[False, bull[:-1]]
    trig = recent(any_of(buys, TRIGGER, n), 3)
    money = recent(any_of(buys, MONEY, n) | (bull & ~prev), 5)
    other = (recent(any_of(buys, CHIPS, n), 10) | recent(any_of(buys, RS, n), 10)
             | recent(any_of(buys, REV, n), 40))
    states = {"main": env & trig & money & other, "no_env": trig & money & other,
              "no_other": env & trig & money, "trigger_only": any_of(buys, TRIGGER, n)}
    buy = {k: v & ~np.r_[False, v[:-1]] if k != "trigger_only" else v for k, v in states.items()}   # first day
    brk = recent(any_of(sells, BREAK, n), 5)
    conf = recent(any_of(sells, OUTFLOW, n) | (~bull & prev), 5) | recent(any_of(sells, WORSE, n), 5)
    base = (brk & conf) | stage4
    sell = {"main": base, "no_stop": base, "break_only": brk | stage4}
    # warnings only (not exits): how often they fired while the main rule kept holding
    warn = any_of(sells, ("s_rsi_p80", "s_rsi_p90", "s_kd_x_80", "s_bb_upper_fail", "s_shoot_high",
                          "s_macd_down", "s_ma20_break"), n)
    return buy, sell, f["atr"].to_numpy(float), S, warn


def stock_job(a):
    sid, g, inu_mask, rank_mask, bench_idx, etf, cp = a
    g = g.reset_index(drop=True)
    buy, sell, atr, S, warn = masks(g)
    ao, ac = S.o, S.c
    dates = g["date"].astype(str).to_numpy()
    costs, nn, out = Costs(), len(cp), {}

    def add(key, rows, keep=None):
        if not rows:
            return
        r = np.array(rows, dtype=float)
        if keep is not None:
            r = r[keep(r)]
            if not len(r):
                return
        e, x = r[:, 1].astype(int), r[:, 2].astype(int)
        net = costs.net(r[:, 3], etf)
        out[key] = {"d": dates[e], "p": cp[e], "net": net, "xs": net - (bench_idx[x] / bench_idx[e] - 1),
                    "held": r[:, 4], "warn": [int(warn[a_:b_ + 1].sum()) for a_, b_ in zip(e, x)]}

    for bk, bm in buy.items():
        for sk, sm in sell.items():
            for filt, em in (("all", inu_mask), ("rank", inu_mask & rank_mask)):
                rows = stop_trades(bm, sm, ao, ac, S.ok, em, STOP if sk != "no_stop" else 0.0)
                add((bk, sk, filt), rows)
    # baselines
    hi55 = pd.Series(S.h).rolling(55).max().shift(1).to_numpy()
    lo20 = pd.Series(S.l).rolling(20).min().shift(1).to_numpy()
    with np.errstate(invalid="ignore"):
        dc_b, dc_s = ac > hi55, ac < lo20
    for filt, em in (("all", inu_mask), ("rank", inu_mask & rank_mask)):
        add(("donchian", "", filt), trades(dc_b, dc_s, ao, ac, S.ok, atr, 250, None, em))
        rows = obvs.trades(S, dict(obvs.DEFAULT, tp=0.0))
        add(("obvs", "", filt), rows, keep=lambda r, em=em: em[r[:, 0].astype(int)])
    return out


def stop_trades(buy, sell, ao, ac, ok, entry_ok, stop):
    """One position; exit decided at the first close with sell, close ≤ entry × (1 − stop), or the cap."""
    n = len(ao)
    b_idx = np.flatnonzero(buy & entry_ok)
    rows, k = [], 0
    while k < len(b_idx):
        t = b_idx[k]
        e = t + 1
        if e >= n:
            break
        if not ok[e] or (np.isfinite(ac[t]) and ao[e] >= ac[t] * 1.095):
            k += 1
            continue
        entry = ao[e]
        seg = sell[e:min(n, e + CAP)].copy()
        if stop > 0:
            seg |= ac[e:min(n, e + CAP)] <= entry * (1 - stop)
        hit = np.flatnonzero(seg)
        dec = e + hit[0] if len(hit) else min(n, e + CAP - 1)
        x = dec + 1
        while x < n and not ok[x]:
            x += 1
        if x >= n:
            rows.append((t, e, n - 1, ac[n - 1] / entry - 1, n - e, True))
            break
        rows.append((t, e, x, ao[x] / entry - 1, dec - e + 1, False))
        k = np.searchsorted(b_idx, x)
    return rows


def main(argv=None):
    a = parse_args(argv)
    t0 = time.time()
    prep = prepare(a, a.universe, a.years)
    cal = list(prep["calendar"])
    pos = {d: i for i, d in enumerate(cal)}
    adj, inu = prep["adjusted"], prep["inu"]
    inu_set = set(zip(inu["stock_id"], inu["date"]))
    rets = []
    for sid, g in adj.items():
        o, d = g["aopen"].to_numpy(float), g["date"].to_numpy()
        keep = np.array([(sid, x) in inu_set for x in d])
        rets.append(pd.DataFrame({"date": d[keep], "r": np.r_[o[1:] / o[:-1] - 1, np.nan][keep]}))
    br = pd.concat(rets).groupby("date")["r"].mean().reindex(cal).fillna(0.0).clip(-0.2, 0.2)
    bidx_cal = np.r_[1.0, np.cumprod(1 + br.to_numpy())[:-1]]
    P = {"open": wide(adj, "aopen", cal), "close": wide(adj, "aclose", cal),
         "rev_yoy": wide(adj, "rev_yoy", cal), "pb": wide(adj, "pb", cal)}
    P["close_ff"] = P["close"].ffill()
    cols = P["close"].columns
    M = (inu.assign(v=True).pivot_table(index="date", columns="stock_id", values="v", aggfunc="any")
         .reindex(index=cal, columns=cols).fillna(False).astype(bool))
    in_rank = pd.DataFrame(False, index=range(len(cal)), columns=cols)
    for t in range(LOOK, len(cal), EVERY):
        sc = scores_at(t, P, {t: M.iloc[t]})["combo"].dropna()
        if len(sc) >= 2 * RANK_TOP:
            in_rank.loc[t:t + EVERY - 1, sc.sort_values(ascending=False).index[:RANK_TOP]] = True
    jobs = []
    for sid, g in adj.items():
        g = g.reset_index(drop=True)
        d = g["date"].to_numpy()
        m = np.array([(sid, x) in inu_set for x in d])
        if not m.any():
            continue
        cp = np.array([pos[x] for x in d])
        rk = in_rank[sid].to_numpy()[cp] if sid in in_rank else np.zeros(len(d), bool)
        jobs.append((sid, g, m, rk, bidx_cal[cp], prep["meta"][sid]["etf"], cp))
    log(f"[combo] {len(jobs)} stocks prepared in {time.time() - t0:.0f}s")
    res = {}
    with ProcessPoolExecutor(a.workers) as ex:
        for out in ex.map(stock_job, jobs, chunksize=4):
            for k, v in out.items():
                res.setdefault(k, []).append(pd.DataFrame(v))
    res = {k: pd.concat(v, ignore_index=True) for k, v in res.items()}
    log(f"[combo] simulated in {time.time() - t0:.0f}s")

    def cells(df):
        if df is None or len(df) < 20:
            return [f"{0 if df is None else len(df)}", "–", "–", "–", "–", "–"]
        t, _ = ev_mod.t_cluster(df["xs"].to_numpy(), (df["p"] // ev_mod.CLUSTER_DAYS).to_numpy())
        per100 = df["xs"].sum() / df["held"].sum() * 100
        return [f"{len(df):,}", f"{100 * df['xs'].mean():+.2f}%", f"{100 * (df['xs'] > 0).mean():.0f}%", f"{t:+.1f}",
                f"{df['held'].mean():.0f}", f"{100 * per100:+.2f}%"]

    def per_period(df):
        out = []
        for _, lo, hi in PERIODS:
            sub = df[(df["d"] >= lo) & (df["d"] <= hi)] if df is not None else None
            if sub is None or len(sub) < 20:
                out.append("–")
                continue
            t, _ = ev_mod.t_cluster(sub["xs"].to_numpy(), (sub["p"] // ev_mod.CLUSTER_DAYS).to_numpy())
            out.append(f"{100 * sub['xs'].mean():+.2f}% / {t:+.1f} / {len(sub):,}")
        return out

    rows_spec = [(("main", "main"), "**組合拳主要版本**")]
    rows_spec += [((bk, "main"), f"買：{BUYS[bk]}") for bk in BUYS if bk != "main"]
    rows_spec += [(("main", sk), f"賣：{SELLS[sk]}") for sk in SELLS if sk != "main"]
    rows_spec += [(("trigger_only", "break_only"), "單一訊號買、單一訊號賣（對照）"),
                  (("donchian", ""), "對照：突破 55 日高買、跌破 20 日低賣"),
                  (("obvs", ""), "對照：OBVS 預設（模式 5，不設停利）")]
    L = [f"# 組合拳回測（{cal[0]}–{cal[-1]}）", "",
         "所有股票共用同一套事先寫好的規則（沒有替任何一檔調參數），股票池為每個時點的成交值前 500 大。"
         "收盤判斷、隔天開盤成交，扣手續費（6 折）與證交稅。超額 = 報酬 − 同期間股票池平均。", "",
         "**買進** = 環境（站上上升的季線、大盤非空頭）＋ 近 3 天真突破 ＋ 近 5 天資金確認 ＋ 籌碼／相對強度／營收至少一項。",
         "**賣出** = 結構破壞 ＋（資金流出 或 環境轉壞），或跌破下彎的季線，或從買價跌 10%。過熱、短線轉弱只提醒、不賣。", ""]
    hdr = ["| 規則 | 筆數 | 每筆平均超額 | 勝率(贏大盤) | t | 平均持有日 | 每 100 持有日超額 | "
           + " | ".join(f"{p}（超額 / t / 筆數）" for p, _, _ in PERIODS) + " |",
           "|---|---|---|---|---|---|---|" + "---|" * len(PERIODS)]
    for filt, title in (("all", "全部股票"), ("rank", "只做當月「強勢股＋營收成長」前 100 名")):
        L += [f"## {title}", ""] + hdr
        for (bk, sk), nm in rows_spec:
            df = res.get((bk, sk, filt))
            L.append(f"| {nm} | " + " | ".join(cells(df) + per_period(df)) + " |")
        L.append("")
        mm = res.get(("main", "main", filt))
        if mm is not None and len(mm):
            L.append(f"主要版本持有期間，「過熱／短線轉弱」警示平均出現 {np.mean(mm['warn']):.1f} 次（照規則不賣）。")
            L.append("")
    L += ["t 值以 20 日區塊分群，|t| ≥ 2 才算明顯。「每 100 持有日超額」× 2.5 ≈ 資金全年在場內時一年多賺多少。", ""]
    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    log(f"[combo] report → {a.report} in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
