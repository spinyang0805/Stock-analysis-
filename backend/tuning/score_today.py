"""score_today.py — daily recommender from saved per-stock params (no DB writes).

  python backend/tuning/score_today.py --source parquet
  python backend/tuning/score_today.py --source db --include-unreliable   (diagnostic, clearly flagged)

Reads history/out/stock_params.json (run_tuning output), rebuilds indicators on the latest data and emits
  history/out/recommendations_YYYYMMDD.json   BUY candidates, SELL alerts (holdings), next-day intraday levels
  history/out/recommendations_YYYYMMDD.md     concise zh-TW numeric summary (email / Telegram)
Prices shown to the user are RAW (the latest adjusted price equals the raw price by construction).
"""
import argparse
import json
import os
import sys
from datetime import datetime

import numpy as np

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "tuning"

from tuning import indicators as ind  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402,F401
from tuning.run_tuning import add_common_args, json_safe, prepare  # noqa: E402

DEFAULT_SELL = ("s_atr_25", 10)


def tick(price, etf=False):
    """Taiwan tick size (stocks: 0.01 … 5; ETFs / ETNs: 0.01 below 50, 0.05 from 50)."""
    if etf:
        return 0.01 if price < 50 else 0.05
    for lim, t in ((10, 0.01), (50, 0.05), (100, 0.1), (500, 0.5), (1000, 1.0)):
        if price < lim:
            return t
    return 5.0


def rnd(x, mode="near", etf=False):
    """Round to a valid tick. mode 'above' = the first tick STRICTLY above x (breakout trigger prices)."""
    if x is None or not np.isfinite(x) or x <= 0:
        return None
    t = tick(x, etf)
    if mode == "above":
        return round(float((np.floor(x / t + 1e-9) + 1) * t), 2)
    f = {"near": np.round, "up": np.ceil, "down": np.floor}[mode]
    return round(float(f(x / t - (1e-9 if mode == "up" else 0)) * t), 2)


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--params", default=os.path.join(OUT_DIR, "stock_params.json"))
    ap.add_argument("--as-of", default=None, help="YYYYMMDD: score as of this date (ignore later rows)")
    ap.add_argument("--years", type=float, default=None,
                    help="history loaded for indicators (default: the tuning run's own --years, so valuation "
                         "percentiles and zigzag state are rebuilt on the same history as in tuning)")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--buy-mode", choices=["uniform", "stock"], default="uniform",
                    help="uniform (default, decided 2026-10-19): BUY candidates from the market-wide signal set; "
                         "stock: each stock's own tuned signals. Exits / stops always use the stock's own params.")
    ap.add_argument("--include-unreliable", action="store_true",
                    help="also list stocks labelled 'no reliable signal' (flagged; for diagnostics only)")
    add_common_args(ap)
    ap.set_defaults(finmind_budget=30)
    return ap.parse_args(argv)


def intraday_levels(f, sell_mult, etf=False):
    c = f["close"].to_numpy()
    last = f.iloc[-1]
    hh20 = f["high"].iloc[-20:].max() if len(f) >= 20 else np.nan
    hh55 = f["high"].iloc[-55:].max() if len(f) >= 55 else np.nan
    trail = last["hc22"] - sell_mult * last["atr"]
    ma20_next = c[-19:].sum() / 19 if len(c) >= 19 else np.nan   # x < this ⇒ close below tomorrow's MA20
    ma20 = f["close"].rolling(20).mean()
    ma60 = f["close"].rolling(60).mean()
    vol20 = f["volume"].iloc[-20:].mean() if len(f) >= 20 else np.nan
    # base values for the event-based intraday alerts (public.intraday_context); last row is on the raw scale
    ctx = {"prev_close": fin(float(c[-1])), "ma20": fin(float(ma20.iloc[-1])), "ma60": fin(float(ma60.iloc[-1])),
           "ma20_up": bool(len(ma20) > 5 and ma20.iloc[-1] > ma20.iloc[-6]),
           "atr": fin(float(last["atr"])), "avg_vol20": fin(float(vol20) / 1000),   # shares → lots
           "hh20": fin(float(hh20)), "ll20": fin(float(f["low"].iloc[-20:].min())) if len(f) >= 20 else None}
    return {
        "ctx": ctx,
        "breakout_20": rnd(hh20, "above", etf), "breakout_55": rnd(hh55, "above", etf),
        "ma5_over_ma20": rnd(ind.ma_cross_level(c, 5, 20), "above", etf),
        "ma20_over_ma60": rnd(ind.ma_cross_level(c, 20, 60), "above", etf),
        "bb_lower_touch": rnd(ind.bb_lower_level(c), "down", etf),
        "ma20_break": rnd(ma20_next, "down", etf),
        "trailing_stop": rnd(trail, "down", etf),
        "note": "價格為下一交易日盤中觸發價（以今日收盤後資料推算；突破類以收盤確認為準）",
    }


def quality_flags(p, g, last_date):
    dq = p.get("data_quality", {})
    fl = []
    if g["date"].iloc[-1] != last_date:
        fl.append(f"資料停在 {g['date'].iloc[-1]}")
    if bool(g["yf"].iloc[-20:].any()):
        fl.append("近 20 日含 yfinance 補值")
    if dq.get("unexplained_gaps") or dq.get("markers_unexplained"):
        fl.append("還原權值有未解釋缺口")
    if dq.get("adj_source") == "unavailable":
        fl.append("除權息資料未取得")
    if p.get("short_history"):
        fl.append("歷史過短")
    return fl


def run(args, extra_codes=(), return_prep=False):
    with open(args.params, encoding="utf-8") as f:
        pj = json.load(f)
    params = {p["stock_id"]: p for p in pj["stocks"]}
    p_asof = max((p.get("as_of") or "" for p in params.values()), default="")
    if args.as_of and args.as_of < p_asof:
        raise SystemExit(f"--as-of {args.as_of} is earlier than the params as_of {p_asof}: the params were tuned on "
                         "later data, so this would not be a valid backtest (run run_tuning --end-date first).")
    codes = list(dict.fromkeys(list(params) + [c for c in extra_codes if c]))   # + tracked picks
    years = args.years or float((pj.get("config") or {}).get("years") or 5.0)
    prep = prepare(args, 0, years, codes=codes, end_date=args.as_of)
    adjusted, holdings = prep["adjusted"], set(prep["holdings"])
    last_date = max(g["date"].iloc[-1] for g in adjusted.values())
    buys_out, sells_out, levels = [], [], {}
    uni = pj.get("uniform") or {}
    uni_sigs = uni.get("signals") or []
    if not uni_sigs:                      # older params files: take the set from a stock assigned to 'uniform'
        u = next((p for p in params.values() if p.get("level") == "uniform" and p.get("buy_signals")), None)
        uni_sigs = [{"signal": s["signal"], "h": s["h"], "desc": s["desc"]} for s in (u or {}).get("buy_signals", [])]
    if args.buy_mode == "uniform":
        log(f"[score] buy mode = market-wide signals: {[(s['signal'], s['h']) for s in uni_sigs]}")
    for sid, g in adjusted.items():
        fired = []
        p = params.get(sid)
        f = ind.features(g)
        buys, sells = ind.buy_signals(f), ind.sell_signals(f)
        last = f.iloc[-1]
        sell_sig, sell_h = (p["sell_rule"]["signal"], p["sell_rule"]["h"]) if p else DEFAULT_SELL
        mult = ind.ATR_MULT.get(sell_sig, 2.5)
        etf = str(sid).startswith("00")
        levels[sid] = intraday_levels(f, mult, etf)
        fresh = g["date"].iloc[-1] == last_date
        flags = quality_flags(p or {}, g, last_date)
        close = float(g["close"].iloc[-1])          # raw (adj_factor of the last row is 1)
        # ── BUY ──
        # uniform mode: every current-universe stock is scanned for the MARKET-WIDE signal set (pooled stats);
        # stock mode: each stock's own tuned signals (only reliable ones unless --include-unreliable)
        if args.buy_mode == "uniform":
            eligible = bool(p and fresh and p.get("in_universe"))
            sig_list = uni_sigs
        else:
            eligible = bool(p and fresh and (p["reliable"] or args.include_unreliable))
            sig_list = p["buy_signals"] if p else []
        if eligible:
            fired = [s for s in sig_list if s["signal"] in buys and bool(buys[s["signal"]].iloc[-1])]
        if eligible and fired and args.buy_mode == "uniform":
            atr = fin(float(last["atr"]))
            stop_pct = -p.get("stop_atr_mult", 2.5) * atr / close if atr else None
            # rank: more market-wide signals firing together first, then the best pooled per-day excess
            score = len(fired) + max((fin(s.get("pooled_mean_xpd")) or 0.0) for s in fired) * 100
            uo = uni.get("oos") or {}
            buys_out.append({
                "stock_id": sid, "name": p["name"], "reliable": False, "level": "uniform",
                "signals": [{"signal": s["signal"], "desc": s["desc"], "h": s["h"]} for s in fired],
                "ref_close": rnd(close, "near", etf),
                "entry": "次日開盤市價（開盤漲幅 >= 9.5% 不追）；停損以實際成交價乘上百分比",
                "stop_pct": stop_pct, "take_profit_pct": None, "mae_p20_pct": fin(p.get("mae_p20")),
                "stop_loss_if_fill_at_ref": rnd(close * (1 + stop_pct), "down", etf) if stop_pct else None,
                "take_profit_if_fill_at_ref": None,
                "hold_days": fired[0]["h"], "hold_days_unit": "該股交易日（列數）",
                # market-wide record (all universe stocks), not this stock's own history
                "oos_n": uo.get("n"), "oos_win": uo.get("win"), "oos_mean": uo.get("mean"),
                "oos_xnet": uo.get("xnet"), "oos_mae_med": p.get("oos_mae_med"),
                "confidence": 0.0, "score": score, "flags": flags + ["全市場統一訊號"],
                "holding": sid in holdings, "sell_rule": p.get("sell_rule"),
            })
            fired = []                                   # handled
        if eligible and fired:
            if fired:
                mae = fin(p.get("oos_mae_med"))
                risk = abs(mae) if mae else 0.03
                exp_ret = fin(p.get("oos_deployed_mean") if p.get("oos_deployed_n") else p.get("oos_mean")) or 0.0
                score = exp_ret / max(risk, 0.01) * ((fin(p.get("confidence")) or 0.0) if p["reliable"] else 0.0)
                atr = fin(float(last["atr"]))
                stop_pct = -p.get("stop_atr_mult", 2.5) * atr / close if atr else None   # from the actual fill
                tp_pct = fin(p.get("tp_pct"))
                buys_out.append({
                    "stock_id": sid, "name": p["name"], "reliable": p["reliable"], "level": p["level"],
                    "signals": [{"signal": s["signal"], "desc": s["desc"], "h": s["h"]} for s in fired],
                    "ref_close": rnd(close, "near", etf),
                    "entry": "次日開盤市價（開盤漲幅 >= 9.5% 不追）；停損停利以實際成交價乘上百分比",
                    "stop_pct": stop_pct, "take_profit_pct": tp_pct, "mae_p20_pct": fin(p.get("mae_p20")),
                    "stop_loss_if_fill_at_ref": rnd(close * (1 + stop_pct), "down", etf) if stop_pct else None,
                    "take_profit_if_fill_at_ref": rnd(close * (1 + tp_pct), "up", etf) if tp_pct else None,
                    "hold_days": fired[0]["h"], "hold_days_unit": "該股交易日（列數）",
                    # deployed-policy OOS record (the exits the tracker uses) when available, else fixed-horizon
                    "oos_n": p.get("oos_deployed_n") or p["oos_n"],
                    "oos_win": p.get("oos_deployed_win") if p.get("oos_deployed_n") else p["oos_win"],
                    "oos_mean": p.get("oos_deployed_mean") if p.get("oos_deployed_n") else p["oos_mean"],
                    "oos_xnet": p.get("oos_deployed_x") if p.get("oos_deployed_n") else p.get("oos_xnet"),
                    "oos_mae_med": p.get("oos_mae_med"),
                    "confidence": p["confidence"], "score": score, "flags": flags,
                    "holding": sid in holdings, "sell_rule": p.get("sell_rule"),
                })
        # ── SELL (holdings) ──
        if sid in holdings:
            reasons = []
            rule_fired = sell_sig in sells and bool(sells[sell_sig].iloc[-1])
            if rule_fired:
                reasons.append(f"出場規則觸發：{ind.SELL_DESC[sell_sig]}")
            trail = last["hc22"] - mult * last["atr"]
            if np.isfinite(trail) and close < trail and not (rule_fired and sell_sig in ind.ATR_MULT):
                reasons.append(f"收盤低於追蹤停損 {levels[sid]['trailing_stop']}")
            if bool(sells["s_ma20_break"].iloc[-1]) and sell_sig != "s_ma20_break":
                reasons.append("今日跌破 MA20（參考）")
            if bool(sells["s_rsi_p90"].iloc[-1]) and sell_sig != "s_rsi_p90":
                reasons.append("RSI 高於自身 P90（參考）")
            tp = p.get("tp_pct") if p else None
            nm = (p or {}).get("name") or (str(g["name"].dropna().iloc[-1]).strip()
                                           if "name" in g and g["name"].notna().any() else "")
            sells_out.append({
                "stock_id": sid, "name": nm, "close": rnd(close, "near", etf),
                "alert": bool(reasons), "reasons": reasons,
                "sell_rule": {"signal": sell_sig, "h": sell_h, "desc": ind.SELL_DESC[sell_sig],
                              "oos_n": (p or {}).get("sell_rule", {}).get("oos_n")},
                "trailing_stop": levels[sid]["trailing_stop"], "ma20_break": levels[sid]["ma20_break"],
                "take_profit_pct": tp, "flags": flags,
            })
    buys_out.sort(key=lambda r: (not r["reliable"], -(fin(r["score"]) or 0.0), -(fin(r["oos_mean"]) or 0.0)))
    out = {"as_of": last_date, "generated_at": datetime.now().isoformat(timespec="seconds"),
           "params_generated_at": pj.get("generated_at"), "include_unreliable": args.include_unreliable,
           "fee_discount": (pj.get("config") or {}).get("fee_discount"),
           "buy_mode": args.buy_mode, "uniform": {"signals": uni_sigs, "oos": uni.get("oos"), "holdout": uni.get("holdout")},
           "n_scored": len(adjusted), "n_reliable_params": sum(1 for p in params.values() if p["reliable"]),
           "buy": buys_out, "sell_alerts": sells_out, "intraday_levels": levels}
    os.makedirs(args.out_dir, exist_ok=True)
    jp = os.path.join(args.out_dir, f"recommendations_{last_date}.json")
    with open(jp, "w", encoding="utf-8") as f:
        json.dump(json_safe(out), f, ensure_ascii=False, indent=1, allow_nan=False)
    md = render_md(out, args.top)
    with open(os.path.join(args.out_dir, f"recommendations_{last_date}.md"), "w", encoding="utf-8") as f:
        f.write(md)
    log(f"[score] {len(buys_out)} buy candidates, {sum(s['alert'] for s in sells_out)}/{len(sells_out)} holding alerts → {jp}")
    return (out, prep) if return_prep else out


def fin(x):
    """float(x) if finite else None (NaN / inf / None / non-numeric → None)."""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return x if np.isfinite(x) else None


def _p(x, d=1):
    return "–" if x is None or (isinstance(x, float) and not np.isfinite(x)) else f"{x * 100:.{d}f}%"


def render_md(out, top):
    d = out["as_of"]
    L = [f"# 每日訊號 {d[:4]}-{d[4:6]}-{d[6:]}", ""]
    rel = [b for b in out["buy"] if b["reliable"]]
    L.append(f"參數檔 {out['params_generated_at']}；評分 {out['n_scored']} 檔；可靠參數 {out['n_reliable_params']} 檔；"
             f"今日觸發（可靠）{len(rel)} 檔")
    L.append("")
    L.append("## 買進候選")
    L.append("")
    if not out["buy"]:
        L.append("今日無觸發。")
    else:
        L.append("進場＝次日開盤；停損／停利為相對實際成交價的百分比，括號內為以參考收盤成交時的價位。")
        L.append("")
        L.append("| # | 代號 | 名稱 | 訊號 | 參考收盤 | 停損 | 停利 | 持有日 | OOS 勝率 | OOS n | OOS 平均淨 | 信心 | 註記 |")
        L.append("|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
        for i, b in enumerate(out["buy"][:top], 1):
            flags = list(b["flags"]) + ([] if b["reliable"] else ["未通過驗證"])
            L.append(f"| {i} | {b['stock_id']} | {b['name']} | {'、'.join(s['desc'] for s in b['signals'])} | "
                     f"{b['ref_close']} | {_p(b['stop_pct'])} ({b['stop_loss_if_fill_at_ref'] or '–'}) | "
                     f"{_p(b['take_profit_pct'])} ({b['take_profit_if_fill_at_ref'] or '–'}) | {b['hold_days']} | "
                     f"{_p(b['oos_win'], 0)} | {b['oos_n']} | {_p(b['oos_mean'])} | {fin(b['confidence']) or 0:.2f} | "
                     f"{'；'.join(flags) or ''} |")
    L.append("")
    L.append("## 持股出場警示")
    L.append("")
    al = [s for s in out["sell_alerts"] if s["alert"]]
    if not al:
        L.append(f"持股 {len(out['sell_alerts'])} 檔，今日無警示。")
    else:
        L.append("| 代號 | 名稱 | 收盤 | 原因 | 追蹤停損 |")
        L.append("|---|---|---:|---|---:|")
        for s in al:
            L.append(f"| {s['stock_id']} | {s['name']} | {s['close']} | {'；'.join(s['reasons'])} | {s['trailing_stop'] or '–'} |")
    L.append("")
    L.append("## 持股明日盤中關鍵價")
    L.append("")
    L.append("| 代號 | 追蹤停損 | 跌破 MA20 | 觸及布林下軌 | 突破 20 日高 |")
    L.append("|---|---:|---:|---:|---:|")
    for s in out["sell_alerts"]:
        lv = out["intraday_levels"][s["stock_id"]]
        L.append(f"| {s['stock_id']} | {lv['trailing_stop'] or '–'} | {lv['ma20_break'] or '–'} | "
                 f"{lv['bb_lower_touch'] or '–'} | {lv['breakout_20'] or '–'} |")
    L.append("")
    L.append("回測期間短，數字僅供參考，非投資建議。")
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    run(parse_args())
