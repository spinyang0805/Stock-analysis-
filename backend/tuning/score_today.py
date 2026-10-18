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
from tuning.data import OUT_DIR, log  # noqa: E402
from tuning.run_tuning import prepare  # noqa: E402

DEFAULT_SELL = ("s_atr_25", 10)


def tick(price):
    """Taiwan stock tick size."""
    for lim, t in ((10, 0.01), (50, 0.05), (100, 0.1), (500, 0.5), (1000, 1.0)):
        if price < lim:
            return t
    return 5.0


def rnd(x, mode="near"):
    if x is None or not np.isfinite(x) or x <= 0:
        return None
    t = tick(x)
    f = {"near": np.round, "up": np.ceil, "down": np.floor}[mode]
    return round(float(f(x / t) * t), 2)


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--params", default=os.path.join(OUT_DIR, "stock_params.json"))
    ap.add_argument("--out-dir", default=OUT_DIR)
    ap.add_argument("--as-of", default=None, help="YYYYMMDD: score as of this date (ignore later rows)")
    ap.add_argument("--years", type=float, default=1.8, help="history needed for indicators/percentiles")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--include-unreliable", action="store_true",
                    help="also list stocks labelled 'no reliable signal' (flagged; for diagnostics only)")
    ap.add_argument("--holdings", default=None)
    ap.add_argument("--no-holdings", action="store_true")
    ap.add_argument("--finmind-budget", type=int, default=30)
    ap.add_argument("--finmind-interval", type=float, default=2.0)
    ap.add_argument("--finmind-offline", action="store_true")
    return ap.parse_args(argv)


def intraday_levels(f, sell_mult):
    c = f["close"].to_numpy()
    last = f.iloc[-1]
    hh20 = f["high"].iloc[-20:].max() if len(f) >= 20 else np.nan
    hh55 = f["high"].iloc[-55:].max() if len(f) >= 55 else np.nan
    trail = last["hc22"] - sell_mult * last["atr"]
    ma20_next = c[-19:].sum() / 19 if len(c) >= 19 else np.nan   # x < this ⇒ close below tomorrow's MA20
    return {
        "breakout_20": rnd(hh20, "up"), "breakout_55": rnd(hh55, "up"),
        "ma5_over_ma20": rnd(ind.ma_cross_level(c, 5, 20), "up"),
        "ma20_over_ma60": rnd(ind.ma_cross_level(c, 20, 60), "up"),
        "bb_lower_touch": rnd(ind.bb_lower_level(c), "down"),
        "ma20_break": rnd(ma20_next, "down"),
        "trailing_stop": rnd(trail, "down"),
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
    if dq.get("finmind") == "unavailable":
        fl.append("除權息資料未取得")
    if p.get("short_history"):
        fl.append("歷史過短")
    return fl


def run(args):
    with open(args.params, encoding="utf-8") as f:
        pj = json.load(f)
    params = {p["stock_id"]: p for p in pj["stocks"]}
    prep = prepare(args, 0, args.years, codes=list(params), end_date=args.as_of)
    adjusted, holdings = prep["adjusted"], set(prep["holdings"])
    last_date = max(g["date"].iloc[-1] for g in adjusted.values())
    buys_out, sells_out, levels = [], [], {}
    for sid, g in adjusted.items():
        p = params.get(sid)
        f = ind.features(g)
        buys, sells = ind.buy_signals(f), ind.sell_signals(f)
        last = f.iloc[-1]
        sell_sig, sell_h = (p["sell_rule"]["signal"], p["sell_rule"]["h"]) if p else DEFAULT_SELL
        mult = ind.ATR_MULT.get(sell_sig, 2.5)
        levels[sid] = intraday_levels(f, mult)
        fresh = g["date"].iloc[-1] == last_date
        flags = quality_flags(p or {}, g, last_date)
        close = float(g["close"].iloc[-1])          # raw (adj_factor of the last row is 1)
        # ── BUY ──
        if p and fresh and (p["reliable"] or args.include_unreliable):
            fired = [s for s in p["buy_signals"] if s["signal"] in buys and bool(buys[s["signal"]].iloc[-1])]
            if fired:
                risk = abs(p.get("oos_mae_med") or 0.0) or 0.03
                exp_ret = p.get("oos_mean")
                exp_ret = exp_ret if exp_ret is not None and np.isfinite(exp_ret) else 0.0
                score = exp_ret / max(risk, 0.01) * (p["confidence"] if p["reliable"] else 0.0)
                atr_stop = close - p.get("stop_atr_mult", 2.5) * float(last["atr"])
                mae_stop = close * (1 + p["mae_p20"]) if p.get("mae_p20") is not None else None
                buys_out.append({
                    "stock_id": sid, "name": p["name"], "reliable": p["reliable"], "level": p["level"],
                    "signals": [{"signal": s["signal"], "desc": s["desc"], "h": s["h"]} for s in fired],
                    "trigger_price": rnd(close), "entry": "次日開盤（漲停開出不追）",
                    "stop_loss": rnd(atr_stop, "down"), "stop_mae_p20": rnd(mae_stop, "down"),
                    "take_profit": rnd(close * (1 + p["tp_pct"]), "up") if p.get("tp_pct") else None,
                    "hold_days": fired[0]["h"], "oos_n": p["oos_n"], "oos_win": p["oos_win"],
                    "oos_mean": p["oos_mean"], "oos_xnet": p.get("oos_xnet"), "oos_mae_med": p.get("oos_mae_med"),
                    "confidence": p["confidence"], "score": score, "flags": flags,
                    "holding": sid in holdings,
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
            sells_out.append({
                "stock_id": sid, "name": (p or {}).get("name", ""), "close": rnd(close),
                "alert": bool(reasons), "reasons": reasons,
                "sell_rule": {"signal": sell_sig, "h": sell_h, "desc": ind.SELL_DESC[sell_sig],
                              "oos_n": (p or {}).get("sell_rule", {}).get("oos_n")},
                "trailing_stop": levels[sid]["trailing_stop"], "ma20_break": levels[sid]["ma20_break"],
                "take_profit_pct": tp, "flags": flags,
            })
    buys_out.sort(key=lambda r: (not r["reliable"], -r["score"], -(r["oos_mean"] or 0)))
    out = {"as_of": last_date, "generated_at": datetime.now().isoformat(timespec="seconds"),
           "params_generated_at": pj.get("generated_at"), "include_unreliable": args.include_unreliable,
           "n_scored": len(adjusted), "n_reliable_params": sum(1 for p in params.values() if p["reliable"]),
           "buy": buys_out, "sell_alerts": sells_out, "intraday_levels": levels}
    os.makedirs(args.out_dir, exist_ok=True)
    jp = os.path.join(args.out_dir, f"recommendations_{last_date}.json")
    with open(jp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1, default=lambda o: float(o) if isinstance(o, np.floating) else str(o))
    md = render_md(out, args.top)
    with open(os.path.join(args.out_dir, f"recommendations_{last_date}.md"), "w", encoding="utf-8") as f:
        f.write(md)
    log(f"[score] {len(buys_out)} buy candidates, {sum(s['alert'] for s in sells_out)}/{len(sells_out)} holding alerts → {jp}")
    return out


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
        L.append("| # | 代號 | 名稱 | 訊號 | 觸發價 | 停損 | 停利 | 持有日 | OOS 勝率 | OOS n | OOS 平均淨 | 信心 | 註記 |")
        L.append("|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
        for i, b in enumerate(out["buy"][:top], 1):
            flags = list(b["flags"]) + ([] if b["reliable"] else ["未通過驗證"])
            L.append(f"| {i} | {b['stock_id']} | {b['name']} | {'、'.join(s['desc'] for s in b['signals'])} | "
                     f"{b['trigger_price']} | {b['stop_loss'] or '–'} | {b['take_profit'] or '–'} | {b['hold_days']} | "
                     f"{_p(b['oos_win'], 0)} | {b['oos_n']} | {_p(b['oos_mean'])} | {b['confidence']:.2f} | "
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
