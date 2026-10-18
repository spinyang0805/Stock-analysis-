"""daily_picks.py — evening job: score today's signals, advance tracked picks, write the morning report.

  python backend/tuning/daily_picks.py --source parquet [--top 10] [--state PATH]

Writes into history/out/daily/:
  report_YYYYMMDD.md     full report (Email / Teams)
  line_YYYYMMDD.txt      short version (LINE: one push per day, quota-friendly)
  latest.json            {as_of, report, line, generated_at} — what send_daily.py sends at 08:50
and updates the tracking state (default history/out/daily/tracked_picks.json).
While no stock passes the reliability gate the picks are labelled 試行 (trial).
"""
import argparse
import json
import os
import sys
from datetime import datetime

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "tuning"

from tuning import score_today, track  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402

DAILY_DIR = os.path.join(OUT_DIR, "daily")


def pct(x, d=1):
    return "–" if x is None else f"{x * 100:+.{d}f}%"


def fmt_date(d):
    return f"{d[:4]}/{d[4:6]}/{d[6:]}" if d else "–"


def render(out, state, added, closed_today, top):
    d = out["as_of"]
    trial = out["n_reliable_params"] == 0
    L = [f"# 每日股票報告（{fmt_date(d)} 收盤後計算）", ""]
    if trial:
        L += ["> **試行**：目前沒有股票通過可靠度驗證，以下推薦依回測排序，僅供觀察，不構成投資建議。", ""]
    L += ["## 今日買進候選（明日開盤進場）", ""]
    buys = out["buy"][:top]
    if not buys:
        L.append("今日無符合條件的股票。")
    else:
        L.append("| # | 代號 | 名稱 | 訊號 | 參考收盤 | 停損 | 停利 | 持有日 | 回測勝率 | 回測次數 |")
        L.append("|---:|---|---|---|---:|---:|---:|---:|---:|---:|")
        for i, b in enumerate(buys, 1):
            tag = "" if b.get("reliable") else "（試行）"
            L.append(f"| {i} | {b['stock_id']} | {b['name']}{tag} | {'、'.join(s['desc'] for s in b['signals'])} | "
                     f"{b['ref_close']} | {pct(b['stop_pct'])} | {pct(b['take_profit_pct'])} | {b['hold_days']} | "
                     f"{pct(b['oos_win'], 0).lstrip('+')} | {b['oos_n']} |")
    L.append("")
    act = [p for p in state if p["status"] in track.ACTIVE]
    L += ["## 追蹤中（每日評估，直到出場或你賣出）", ""]
    if not act:
        L.append("目前沒有追蹤中的推薦。")
    else:
        L.append("| 代號 | 名稱 | 推薦日 | 進場價 | 收盤 | 報酬(扣成本) | 持有/計畫 | 停損 | 停利 | 建議 | 原因 |")
        L.append("|---|---|---|---:|---:|---:|---:|---:|---:|---|---|")
        order = {"出場": 0, "注意": 1, "續抱": 2, "待進場": 3}
        for p in sorted(act, key=lambda p: order.get(p.get("action"), 9)):
            hold = "✔持有" if p.get("user_holds") else ""
            L.append(f"| {p['stock_id']} | {p['name']}{hold} | {fmt_date(p['pick_date'])} | "
                     f"{p.get('entry_price') or '–'} | {p.get('last_close') or '–'} | {pct(p.get('ret_net'))} | "
                     f"{p.get('days_held') or 0}/{p['plan_hold']} | {p.get('stop_price') or '–'} | "
                     f"{p.get('tp_price') or '–'} | **{p.get('action', '')}** | {p.get('reason', '')} |")
    L.append("")
    if closed_today:
        L += ["## 今日結案", ""]
        for p in closed_today:
            L.append(f"- {p['stock_id']} {p['name']}：{p.get('action')}，{p.get('reason')}"
                     + (f"，報酬 {pct(p.get('ret_net'))}" if p.get("ret_net") is not None else ""))
        L.append("")
    al = [s for s in out["sell_alerts"] if s["alert"]]
    L += ["## 持股出場警示", ""]
    L.append("、".join(f"{s['stock_id']} {s['name']}：{'；'.join(s['reasons'])}" for s in al) if al
             else f"持股 {len(out['sell_alerts'])} 檔，今日無警示。")
    L.append("")
    sm = track.summary(state)
    if sm:
        L += ["## 推薦實績（已結案）", "",
              f"共 {sm['n']} 筆，勝率 {sm['win']:.0%}，平均每筆 {pct(sm['mean'])}（扣手續費與證交稅）", ""]
    L.append("數字來自歷史回測與規則判斷，非投資建議。")
    return "\n".join(L) + "\n"


def render_line(out, state, top, closed_today=()):
    d = out["as_of"]
    trial = out["n_reliable_params"] == 0
    L = [f"📈 {fmt_date(d)} 收盤後{'（試行）' if trial else ''}"]
    buys = out["buy"][:min(top, 5)]
    L.append("【買進候選】" + ("無" if not buys else ""))
    for b in buys:
        tag = "" if b.get("reliable") else "（試行）"
        L.append(f"{b['stock_id']} {b['name']}{tag} {b['ref_close']}｜{b['signals'][0]['desc']}｜停損 {pct(b['stop_pct'])}")
    if closed_today:
        L.append("【今日結案】")
        for p in closed_today:
            L.append(f"{p['stock_id']} {p['name']}：{p.get('action')}－{p.get('reason', '')}"
                     + (f"（{pct(p.get('ret_net'))}）" if p.get("ret_net") is not None else ""))
    imp = [p for p in state if p["status"] in track.ACTIVE and p.get("action") in ("出場", "注意")]
    if imp:
        L.append("【追蹤提醒】")
        for p in imp:
            L.append(f"{p['stock_id']} {p['name']}：{p['action']}－{p.get('reason', '')}")
    al = [s for s in out["sell_alerts"] if s["alert"]]
    if al:
        L.append("【持股警示】")
        for s in al:
            L.append(f"{s['stock_id']} {s['name']}：{'；'.join(s['reasons'])}")
    L.append("完整報告請看 Email")
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--state", default=os.path.join(DAILY_DIR, "tracked_picks.json"))
    ap.add_argument("--write-alerts", action="store_true",
                    help="write tomorrow's intraday alert levels to Supabase (public.alert_levels)")
    known, rest = ap.parse_known_args(argv)
    sargs = score_today.parse_args(rest + ["--include-unreliable", "--out-dir", DAILY_DIR])
    os.makedirs(DAILY_DIR, exist_ok=True)
    state = track.load_state(known.state)
    tracked = [p["stock_id"] for p in state if p["status"] in track.ACTIVE]
    out, prep = score_today.run(sargs, extra_codes=tracked, return_prep=True)
    as_of = out["as_of"]
    holdings = set(prep["holdings_all"]) if prep.get("holdings_ok", True) else None
    if holdings is None:
        log("[daily] holdings query FAILED — no user-sold / user-holds transitions today")
    # evaluation is a deterministic re-simulation → safe to repeat; new picks are opened once per as_of
    from tuning.evaluate import Costs
    costs = Costs(discount=float(out.get("fee_discount") or 0.6))
    closed_today = track.evaluate(state, prep["adjusted"], holdings, as_of, costs)
    closed_today = [p for p in state if p.get("closed_on") == as_of]   # incl. ones closed by an earlier rerun
    if any(p.get("pick_date") == as_of for p in state):
        log(f"[daily] picks for {as_of} already opened — not adding again")
        added = []
    else:
        added = track.open_new(state, out["buy"], as_of, known.top)
    track.save_state(known.state, state)
    report = render(out, state, added, closed_today, known.top)
    line = render_line(out, state, known.top, closed_today)
    rp, lp = os.path.join(DAILY_DIR, f"report_{as_of}.md"), os.path.join(DAILY_DIR, f"line_{as_of}.txt")
    for path, text in ((rp, report), (lp, line)):
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
    with open(os.path.join(DAILY_DIR, "latest.json"), "w", encoding="utf-8") as f:
        json.dump({"as_of": as_of, "report": os.path.basename(rp), "line": os.path.basename(lp),
                   "generated_at": datetime.now().isoformat(timespec="seconds"),
                   "n_buy": len(out["buy"]), "n_tracked": sum(p["status"] in track.ACTIVE for p in state)},
                  f, ensure_ascii=False)
    if known.write_alerts:
        from tuning import alert_levels
        try:
            alert_levels.write(out, state)
        except Exception as exc:          # alerts are best-effort; the morning report must still be produced
            log(f"[daily] alert levels NOT written: {type(exc).__name__}: {str(exc)[:200]}")
    log(f"[daily] {as_of}: {len(added)} new picks, {len(closed_today)} closed, "
        f"{sum(p['status'] in track.ACTIVE for p in state)} active → {rp}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
