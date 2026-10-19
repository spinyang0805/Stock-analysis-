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
    u = out.get("uniform") or {}
    if out.get("buy_mode") == "uniform":
        sig = "、".join(f"{x['desc']}（持有 {x['h']} 日）" for x in u.get("signals", [])) or "–"
        L.append(f"推薦依據：**全市場統一訊號**（{sig}）。出場與停損依各股自己的調參結果。")
        o, hd = u.get("oos") or {}, u.get("holdout") or {}
        if o or hd:
            L.append(f"回測紀錄（扣成本）：模型沒看過的期間 {o.get('n', '–')} 筆，平均 {pct(o.get('mean'))}、"
                     f"勝率 {pct(o.get('win'), 0).lstrip('+')}、比大盤 {pct(o.get('xnet'))}；"
                     f"最後半年驗證 {hd.get('n', '–')} 筆，平均 {pct(hd.get('mean'))}、比大盤 {pct(hd.get('xnet'))}。")
        L.append("")
    L += ["## 今日買進候選（明日開盤進場）", ""]
    buys = out["buy"][:top]
    if not buys:
        L.append("今日無符合條件的股票。")
    else:
        L.append("| # | 代號 | 名稱 | 訊號 | 參考收盤 | 停損 | 目標（參考） | 持有日 | 回測勝率 | 回測次數 |")
        L.append("|---:|---|---|---|---:|---:|---:|---:|---:|---:|")
        for i, b in enumerate(buys, 1):
            tag = "" if b.get("reliable") else "（試行）"
            L.append(f"| {i} | {b['stock_id']} | {b['name']}{tag} | {'、'.join(s['desc'] for s in b['signals'])} | "
                     f"{b['ref_close']} | {pct(b['stop_pct'])} | {pct(b['take_profit_pct'])} | {b['hold_days']} | "
                     f"{pct(b['oos_win'], 0).lstrip('+')} | {b['oos_n'] if b.get('oos_n') is not None else '–'} |")
    L.append("")
    act = [p for p in state if p["status"] in track.ACTIVE]
    L += ["## 追蹤中（每日評估，直到出場或你賣出）", ""]
    if not act:
        L.append("目前沒有追蹤中的推薦。")
    else:
        L.append("| 代號 | 名稱 | 推薦日 | 進場價 | 收盤 | 報酬(扣成本) | 持有/計畫 | 停損 | 目標（參考） | 建議 | 原因 |")
        L.append("|---|---|---|---:|---:|---:|---:|---:|---:|---|---|")
        order = {"出場": 0, "注意": 1, "續抱": 2, "待進場": 3}
        for p in sorted(act, key=lambda p: order.get(p.get("action"), 9)):
            L.append(f"| {p['stock_id']} | {p['name']} | {fmt_date(p['pick_date'])} | "
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
    L.append("完整報告請看 Email")
    return "\n".join(L)


def render_private(out, state, advices):
    """PRIVATE part (holdings: shares, costs, which picks the user holds) — stored in Supabase private_store and
    appended to the email / Teams report at send time; never published."""
    from tuning import positions
    L = [positions.render(advices, out["as_of"])]
    held_picks = [p for p in state if p["status"] in track.ACTIVE and p.get("user_holds")]
    if held_picks:
        L += ["", "### 你有買進的推薦股", ""]
        for p in held_picks:
            L.append(f"- {p['stock_id']} {p['name']}：{p.get('action', '')}－{p.get('reason', '')}"
                     f"（報酬 {pct(p.get('ret_net'))}）")
    al = [s for s in out["sell_alerts"] if s["alert"]]
    if al:
        L += ["", "### 今日持股出場訊號（明細）", ""]
        L += [f"- {s['stock_id']} {s['name']}：{'；'.join(s['reasons'])}" for s in al]
    return "\n".join(L) + "\n"


def load_tracking(path, private):
    """Tracking state: in the cloud it lives in Supabase private_store (it reveals which picks the user holds);
    locally a JSON file. A legacy file is migrated once into the private store."""
    if private:
        from tuning import private_store
        body = private_store.get("tracked_picks")
        if body is not None:
            return json.loads(body)
        log("[daily] no tracking state in private_store yet — starting from the local file if any")
    return track.load_state(path)


def save_tracking(path, state, private):
    track.save_state(path, state)
    if private:
        from tuning import private_store
        private_store.put("tracked_picks", json.dumps(state, ensure_ascii=False))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--state", default=os.path.join(DAILY_DIR, "tracked_picks.json"))
    ap.add_argument("--private", action="store_true",
                    help="keep tracking state and the holdings advice in Supabase private_store (cloud mode)")
    ap.add_argument("--write-alerts", action="store_true",
                    help="write tomorrow's intraday alert levels to Supabase (public.alert_levels)")
    known, rest = ap.parse_known_args(argv)
    sargs = score_today.parse_args(rest + ["--include-unreliable", "--out-dir", DAILY_DIR])
    os.makedirs(DAILY_DIR, exist_ok=True)
    state = load_tracking(known.state, known.private)
    tracked = [p["stock_id"] for p in state if p["status"] in track.ACTIVE]
    try:
        from tuning import holdings_report
        pos, style = holdings_report.load_positions()
    except Exception as exc:
        log(f"[daily] holdings unavailable: {type(exc).__name__}: {str(exc)[:120]}")
        pos, style = {}, {}
    out, prep = score_today.run(sargs, extra_codes=tracked + list(pos), return_prep=True)
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
    save_tracking(known.state, state, known.private)
    report = render(out, state, added, closed_today, known.top)
    line = render_line(out, state, known.top, closed_today)
    # private holdings advice (shares / costs) → never in the public files
    adv = []
    try:
        from tuning import holdings_report, positions
        adv = holdings_report.advise_all(out, prep, sargs.params, pos, style) if pos else []
        priv, priv_line = render_private(out, state, adv), positions.render_line(adv)
        if known.private:
            from tuning import private_store
            private_store.put(f"holdings:{as_of}", priv)
            private_store.put(f"holdings_line:{as_of}", priv_line)
        else:
            with open(os.path.join(DAILY_DIR, f"holdings_{as_of}.md"), "w", encoding="utf-8") as f:
                f.write(priv)
        log(f"[daily] holdings advice: {len(adv)} stocks ({'private_store' if known.private else 'local file'})")
    except Exception as exc:
        log(f"[daily] holdings advice NOT produced: {type(exc).__name__}: {str(exc)[:200]}")
    # the public recommendations file must not carry the holdings (alerts, intraday levels)
    rec = os.path.join(DAILY_DIR, f"recommendations_{as_of}.json")
    if os.path.exists(rec):
        with open(rec, encoding="utf-8") as f:
            pub = json.load(f)
        buy_ids = {b["stock_id"] for b in pub.get("buy", [])}
        pub["sell_alerts"] = []
        pub["intraday_levels"] = {k: v for k, v in (pub.get("intraday_levels") or {}).items() if k in buy_ids}
        for b in pub.get("buy", []):
            b.pop("holding", None)
        with open(rec, "w", encoding="utf-8") as f:
            json.dump(pub, f, ensure_ascii=False, indent=1)
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
            alert_levels.write(out, state, top=known.top, style=style,
                               advice={a['stock_id']: a for a in adv} if adv else None)
        except Exception as exc:          # alerts are best-effort; the morning report must still be produced
            log(f"[daily] alert levels NOT written: {type(exc).__name__}: {str(exc)[:200]}")
    log(f"[daily] {as_of}: {len(added)} new picks, {len(closed_today)} closed, "
        f"{sum(p['status'] in track.ACTIVE for p in state)} active → {rp}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
