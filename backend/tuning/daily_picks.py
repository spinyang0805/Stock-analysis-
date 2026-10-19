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


WEAK_TOP = 5   # picks shown while the market-wide signals lose in the last-half-year holdout


def weak_period(out):
    """True when the uniform signal set lost to the market in the untouched last-half-year holdout."""
    hd = (out.get("uniform") or {}).get("holdout") or {}
    x = hd.get("xnet")
    return out.get("buy_mode") == "uniform" and x is not None and x < 0


def effective_top(out, top):
    return min(top, WEAK_TOP) if weak_period(out) else top


REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RANK_PAPER = os.path.join(REPO, "public", "data", "rank_paper.json")


def rank_paper_new(as_of, path=RANK_PAPER):
    """The monthly ranking list made at this close (rank_paper.py, committed before this job), else None."""
    try:
        with open(path, encoding="utf-8") as f:
            per = json.load(f).get("periods") or []
        with open(os.path.join(REPO, "public", "data", "stocklist.json"), encoding="utf-8") as f:
            names = {s["code"]: s["name"].replace("股份有限公司", "") for s in json.load(f)}
    except (OSError, ValueError):
        return None
    if not per or per[-1].get("as_of") != as_of:
        return None
    prev = next((p for p in reversed(per[:-1]) if p.get("net") is not None), None)
    return {"picks": [(q["id"], names.get(q["id"], "")) for q in per[-1]["picks"]], "prev": prev}


COMBO = os.path.join(REPO, "public", "data", "combo_signals.json")


def load_combo(as_of, path=COMBO):
    """Tonight's 組合拳 file (combo_daily.py, committed before this job), else None."""
    try:
        with open(path, encoding="utf-8") as f:
            c = json.load(f)
    except (OSError, ValueError):
        return None
    return c if c.get("updated") == as_of else None


def render_combo(c):
    pos = c.get("positions") or []
    new = [p for p in pos if p["stock_id"] in set(c.get("new_today") or [])]
    sells = [p for p in pos if p["status"] == "sell_signal"]
    L = ["## 今日買進候選：組合拳（明日開盤進場）", "",
         "條件：站上往上的季線且大盤非空頭 ＋ 真突破 ＋ 資金流入 ＋ 籌碼／相對強度／營收至少一項。"
         "賣出要 ≥2 種結構破壞＋≥2 類確認，或跌破下彎季線，或跌 10%。⭐ = 也在當月排名前 20。", ""]
    if not new:
        L.append("今日無符合組合拳條件的股票。")
    for i, p in enumerate(new, 1):
        L.append(f"**{i}. {p['name']}（{p['stock_id']}）**{' ⭐' if p.get('in_rank') else ''}　收盤 {p['signal_close']}")
        for k, v in (p.get("buy_reasons") or {}).items():
            L.append(f"- {k}：{'、'.join(v[:3])}")
        L.append("")
    L += ["", "## 組合拳賣出確認（明日開盤賣出）", ""]
    if not sells:
        L.append("今日沒有賣出確認。")
    for p in sells:
        why = "；".join(f"{k}：{'、'.join(v[:2])}" for k, v in (p.get("sell_reasons") or {}).items())
        L.append(f"- **{p['name']}（{p['stock_id']}）** 買進 {p.get('entry_date', '–')} @ {p.get('entry_price', '–')}，"
                 f"目前 {pct(p.get('ret'))}｜{why}")
    hold = sum(p["status"] == "holding" for p in pos)
    L += ["", f"組合拳紙上追蹤中 {hold} 檔，完整清單請看網站「組合拳」頁。", ""]
    return L


def render(out, state, added, closed_today, top, combo=None):
    d = out["as_of"]
    trial = out["n_reliable_params"] == 0
    top = effective_top(out, top)
    L = [f"# 每日股票報告（{fmt_date(d)} 收盤後計算）", ""]
    if trial and combo is None:
        L += ["> **試行**：目前沒有股票通過可靠度驗證，以下推薦依回測排序，僅供觀察，不構成投資建議。", ""]
    if weak_period(out) and combo is None:
        L += [f"> ⚠️ **近期表現差**：這組訊號最近半年平均比大盤差 {pct(out['uniform']['holdout']['xnet'])}，"
              f"今天只列前 {WEAK_TOP} 檔，請用小部位或只觀察。", ""]
    u = out.get("uniform") or {}
    if out.get("buy_mode") == "uniform" and combo is None:
        sig = "、".join(f"{x['desc']}（持有 {x['h']} 日）" for x in u.get("signals", [])) or "–"
        L.append(f"推薦依據：**全市場統一訊號**（{sig}）。出場與停損依各股自己的調參結果。")
        o, hd = u.get("oos") or {}, u.get("holdout") or {}
        if o or hd:
            L.append(f"回測紀錄（扣成本）：模型沒看過的期間 {o.get('n', '–')} 筆，平均 {pct(o.get('mean'))}、"
                     f"勝率 {pct(o.get('win'), 0).lstrip('+')}、比大盤 {pct(o.get('xnet'))}；"
                     f"最後半年驗證 {hd.get('n', '–')} 筆，平均 {pct(hd.get('mean'))}、比大盤 {pct(hd.get('xnet'))}。")
        L.append("")
    if combo is not None:
        L += render_combo(combo)
        buys = []
    else:
        L += ["## 今日買進候選（明日開盤進場）", ""]
        buys = out["buy"][:top]
        if not buys:
            L.append("今日無符合條件的股票。")
    if buys:
        # one short card per pick (readable on a phone)
        for i, b in enumerate(buys, 1):
            tag = "（試行）" if not b.get("reliable") and out.get("buy_mode") != "uniform" else ""
            L.append(f"**{i}. {b['name']}（{b['stock_id']}）**{tag}")
            stop = b.get("stop_loss_if_fill_at_ref")
            L.append(f"- 參考收盤 {b['ref_close']}；停損 {pct(b['stop_pct'])}" + (f"（約 {stop}）" if stop else "")
                     + (f"；目標 {pct(b['take_profit_pct'])}" if b.get("take_profit_pct") else "")
                     + f"；持有約 {b['hold_days']} 日")
            L.append(f"- 訊號：{'、'.join(s['desc'] for s in b['signals'])}")
            if b.get("pullback_price"):
                L.append(f"- 已離月線 {b['ext_atr']:.1f} 倍日波動：可改掛拉回價 **{b['pullback_price']}**"
                         "（回測約一半會成交，5 個交易日內沒成交就放棄）")
            if b.get("reliable") or out.get("buy_mode") != "uniform":
                L.append(f"- 回測：勝率 {pct(b['oos_win'], 0).lstrip('+')}、{b['oos_n'] if b.get('oos_n') is not None else '–'} 次")
            L.append("")
    L.append("")
    act = [p for p in state if p["status"] in track.ACTIVE]
    L += ["## 舊版推薦追蹤中（單一訊號，不再新增，賣完為止）" if combo is not None
          else "## 追蹤中（每日評估，直到出場或你賣出）", ""]
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
    rk = rank_paper_new(d)
    if rk:
        L += ["## 本月排名前 20（紙上模擬，不是買進建議）", "",
              "強勢股＋營收成長合併排名，明日開盤起算、持有到下月換榜。只記錄成績，看半年後是否真的贏 0050。", "",
              "、".join(f"{c} {n}" for c, n in rk["picks"]), ""]
        if rk["prev"]:
            pv = rk["prev"]
            L += [f"上一期（{fmt_date(pv['as_of'])} 起）：清單 {pct(pv['net'])}、0050 {pct(pv.get('b0050'))}。", ""]
    sm = track.summary(state)
    if sm:
        L += ["## 推薦實績（已結案）", "",
              f"共 {sm['n']} 筆，勝率 {sm['win']:.0%}，平均每筆 {pct(sm['mean'])}（扣手續費與證交稅）", ""]
    L.append("數字來自歷史回測與規則判斷，非投資建議。")
    return "\n".join(L) + "\n"


def render_line(out, state, top, closed_today=(), combo=None):
    d = out["as_of"]
    trial = out["n_reliable_params"] == 0
    if combo is not None:
        pos = combo.get("positions") or []
        new = [p for p in pos if p["stock_id"] in set(combo.get("new_today") or [])]
        sells = [p for p in pos if p["status"] == "sell_signal"]
        L = [f"📈 {fmt_date(d)} 收盤後｜組合拳"]
        L.append("【買進候選】" + ("無" if not new else f"{len(new)} 檔（明日開盤）"))
        for p in new[:8]:
            L.append(f"{p['stock_id']} {p['name']}{'⭐' if p.get('in_rank') else ''} {p['signal_close']}｜"
                     + "＋".join((p.get("buy_reasons") or {}).keys()))
        if sells:
            L.append("【賣出確認】明日開盤賣")
            for p in sells:
                L.append(f"{p['stock_id']} {p['name']}（{pct(p.get('ret'))}）")
        imp = [p for p in state if p["status"] in track.ACTIVE and p.get("action") in ("出場", "注意")]
        if imp:
            L.append("【舊推薦提醒】")
            for p in imp:
                L.append(f"{p['stock_id']} {p['name']}：{p['action']}－{p.get('reason', '')}")
        L.append("完整報告請看 Email／網站")
        return "\n".join(L)
    L = [f"📈 {fmt_date(d)} 收盤後{'（試行）' if trial else ''}"]
    buys = out["buy"][:min(effective_top(out, top), 5)]
    L.append("【買進候選】" + ("無" if not buys else "") + ("（近期表現差，小部位）" if weak_period(out) and buys else ""))
    for b in buys:
        tag = "" if b.get("reliable") else "（試行）"
        L.append(f"{b['stock_id']} {b['name']}{tag} {b['ref_close']}｜{b['signals'][0]['desc']}｜停損 {pct(b['stop_pct'])}"
                 + (f"｜拉回 {b['pullback_price']}" if b.get("pullback_price") else ""))
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
    if rank_paper_new(d):
        L.append("【每月排名】本月前 20 名已更新（紙上模擬），看 Email 或網站")
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
    combo = load_combo(as_of)
    log(f"[daily] 組合拳 file {'loaded' if combo else 'NOT found for ' + as_of} — "
        f"{'old single-signal picks are no longer opened' if combo else 'falling back to the old picks'}")
    if combo is not None:
        added = []
    elif any(p.get("pick_date") == as_of for p in state):
        log(f"[daily] picks for {as_of} already opened — not adding again")
        added = []
    else:
        added = track.open_new(state, out["buy"], as_of, effective_top(out, known.top))
    save_tracking(known.state, state, known.private)
    report = render(out, state, added, closed_today, known.top, combo)
    line = render_line(out, state, known.top, closed_today, combo)
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
            alert_levels.write(out, state, top=effective_top(out, known.top), style=style,
                               advice={a['stock_id']: a for a in adv} if adv else None)
        except Exception as exc:          # alerts are best-effort; the morning report must still be produced
            log(f"[daily] alert levels NOT written: {type(exc).__name__}: {str(exc)[:200]}")
    log(f"[daily] {as_of}: {len(added)} new picks, {len(closed_today)} closed, "
        f"{sum(p['status'] in track.ACTIVE for p in state)} active → {rp}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
