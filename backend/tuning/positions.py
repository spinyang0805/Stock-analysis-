"""positions.py — the user's own holdings: average cost, P/L, break-even and a rule-based action per stock.

PRIVATE: the output contains share counts and costs — it is stored in Supabase (private_store) and emailed,
never written to a public release.

Cost method: moving weighted average (as Taiwanese brokers show it). Buys add price × shares + fee; sells
remove shares at the average cost (realised P/L recorded); stock dividends add shares at zero cost; cash
dividends lower the cost basis ("含息成本").

Actions (transparent rules, not predictions; averaging down raises risk and is never unconditional):
  profit, exit signal                  → 分批獲利了結（先賣一半），其餘以保本價／移動停利保護
  profit, trend intact                 → 續抱；停利價 = max(移動停損, 保本價)
  loss, trend broken / exit signal     → 不攤平；反彈到壓力價減碼，跌破停損價出場
  loss −5%…−25%, trend intact         → 可分批攤平：在支撐價買進，加碼後總投入 ≤ 原投入 1.5 倍
  loss < −25%                          → 不建議攤平（虧損過深，攤平會放大風險）
  存股 style                           → 估值便宜（本益比/淨值比在自身低 20%）才分批加碼；昂貴時考慮減碼
"""
import numpy as np
import pandas as pd

from tuning import indicators as ind
from tuning.evaluate import Costs

MAX_ADD_RATIO = 0.5      # one averaging-down step buys at most half of the current shares
MAX_EXPOSURE = 1.5       # total cost after adding ≤ 1.5 × the current cost
DEEP_LOSS = -0.25
SMALL_LOSS = -0.05


def load_transactions(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT trade_date, upper(stock_id), stock_name, side, shares, price, fee, tax
                         FROM transactions ORDER BY trade_date, created_at""")
        tx = pd.DataFrame(cur.fetchall(), columns=["date", "stock_id", "name", "side", "shares", "price", "fee", "tax"])
        cur.execute("SELECT upper(stock_id), style FROM holding_settings")
        style = dict(cur.fetchall())
    for c in ("shares", "price", "fee", "tax"):
        tx[c] = pd.to_numeric(tx[c], errors="coerce").fillna(0.0).astype(float)
    return tx, style


def positions(tx):
    """Per stock: shares, average cost by FIRST-IN-FIRST-OUT lots (matches the user's broker: a sale consumes
    the oldest lots first), cost net of cash dividends, realised P/L, first/last buy.
    Stock dividends add a zero-cost lot."""
    out = {}
    for sid, d in tx.groupby("stock_id", sort=False):
        lots = []                                   # [shares, unit cost incl. buy fee]
        div = realised = 0.0
        m_sh = m_cost = m_real = 0.0                # moving weighted average (整體均價) in parallel
        first = last = None
        name = d["name"].dropna().iloc[-1] if d["name"].notna().any() else ""
        for r in d.itertuples():
            if r.side == "buy" and r.shares > 0:
                lots.append([r.shares, (r.shares * r.price + r.fee) / r.shares])
                m_sh += r.shares
                m_cost += r.shares * r.price + r.fee
                first = first or r.date
                last = r.date
            elif r.side == "sell":
                q = r.shares
                proceeds = r.shares * r.price - r.fee - r.tax
                if m_sh > 0:
                    mq = min(r.shares, m_sh)
                    m_avg = m_cost / m_sh
                    m_real += proceeds - mq * m_avg
                    m_cost -= mq * m_avg
                    m_sh -= mq
                basis = 0.0
                while q > 1e-9 and lots:
                    take = min(q, lots[0][0])
                    basis += take * lots[0][1]
                    lots[0][0] -= take
                    q -= take
                    if lots[0][0] <= 1e-9:
                        lots.pop(0)
                realised += proceeds - basis
            elif r.side == "stock_dividend" and r.shares > 0:
                # stock dividend / split: the new shares belong to the existing lots — total cost unchanged,
                # unit cost of every lot scaled down (otherwise FIFO sales would leave zero-cost shares behind)
                held = sum(s for s, _ in lots)
                if held > 1e-9:
                    k = (held + r.shares) / held
                    lots = [[s * k, c / k] for s, c in lots]
                else:
                    lots.append([r.shares, 0.0])
                m_sh += r.shares
            elif r.side == "cash_dividend":
                div += r.shares * r.price if r.shares else r.price
        sh = sum(s for s, _ in lots)
        if sh > 1e-9:
            cost = sum(s * c for s, c in lots)
            out[sid] = {"stock_id": sid, "name": name, "shares": sh, "cost": cost, "avg": cost / sh,
                        "avg_net_div": max(cost - div, 0) / sh, "realised": realised,
                        "mavg": m_cost / m_sh if m_sh > 1e-9 else 0.0, "m_realised": m_real,
                        "first_buy": str(first) if first else None, "last_buy": str(last) if last else None}
    return out


def fundamentals(f):
    """Point score from data the engine already has (all known at the last close):
    revenue (last 3 months YoY, 12-month high), valuation vs own 3 years (PE percentile), institutions
    (foreign + trust net buy over 20 days), dividend yield. Returns (score, label, reasons)."""
    sc, why = 0, []
    if "rev_yoy" in f and f["rev_yoy"].notna().any():
        ym = f["rev_ym"] if "rev_ym" in f else None
        months = f.loc[ym.ne(ym.shift(1)) & ym.notna(), "rev_yoy"] if ym is not None else f["rev_yoy"].dropna()
        y3 = months.tail(3)
        if len(y3):
            m = float(y3.mean())
            if m > 0:
                sc += 1
                why.append(f"營收近3月年增 {m:+.0f}%")
            elif m < -10:
                sc -= 1
                why.append(f"營收近3月年減 {m:.0f}%")
        rev = f["rev"].dropna()
        if len(rev) and ym is not None:
            mv = f.loc[ym.ne(ym.shift(1)) & ym.notna(), "rev"].tail(13)
            if len(mv) >= 13 and mv.iloc[-1] >= mv.iloc[:-1].max():
                sc += 1
                why.append("營收創 12 個月新高")
    if "pe" in f:
        pe = f["pe"].where(f["pe"] > 0)
        if pe.notna().sum() > 120 and np.isfinite(pe.iloc[-1]):
            hist = pe.dropna().iloc[-750:]
            q = float((hist < pe.iloc[-1]).mean())
            if q < 0.3:
                sc += 1
                why.append(f"本益比偏低（自身 3 年 {q:.0%} 位置）")
            elif q > 0.8:
                sc -= 1
                why.append(f"本益比偏高（自身 3 年 {q:.0%} 位置）")
    nb = None
    for col in ("foreign", "trust"):
        if col in f and f[col].notna().sum() > 60:
            v = f[col].iloc[-20:].sum()
            nb = v if nb is None else nb + v
    if nb is not None:
        vol20 = f["volume"].iloc[-20:].sum()
        r = nb / vol20 if vol20 else 0
        if r > 0.02:
            sc += 1
            why.append("法人近20日買超")
        elif r < -0.02:
            sc -= 1
            why.append("法人近20日賣超")
    if "dy" in f and np.isfinite(f["dy"].iloc[-1]) and f["dy"].iloc[-1] >= 4:
        sc += 1
        why.append(f"殖利率 {f['dy'].iloc[-1]:.1f}%")
    label = "佳" if sc >= 2 else ("轉弱" if sc <= -1 else "普通")
    return sc, label, why


def _tick_round(x, etf, mode="down"):
    from tuning.score_today import rnd
    return rnd(x, mode, etf)


def advise(pos, g, params, style, sell_alert, levels, costs=Costs()):
    """Rule-based action for one holding. g = adjusted bars (latest adjusted == raw)."""
    sid = pos["stock_id"]
    etf = sid.startswith("00")
    f = ind.features(g)
    last = f.iloc[-1]
    price = float(g["close"].iloc[-1])
    sell_cost = costs.sell(etf)
    breakeven = pos["avg"] / (1 - sell_cost)
    # zero cost (shares only from stock dividends) → treat as fully in profit
    pnl = price * (1 - sell_cost) / pos["avg"] - 1 if pos["avg"] > 0 else 1.0
    pnl_amt = pos["shares"] * (price * (1 - sell_cost) - pos["avg"])
    ma20, ma60 = float(last["ma20"]), float(last["ma60"])
    ma60_up = bool(last["ma60"] > f["ma60"].iloc[-6]) if len(f) > 6 else False
    struct = int(ind.patterns(f)["struct"].iloc[-1])
    trend_ok = price > ma60 and ma60_up and struct != -1
    trend_bad = (price < ma60 and not ma60_up) or struct == -1
    exit_sig = bool(sell_alert and sell_alert.get("alert"))
    trail = (levels or {}).get("trailing_stop")
    mavg = pos.get("mavg", pos["avg"])
    total_pl = pos["shares"] * price * (1 - sell_cost) - pos["shares"] * mavg + pos.get("m_realised", 0.0)
    a = {"stock_id": sid, "name": pos["name"], "shares": int(round(pos["shares"])), "avg": round(pos["avg"], 2),
         "mavg": round(mavg, 2), "m_pnl": price * (1 - sell_cost) / mavg - 1 if mavg > 0 else None,
         "total_pl": total_pl, "realised": pos.get("m_realised", 0.0),
         "price": price, "pnl": pnl, "pnl_amt": pnl_amt,
         "breakeven": _tick_round(breakeven, etf, "up") if pos["avg"] > 0 else None,
         # no style set: ETFs are treated as long-term holdings (存股), stocks as swing positions (波段)
         "style": style or ("存股" if etf else "波段"),
         "trend": "多頭" if trend_ok else ("空頭" if trend_bad else "盤整"),
         "reasons": list((sell_alert or {}).get("reasons") or [])}
    if a["style"] == "存股":
        pe = f["pe"].where(f["pe"] > 0) if "pe" in f else None
        has_pe = bool(pe is not None and pe.notna().sum() > 120)
        cheap = has_pe and pe.iloc[-1] < ind.val_q(pe, 0.2).iloc[-1]
        exp = has_pe and pe.iloc[-1] > ind.val_q(pe, 0.9).iloc[-1]
        if cheap:
            a.update(action="分批加碼", detail="存股：本益比在自身近 3 年低 20%（便宜區）")
            _add_plan(a, pos, price, ma20, ma60, etf)
        elif exp and pnl > 0:
            a.update(action="考慮減碼", detail="存股：本益比在自身近 3 年高 10%（昂貴區），可分批獲利")
        elif not has_pe:
            a.update(action="續抱", detail="存股（ETF）：長期持有，不因短線訊號賣出" +
                     ("；短線轉弱可暫緩加碼" if exit_sig or trend_bad else ""))
        else:
            a.update(action="續抱", detail="存股：估值不在便宜或昂貴區")
        return a
    # dividend yield (BWIBBU / TPEx 殖利率 %, latest known) — the user's bar for a profit worth selling
    dy = float(f["dy"].iloc[-1]) / 100 if "dy" in f and np.isfinite(f["dy"].iloc[-1]) else 0.0
    a["dy"] = dy
    resist = [x for x in (ma20, ma60) if np.isfinite(x) and x > price]
    low60 = float(g["alow"].iloc[-60:].min())
    # technical state: 中期轉空 (below a falling MA60 / bearish swing structure), 短線轉弱 (exit signal or close
    # below MA20), else 正常; fundamentals: revenue, valuation, institutions, yield
    weak = exit_sig or (np.isfinite(ma20) and price < ma20)
    tech = "中期轉空" if trend_bad else ("短線轉弱" if weak else "正常")
    fsc, flab, fwhy = fundamentals(f)
    a.update(tech=tech, fund=flab, fund_score=fsc, fund_why=fwhy)
    fund_txt = f"基本面{flab}（{'、'.join(fwhy) or '無明顯訊號'}）"
    reduce_at = _tick_round(min(resist), etf, "up") if resist else None
    if pnl >= 0:
        cands = [x for x in (trail, breakeven) if x]
        if pnl < dy:
            # profit below the dividend yield → normally keep and collect the dividend, unless the trend has
            # turned AND the fundamentals do not support the stock (then exit while still not at a loss)
            if tech != "正常" and flab == "轉弱":
                a.update(action="趁未虧損出場",
                         detail=f"獲利 {pnl:.1%} 低於殖利率 {dy:.1%}，但{tech}且{fund_txt}：繼續放可能轉為虧損，"
                                f"建議在保本價以上出場",
                         stop=_tick_round(breakeven, etf, "down") if pos["avg"] > 0 else None)
            elif tech != "正常":
                a.update(action="續抱領息",
                         detail=f"獲利 {pnl:.1%} 低於殖利率 {dy:.1%}；{tech}，但{fund_txt}，可續抱領股利、"
                                f"轉弱期間不加碼；收盤跌破保本價再考慮出場",
                         stop=_tick_round(breakeven, etf, "down") if pos["avg"] > 0 else None)
            else:
                a.update(action="續抱領息", detail=f"獲利 {pnl:.1%} 未超過殖利率 {dy:.1%}，賣出不划算，續抱領股利；"
                                                  f"{fund_txt}")
            return a
        a["stop"] = _tick_round(max(cands), etf, "down") if cands else None
        if exit_sig or trend_bad:
            a.update(action="分批獲利了結",
                     detail=f"獲利 {pnl:.1%} 已超過殖利率 {dy:.1%}，且{'趨勢轉空' if trend_bad else '出現出場訊號'}："
                            f"建議先賣一半，其餘以停利價保護；{fund_txt}")
            a["sell_shares"] = int(round(pos["shares"] / 2 / 1000)) * 1000 or int(round(pos["shares"] / 2))
        else:
            a.update(action="續抱", detail=f"獲利 {pnl:.1%}（殖利率 {dy:.1%}），趨勢未轉弱；收盤跌破停利價再出場；"
                                         f"{fund_txt}")
        return a
    # ── loss: the user prefers holding long and averaging down. The fundamentals decide whether a downtrend is a
    # reason to exit (weak) or to wait (good). Both options are always listed.
    if trend_bad:
        if flab == "轉弱":
            a.update(action="建議出場", detail=f"中期轉空且{fund_txt}：技術面與基本面都轉弱，繼續持有可能擴大虧損，"
                                            "建議出場或反彈時減碼", reduce_at=reduce_at,
                     stop=_tick_round(trail, etf, "down") if trail else None)
            a["alt"] = "另一選擇：至少不要加碼，反彈先減一半"
        elif flab == "佳":
            a.update(action="長抱等待（基本面佳）",
                     detail=f"中期轉空，但{fund_txt}：可長期持有等待；轉空期間先不加碼，"
                            "等收盤站回季線後再依下列價位分批攤平")
            _stage_plan(a, pos, price, [low60, low60 * 0.9], etf)
            a["alt"] = f"另一選擇：反彈到 {reduce_at} 先減碼" if reduce_at else "另一選擇：不加碼，續抱"
        else:
            a.update(action="反彈減碼", detail=f"中期轉空，{fund_txt}：建議反彈時先減碼一半，其餘等止跌",
                     reduce_at=reduce_at, stop=_tick_round(trail, etf, "down") if trail else None)
            _stage_plan(a, pos, price, [low60, low60 * 0.9], etf)
            a["alt"] = "另一選擇：長線攤平（上面的分批價位，等止跌再買）"
        return a
    a["alt"] = (f"另一選擇：反彈到 {reduce_at} 先減碼" if reduce_at else "另一選擇：不加碼，續抱等待解套")
    if pnl > SMALL_LOSS:
        a.update(action="續抱", detail=f"小幅虧損（{pnl:.1%}），先不動作；{fund_txt}")
        return a
    if flab == "轉弱":
        a.update(action="不攤平，續抱觀察", detail=f"趨勢未轉空，但{fund_txt}：先不加碼，基本面回穩再考慮攤平",
                 stop=_tick_round(trail, etf, "down") if trail else None)
        return a
    sup = [x for x in (ma20, ma60) if np.isfinite(x) and x <= price]
    a.update(action="分批攤平（長線）", detail=f"趨勢未轉空、{fund_txt}：可在支撐價分批加碼、長期持有" +
             ("；短線出現轉弱訊號，第一批可等收盤站回月線再買" if exit_sig else ""))
    _stage_plan(a, pos, price, sup or [low60, low60 * 0.9], etf)
    if pnl < DEEP_LOSS:
        a["detail"] += f"；虧損已 {pnl:.0%}，加碼前請確認資金能長期持有"
    return a


def _stage_plan(a, pos, price, levels, etf):
    """Staged averaging down: up to 2 buy levels (below the price), each 1/4 of the current shares, total cost
    after both ≤ 2 × the current cost (long-term preference). Reports the average after each step."""
    lv = sorted({_tick_round(x, etf, "down") for x in levels if x and np.isfinite(x) and x <= price}, reverse=True)[:2]
    if not lv:
        lv = [_tick_round(price, etf, "down")]
    steps, sh, cost = [], pos["shares"], pos["cost"]
    for at in lv:
        q = pos["shares"] * 0.25
        q = int(q // 1000) * 1000 if q >= 1000 else int(round(q))
        if q <= 0 or cost + q * at > 2 * pos["cost"]:
            break
        sh, cost = sh + q, cost + q * at
        steps.append((at, q, round(cost / sh, 2)))
    a["stages"] = steps


def _add_plan(a, pos, price, ma20, ma60, etf):
    """Buy at the nearest support below the price (rising MA20 / MA60), at most half the current shares and
    total cost ≤ MAX_EXPOSURE × current cost; report the new average."""
    sup = [x for x in (ma20, ma60) if np.isfinite(x) and x <= price]
    at = max(sup) if sup else price
    at = _tick_round(at, etf, "down") or price
    lot = 1000
    max_by_ratio = pos["shares"] * MAX_ADD_RATIO
    max_by_cost = (MAX_EXPOSURE - 1) * pos["cost"] / at
    q = int(min(max_by_ratio, max_by_cost) // lot) * lot
    if q <= 0:
        q = int(min(max_by_ratio, max_by_cost))          # odd lots for small positions
    if q <= 0:
        a.update(add_at=None)
        return
    new_avg = (pos["cost"] + q * at) / (pos["shares"] + q)
    a.update(add_at=at, add_shares=q, new_avg=round(new_avg, 2))


def render(advices, as_of):
    d = f"{as_of[:4]}/{as_of[4:6]}/{as_of[6:]}"
    L = [f"## 我的持股建議（{d} 收盤，僅寄給你本人）", ""]
    if not advices:
        return "\n".join(L + ["目前沒有持股。"]) + "\n"
    tot_cost = sum(a["avg"] * a["shares"] for a in advices)
    tot_pnl = sum(a["pnl_amt"] for a in advices)
    tot_all = sum(a["total_pl"] for a in advices)
    L.append(f"持股 {len(advices)} 檔，成本約 {tot_cost:,.0f} 元（先進先出）；目前持股的未實現損益約 {tot_pnl:+,.0f} 元"
             f"（{(tot_pnl / tot_cost if tot_cost else 0):+.1%}）；這些股票從第一次買進到現在的累計總損益（含已賣出的部分）"
             f"約 {tot_all:+,.0f} 元。皆已扣賣出手續費與證交稅。")
    L.append("")
    order = {"建議出場": 0, "趁未虧損出場": 1, "反彈減碼": 2, "分批獲利了結": 3, "長抱等待（基本面佳）": 4,
             "分批攤平（長線）": 5, "不攤平，續抱觀察": 6, "分批加碼": 7, "考慮減碼": 8, "續抱領息": 9, "續抱": 10}
    L.append("| 代號 | 名稱 | 股數 | 現價 | 均價（先進先出） | 損益（先進先出） | 整體均價 | 損益（整體均價） | 累計總損益（含已實現） | 殖利率 | 保本價 | 技術面 | 基本面 | 建議 | 價位 | 說明 |")
    L.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|")
    for a in sorted(advices, key=lambda a: (order.get(a["action"], 9), a["pnl"])):
        lv = []
        if a.get("add_at"):
            lv.append(f"加碼 {a['add_at']} 買 {a['add_shares']:,} 股 → 均價 {a['new_avg']}")
        for i, (at, q, nav) in enumerate(a.get("stages") or [], 1):
            lv.append(f"第{i}批 {at} 買 {q:,} 股 → 均價 {nav}")
        if a.get("alt"):
            lv.append(a["alt"])
        if a.get("reduce_at"):
            lv.append(f"減碼 {a['reduce_at']}")
        if a.get("sell_shares"):
            lv.append(f"先賣 {a['sell_shares']:,} 股")
        if a.get("stop"):
            lv.append(f"停損/停利 {a['stop']}" + ("（已跌破）" if a["stop"] > a["price"] else ""))
        extra = "；".join(a["reasons"]) if a["reasons"] else ""
        zero = "（零成本：配股）" if a["avg"] <= 0 else ""
        mp = f"{a['m_pnl']:+.1%}" if a.get("m_pnl") is not None else "–"
        L.append(f"| {a['stock_id']} | {a['name']} | {a['shares']:,} | {a['price']} | {a['avg']}{zero} | {a['pnl']:+.1%} | "
                 f"{a['mavg']} | {mp} | {a['total_pl']:+,.0f} 元 | "
                 f"{(format(a['dy'], '.1%') if a.get('dy') else '–')} | {a['breakeven'] or '–'} | {a.get('tech', a['trend'])} | {a.get('fund', '–')} | **{a['action']}** | {'；'.join(lv) or '–'} | "
                 f"{a['detail']}{('；' + extra) if extra else ''} |")
    L += ["", "技術面：「中期轉空」= 跌破下彎的季線或波段結構轉空；「短線轉弱」= 跌破月線或出現出場訊號。"
          "基本面依營收（近 3 月年增、是否創 12 個月新高）、本益比在自身 3 年的位置、法人近 20 日買賣超、殖利率評分："
          "佳／普通／轉弱。技術轉空但基本面佳 → 可長抱等待（不加碼）；技術與基本面都轉弱 → 建議出場。",
          "說明：兩種均價——「先進先出」與券商相同，看目前這批持股賺賠；「整體均價」是移動平均成本，"
          "搭配累計總損益看從以前到現在一共賺了多少。配股、分割增加的股數不另計成本（總成本不變、平均到所有股數）；"
          "若買進的股票都已賣出、只剩配股，成本為 0。建議以先進先出均價判斷。"
          "獲利：只有獲利超過該股殖利率才建議賣出，否則續抱領股利。"
          "虧損：依你的偏好以「分批攤平、長期持有」為主；只有轉為空頭（跌破下彎的季線或波段結構轉空）才建議出場。"
          "每批加碼現有股數的 1/4，最多兩批，加碼後總投入不超過原本的 2 倍。每檔都列出另一種選擇，由你決定。"
          "攤平會增加投入金額與風險，沒有任何方法能保證不賠錢。"]
    return "\n".join(L) + "\n"


def render_line(advices):
    act = [a for a in advices if a["action"] not in ("續抱", "續抱領息")]
    if not act:
        return ""
    L = ["【我的持股】"]
    for a in act:
        p = a.get("add_at") or a.get("reduce_at") or a.get("stop")
        L.append(f"{a['stock_id']} {a['name']} {a['pnl']:+.0%}：{a['action']}" + (f" {p}" if p else ""))
    return "\n".join(L)
