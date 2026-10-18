"""run_tuning.py — per-stock signal selection with walk-forward OOS validation.

  python -m tuning.run_tuning --source parquet --universe 500          (run from backend/)
  python backend/tuning/run_tuning.py --source db --universe 30 --finmind-budget 80

Outputs (history/out/ by default):
  stock_params.parquet / stock_params.json   per-stock choice, OOS metrics, confidence, flags
  tuning_report.md                           method summary + per-stock vs group vs uniform OOS table
  oos_trades.parquet                         every OOS trade of every strategy (for audit)
  adjust_coverage.csv / data_quality.csv     adjustment + cleaning stats per stock
Nothing is written to the database (all DB access is READ ONLY).
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "tuning"

from tuning import adjust as adj_mod  # noqa: E402
from tuning import data as data_mod  # noqa: E402
from tuning import evaluate as ev_mod  # noqa: E402
from tuning import indicators as ind  # noqa: E402
from tuning.data import OUT_DIR, log  # noqa: E402


LEVEL_W = {"stock": 1.0, "group": 0.8, "uniform": 0.6, "none": 0.0}  # confidence discount by evidence level


@dataclass
class Cfg:
    horizons: tuple = (3, 5, 10, 20)
    train_days: int = 504
    min_train_days: int = 189
    test_days: int = 63
    n_min: int = 30
    fdr_q: float = 0.10
    t_alt: float = 3.0
    shrink_k: float = 20.0
    max_signals: int = 3
    min_train_events: int = 3
    stable_frac: float = 2 / 3


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--universe", type=int, default=500, help="top-N 4-digit stocks by 60d avg turnover")
    ap.add_argument("--years", type=float, default=5.0, help="history loaded (first ~3 months are warm-up)")
    ap.add_argument("--horizons", default="3,5,10,20")
    ap.add_argument("--train-days", type=int, default=504, help="walk-forward train window (trading days)")
    ap.add_argument("--min-train-days", type=int, default=189, help="shortest train window allowed (flagged)")
    ap.add_argument("--test-days", type=int, default=63)
    ap.add_argument("--n-min", type=int, default=30, help="min OOS events for 'reliable' (flag when lowered)")
    ap.add_argument("--fdr-q", type=float, default=0.10)
    ap.add_argument("--shrink-k", type=float, default=20.0)
    ap.add_argument("--fee-discount", type=float, default=0.6)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--holdings", default=None, help="comma list; default = read-only query of transactions")
    ap.add_argument("--no-holdings", action="store_true")
    ap.add_argument("--finmind-budget", type=int, default=250, help="max FinMind requests this run")
    ap.add_argument("--finmind-interval", type=float, default=2.0, help="seconds between FinMind requests")
    ap.add_argument("--finmind-offline", action="store_true", help="cache only")
    ap.add_argument("--out-dir", default=OUT_DIR)
    return ap.parse_args(argv)


# ── shared data preparation (also used by score_today) ──────────────────────
def get_holdings(args):
    if args.no_holdings:
        return []
    if args.holdings:
        return [c.strip() for c in args.holdings.split(",") if c.strip()]
    try:
        return data_mod.load_holdings()
    except Exception as e:
        log(f"[holdings] unavailable ({type(e).__name__}); continuing without holdings")
        return []


def chip_net(chips, sid, dates, volume):
    c = chips[chips["stock_id"] == sid] if len(chips) else chips
    if c is None or c.empty:
        return None
    c = c.set_index("date").reindex(dates)
    net = c["foreign_buy"] + c["investment_trust_buy"]
    same = (c["foreign_buy"] == c["foreign_buy"].shift(1)) & (c["investment_trust_buy"] == c["investment_trust_buy"].shift(1))
    bad = (net.abs() > volume.to_numpy()) | (same & (net != 0))
    return net.where(~bad).to_numpy()


def prepare(args, universe_size, years, codes=None, end_date=None):
    """Load → clean → universe → adjust → chips. codes: fixed list (score_today) instead of top-N;
    end_date: drop rows after this date (as-of runs)."""
    t0 = time.time()
    holdings = get_holdings(args)
    start = (date.today() - timedelta(days=int(years * 365.25))).strftime("%Y%m%d")
    raw = data_mod.load_prices(args.source, start, holdings + list(codes or []))
    if end_date:
        raw = raw[raw["date"] <= end_date]
    clean, quality, calendar = data_mod.clean_prices(raw)
    top, rank = data_mod.select_universe(clean, universe_size, holdings)
    if codes is not None:
        have = set(clean["stock_id"])
        top = [c for c in dict.fromkeys(list(codes) + holdings) if c in have]
    codes = top
    meta = data_mod.stock_meta(clean[clean["stock_id"].isin(set(codes))])
    log(f"[prepare] {len(raw):,} raw rows → {len(clean):,} clean, calendar {calendar[0]}..{calendar[-1]} "
        f"({len(calendar)} days), universe {len(codes)} ({len(holdings)} holdings) in {time.time() - t0:.0f}s")
    fetcher = adj_mod.FinMindFetcher(budget=args.finmind_budget, interval=args.finmind_interval,
                                     offline=args.finmind_offline)
    adjusted, coverage = adj_mod.adjust_all(clean, codes, fetcher)
    log(f"[prepare] adjusted {len(adjusted)} stocks, FinMind requests {fetcher.fetched}"
        f"{' (stopped: quota)' if fetcher.blocked else ''} in {time.time() - t0:.0f}s")
    chips = data_mod.load_chips(args.source, start, codes)
    chip_rows = {}
    for sid, g in adjusted.items():
        nb = chip_net(chips, sid, g["date"].to_numpy(), g["volume"])
        g["net_buy"] = nb if nb is not None else np.nan
        chip_rows[sid] = int(pd.notna(g["net_buy"]).sum())
    quality = quality.reindex(list(adjusted)).assign(chip_valid_rows=pd.Series(chip_rows))
    return {"adjusted": adjusted, "coverage": coverage, "quality": quality, "meta": meta, "rank": rank,
            "holdings": [h for h in holdings if h in adjusted], "calendar": calendar}


def _stock_job(a):
    sid, g, horizons, costs, etf = a
    ev, fwd, *_ = ev_mod.stock_events(sid, g, horizons, costs, etf)
    fwd["stock_id"] = sid
    return ev, fwd


def build_events(prep, cfg, costs, workers):
    jobs = [(sid, g, cfg.horizons, costs, prep["meta"][sid]["etf"]) for sid, g in prep["adjusted"].items()]
    if workers > 1:
        with ProcessPoolExecutor(workers) as ex:
            res = list(ex.map(_stock_job, jobs, chunksize=8))
    else:
        res = [_stock_job(j) for j in jobs]
    events = pd.concat([r[0] for r in res], ignore_index=True)
    fwd = pd.concat([r[1] for r in res], ignore_index=True)
    bench = fwd.groupby(["date", "h"])["gross"].mean().rename("bench")
    events = events.join(bench, on=["date", "h"])
    events["excess"] = events["gross"] - events["bench"]
    return ev_mod.add_scores(events), bench


# ── walk-forward ─────────────────────────────────────────────────────────────
def make_folds(prep, cfg):
    cal = sorted(set().union(*[set(g["date"]) for g in prep["adjusted"].values()]))
    pos = {d: i for i, d in enumerate(cal)}
    firsts = sorted(pos[g["date"].iloc[0]] for g in prep["adjusted"].values())
    start = firsts[len(firsts) // 2]                      # median listing/first-data position
    avail = len(cal) - start
    train = min(cfg.train_days, max(cfg.min_train_days, avail - 2 * cfg.test_days))
    if avail - train < max(cfg.horizons) + 5:
        raise SystemExit(f"not enough history: {avail} trading days available, train {train}")
    folds, s = [], start + train
    while s < len(cal) - min(cfg.horizons):
        folds.append({"k": len(folds), "train_lo": s - train, "test_lo": s, "test_hi": min(s + cfg.test_days, len(cal))})
        s += cfg.test_days
    return cal, pos, folds, train, avail


def run(args):
    t0 = time.time()
    cfg = Cfg(horizons=tuple(int(x) for x in args.horizons.split(",")), train_days=args.train_days,
              min_train_days=args.min_train_days, test_days=args.test_days, n_min=args.n_min,
              fdr_q=args.fdr_q, shrink_k=args.shrink_k)
    costs = ev_mod.Costs(discount=args.fee_discount)
    prep = prepare(args, args.universe, args.years)
    events, _ = build_events(prep, cfg, costs, args.workers)
    log(f"[events] {len(events):,} events ({(events['side'] == 'buy').sum():,} buy) in {time.time() - t0:.0f}s")
    cal, pos, folds, train_days, avail = make_folds(prep, cfg)
    events["p"] = events["date"].map(pos)
    events["xp"] = events["exit_date"].map(pos)
    ret_panel = pd.DataFrame({sid: g.set_index("date")["aclose"].pct_change() for sid, g in prep["adjusted"].items()}).reindex(cal)
    meta = prep["meta"]
    log(f"[folds] {len(folds)} folds, train {train_days}d, test {cfg.test_days}d, history {avail}d")

    trades, sell_trades = [], []
    for fd in folds:
        tr = events[(events["p"] >= fd["train_lo"]) & (events["p"] < fd["test_lo"]) & (events["xp"] < fd["test_lo"])]
        te = events[(events["p"] >= fd["test_lo"]) & (events["p"] < fd["test_hi"])]
        groups = ev_mod.assign_groups(ret_panel, meta, fd["test_lo"], train_days)
        sel = ev_mod.select(tr, groups, cfg, "buy")
        strategies = {
            "stock": sel["stock"],
            "group": {sid: sel["group"].get(groups.get(sid), []) for sid in prep["adjusted"]},
            "uniform": {sid: sel["uniform"] for sid in prep["adjusted"]},
        }
        for name, ch in strategies.items():
            t = ev_mod.strategy_trades(te, ch, "buy")
            trades.append(t.assign(strategy=name, fold=fd["k"], group=t["stock_id"].map(groups)))
        ssel = ev_mod.select(tr, groups, cfg, "sell")
        st = ev_mod.strategy_trades(te, ssel["stock"], "sell")
        sell_trades.append(st.assign(fold=fd["k"], avoid=-st["gross"], xavoid=-st["excess"]))
        log(f"[fold {fd['k']}] train {cal[fd['train_lo']]}..{cal[fd['test_lo'] - 1]} test {cal[fd['test_lo']]}.."
            f"{cal[fd['test_hi'] - 1]}: uniform={sel['uniform']}")
    trades = pd.concat(trades, ignore_index=True)
    sell_trades = pd.concat(sell_trades, ignore_index=True)

    # final selection on the most recent window (all completed events)
    n = len(cal)
    final_tr = events[(events["p"] >= n - train_days) & events["xp"].notna()]
    groups = ev_mod.assign_groups(ret_panel, meta, n, train_days)
    fsel = ev_mod.select(final_tr, groups, cfg, "buy")
    fsell = ev_mod.select(final_tr, groups, cfg, "sell")
    params = decide(prep, cfg, trades, sell_trades, fsel, fsell, groups, events, len(folds), train_days, avail)
    write_outputs(args, cfg, prep, params, trades, sell_trades, folds, cal, train_days, avail, fsel, time.time() - t0)
    return params, trades


def fold_stability(tr, col="xnet"):
    fm = tr.groupby("fold")[col].mean()
    return int(len(fm)), int((fm > 0).sum())


def decide(prep, cfg, trades, sell_trades, fsel, fsell, groups, events, n_folds, train_days, avail):
    cand = fsel["cand"]
    gstats = fsel["group_stats"].copy()
    ustats = fsel["uniform_stats"].copy()
    as_of = max(g["date"].iloc[-1] for g in prep["adjusted"].values())
    # BH per stock across all its candidates; per group across group candidates; uniform across all
    fdr_stock = {}
    for sid, d in cand.groupby("stock_id"):
        rej = ev_mod.bh_reject(d["p"].to_numpy(), cfg.fdr_q)
        fdr_stock[sid] = {(s, h) for s, h, r, t in zip(d["signal"], d["h"], rej, d["t"]) if r or abs(t) > cfg.t_alt}
    gstats["p"] = [ev_mod.pvalue(t) for t in gstats["t"]]
    fdr_group = {}
    for grp, d in gstats.groupby("group"):
        rej = ev_mod.bh_reject(d["p"].to_numpy(), cfg.fdr_q)
        fdr_group[grp] = {(s, h) for s, h, r, t in zip(d["signal"], d["h"], rej, d["t"]) if r or abs(t) > cfg.t_alt}
    ustats["p"] = [ev_mod.pvalue(t) for t in ustats["t"]]
    rej = ev_mod.bh_reject(ustats["p"].to_numpy(), cfg.fdr_q)
    fdr_uni = {(s, h) for s, h, r, t in zip(ustats["signal"], ustats["h"], rej, ustats["t"]) if r or abs(t) > cfg.t_alt}

    def level_ok(tr, chosen, fdr_set):
        s = ev_mod.summarize(tr)
        nf, npos = fold_stability(tr) if len(tr) else (0, 0)
        ok = (s["n"] >= cfg.n_min and s["mean"] > 0 and s["xnet"] > 0 and nf > 0
              and npos / nf >= cfg.stable_frac and any(c in fdr_set for c in chosen))
        return ok, s, nf, npos

    uni_tr = trades[trades["strategy"] == "uniform"]
    uni_ok, uni_s, uni_nf, uni_np = level_ok(uni_tr, fsel["uniform"], fdr_uni)
    group_cache = {}
    out = []
    for sid, g in prep["adjusted"].items():
        m = prep["meta"][sid]
        grp = groups.get(sid)
        st_tr = trades[(trades["strategy"] == "stock") & (trades["stock_id"] == sid)]
        gp_tr_stock = trades[(trades["strategy"] == "group") & (trades["stock_id"] == sid)]
        un_tr_stock = uni_tr[uni_tr["stock_id"] == sid]
        s_ok, s_sum, s_nf, s_np = level_ok(st_tr, fsel["stock"].get(sid, []), fdr_stock.get(sid, set()))
        g_sum_stock, u_sum_stock = ev_mod.summarize(gp_tr_stock), ev_mod.summarize(un_tr_stock)
        beats = all(not (o["n"] > 0 and o["xnet"] >= s_sum["xnet"]) for o in (g_sum_stock, u_sum_stock))
        if grp not in group_cache:
            gtr = trades[(trades["strategy"] == "group") & (trades["group"] == grp)]
            group_cache[grp] = level_ok(gtr, fsel["group"].get(grp, []), fdr_group.get(grp, set()))
        g_ok, g_sum, g_nf, g_np = group_cache[grp]
        not_contra = lambda s: s["n"] < 5 or s["xnet"] >= 0  # noqa: E731
        if s_ok and beats and fsel["stock"].get(sid):
            level, chosen, oos, nf, npos = "stock", fsel["stock"][sid], s_sum, s_nf, s_np
        elif g_ok and fsel["group"].get(grp) and not_contra(g_sum_stock):
            level, chosen, oos, nf, npos = "group", fsel["group"][grp], g_sum, g_nf, g_np
        elif uni_ok and fsel["uniform"] and not_contra(u_sum_stock):
            level, chosen, oos, nf, npos = "uniform", fsel["uniform"], uni_s, uni_nf, uni_np
        else:
            level, chosen, oos, nf, npos = "none", fsel["stock"].get(sid, []), s_sum, s_nf, s_np
        reliable = level != "none"
        conf = 0.0
        if reliable:
            conf = (LEVEL_W[level] * (oos["n"] / (oos["n"] + cfg.shrink_k)) * (npos / max(nf, 1))
                    * min(1.0, max(oos["xt"], 0) / 3))
        c_rows = cand[cand["stock_id"] == sid].set_index(["signal", "h"])
        sigs = []
        for s, h in chosen:
            r = c_rows.loc[(s, h)] if (s, h) in c_rows.index else None
            sigs.append({"signal": s, "h": int(h), "desc": ind.BUY_DESC.get(s, s), "group": ind.SIGNAL_GROUP.get(s),
                         "train_n": int(r["n"]) if r is not None else 0,
                         "train_mean": float(r["mean"]) if r is not None else None,
                         "train_t": float(r["t"]) if r is not None else None,
                         "train_score": float(r["score"]) if r is not None else None})
        # take-profit / stop guidance from completed events of the chosen signals on this stock
        evs = events[(events["stock_id"] == sid) & (events["side"] == "buy")]
        ce = evs[[k in set(chosen) for k in zip(evs["signal"], evs["h"])]] if chosen else evs.iloc[:0]
        tp = float(ce["mfe"].quantile(0.6)) if len(ce) >= 8 else None
        mae20 = float(ce["mae"].quantile(0.2)) if len(ce) >= 8 else None
        sell_choice = (fsell["stock"].get(sid) or fsell["group"].get(grp) or [("s_atr_25", 10)])[0]
        so = sell_trades[sell_trades["stock_id"] == sid]
        q = prep["quality"].loc[sid]
        cv = prep["coverage"].loc[sid]
        out.append({
            "stock_id": sid, "name": m["name"], "market": m["market"], "etf": m["etf"], "group": grp,
            "universe_rank": prep["rank"].get(sid), "holding": sid in prep["holdings"],
            "level": level, "reliable": reliable, "group_fallback": level in ("group", "uniform"),
            "label": {"stock": "個股專屬訊號", "group": "同組訊號(個股樣本不足)", "uniform": "全市場統一訊號",
                      "none": "no reliable signal"}[level],
            "buy_signals": sigs, "best_h": int(chosen[0][1]) if chosen else None,
            "oos_n": oos["n"], "oos_mean": oos["mean"], "oos_median": oos["median"], "oos_win": oos["win"],
            "oos_t": oos["t"], "oos_excess": oos["excess"], "oos_xnet": oos["xnet"], "oos_xt": oos["xt"], "oos_mae_med": oos.get("mae_med"),
            "oos_mfe_med": oos.get("mfe_med"), "oos_folds": nf, "oos_folds_pos": npos,
            "stock_oos_n": s_sum["n"], "stock_oos_xnet": s_sum["xnet"],
            "stock_oos_group_xnet": g_sum_stock["xnet"], "stock_oos_uniform_xnet": u_sum_stock["xnet"],
            "confidence": round(conf, 4), "tp_pct": tp, "mae_p20": mae20,
            "sell_rule": {"signal": sell_choice[0], "h": int(sell_choice[1]), "desc": ind.SELL_DESC[sell_choice[0]],
                          "oos_n": int(len(so)), "oos_avoid_mean": float(so["avoid"].mean()) if len(so) else None,
                          "oos_xavoid_mean": float(so["xavoid"].mean()) if len(so) else None},
            "stop_atr_mult": ind.ATR_MULT.get(sell_choice[0], 2.5),
            "data_quality": {"history_days": int(len(g)), "first_date": g["date"].iloc[0],
                             "yf_only_rows": int(q["yf_only_rows"]), "yf_unanchored_rows": int(q["yf_unanchored_rows"]),
                             "dropped_copies": int(q["rows_dropped_copies"]), "chip_valid_rows": int(q["chip_valid_rows"]),
                             "adj_events": int(cv["events_applied"]), "finmind": cv["finmind_status"],
                             "unexplained_gaps": int(cv["unexplained_gaps"]),
                             "markers_unexplained": int(cv["markers_unexplained"])},
            "short_history": train_days < 504 or len(g) < 504 + 63,
            "n_min_lowered": cfg.n_min < 30, "train_days": train_days, "as_of": as_of,
        })
    return out


def _fmt(x, pct=True, d=2):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return "–"
    return f"{x * 100:.{d}f}%" if pct else f"{x:.{d}f}"


def write_outputs(args, cfg, prep, params, trades, sell_trades, folds, cal, train_days, avail, fsel, runtime):
    os.makedirs(args.out_dir, exist_ok=True)
    with open(os.path.join(args.out_dir, "stock_params.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now().isoformat(timespec="seconds"), "config": vars(cfg) | {
            "source": args.source, "universe": args.universe, "years": args.years, "fee_discount": args.fee_discount},
            "stocks": params}, f, ensure_ascii=False, indent=1, default=lambda o: None if o is None else (
                float(o) if isinstance(o, (np.floating,)) else int(o) if isinstance(o, np.integer) else str(o)))
    flat = pd.DataFrame(params)
    for c in ("buy_signals", "sell_rule", "data_quality"):
        flat[c] = flat[c].map(lambda v: json.dumps(v, ensure_ascii=False, default=str))
    flat.to_parquet(os.path.join(args.out_dir, "stock_params.parquet"), index=False)
    trades.drop(columns=["p", "xp"], errors="ignore").to_parquet(os.path.join(args.out_dir, "oos_trades.parquet"), index=False)
    prep["coverage"].to_csv(os.path.join(args.out_dir, "adjust_coverage.csv"), encoding="utf-8-sig")
    prep["quality"].to_csv(os.path.join(args.out_dir, "data_quality.csv"), encoding="utf-8-sig")

    # ── report ──
    L = []
    L.append(f"# 個股訊號調校報告 (as of {params[0]['as_of'] if params else '-'})\n")
    L.append(f"- 產生時間 {datetime.now():%Y-%m-%d %H:%M}，執行 {runtime:.0f} 秒；來源 `{args.source}`；"
             f"股票 {len(params)} 檔（前 {args.universe} 成交值 + 持股 {len(prep['holdings'])} 檔）")
    L.append(f"- 交易日 {cal[0]}..{cal[-1]}（{len(cal)} 日；中位數個股可用 {avail} 日）；walk-forward：train {train_days} 日、"
             f"test {cfg.test_days} 日、{len(folds)} folds；horizons {list(cfg.horizons)}")
    flags = []
    if train_days < 504:
        flags.append(f"**SHORT HISTORY**：train window 只有 {train_days} 日（< 2 年），結果不可靠")
    if cfg.n_min < 30:
        flags.append(f"**n_min 已調降為 {cfg.n_min}**（預設 30）")
    if len(folds) < 4:
        flags.append(f"**只有 {len(folds)} 個 OOS fold**，穩定度判斷（≥2/3 folds 為正）統計力很弱")
    for fl in flags:
        L.append(f"- ⚠ {fl}")
    L.append("")
    L.append("## OOS 比較：個股專屬 vs 同組 vs 全市場統一（baseline）\n")
    L.append("| 策略 | 交易數 | 平均淨報酬 | 中位數 | 勝率 | t | 淨超額(扣成本 vs 等權) | 超額 t | 各 fold 淨超額 |")
    L.append("|---|---:|---:|---:|---:|---:|---:|---:|---|")
    for name in ("stock", "group", "uniform"):
        t = trades[trades["strategy"] == name]
        s = ev_mod.summarize(t)
        fm = t.groupby("fold")["xnet"].mean()
        L.append(f"| {name} | {s['n']} | {_fmt(s['mean'])} | {_fmt(s['median'])} | {_fmt(s['win'], d=1)} | "
                 f"{_fmt(s['t'], False)} | {_fmt(s['xnet'])} | {_fmt(s['xt'], False)} | {' / '.join(_fmt(v) for v in fm)} |")
    # paired per-stock comparison
    per = trades.groupby(["strategy", "stock_id"])["xnet"].mean().unstack(0)
    if {"stock", "uniform"} <= set(per.columns):
        both = per[["stock", "uniform"]].dropna()
        L.append(f"\n個股層級配對比較（兩策略都有 OOS 交易的 {len(both)} 檔）：個股專屬勝過統一 baseline 的比例 "
                 f"{_fmt((both['stock'] > both['uniform']).mean(), d=1)}，平均差 {_fmt((both['stock'] - both['uniform']).mean())}")
    st_mean = ev_mod.summarize(trades[trades["strategy"] == "stock"])["xnet"]
    un_mean = ev_mod.summarize(trades[trades["strategy"] == "uniform"])["xnet"]
    verdict = ("個股專屬選擇 **優於**" if np.isfinite(st_mean) and np.isfinite(un_mean) and st_mean > un_mean
               else "個股專屬選擇 **未優於**")
    L.append(f"\n結論（本次資料）：{verdict} 全市場統一 baseline（OOS 每筆平均淨超額報酬 {_fmt(st_mean)} vs {_fmt(un_mean)}；選擇依據為每持有日淨超額）。")
    df = pd.DataFrame(params)
    L.append("\n## 結果分佈\n")
    L.append("| level | 檔數 |\n|---|---:|")
    for lv, c in df["level"].value_counts().items():
        L.append(f"| {lv} | {c} |")
    L.append(f"\n可靠（reliable）{int(df['reliable'].sum())} / {len(df)} 檔；"
             f"持股 {int(df['holding'].sum())} 檔中可靠 {int((df['holding'] & df['reliable']).sum())} 檔。")
    L.append(f"\n最終全市場統一訊號：{fsel['uniform']}")
    sig_cnt = pd.Series([s["signal"] for p in params if p["level"] == "stock" for s in p["buy_signals"]]).value_counts()
    all_cnt = pd.Series([s["signal"] for p in params for s in p["buy_signals"]]).value_counts()
    h_cnt = df["best_h"].value_counts()
    L.append("\n## 最常被選中的訊號（最終 train window 的個股候選，含不可靠者）\n")
    L.append("| 訊號 | 說明 | 被選次數 | 其中 level=stock |\n|---|---|---:|---:|")
    for s, c in all_cnt.head(15).items():
        L.append(f"| {s} | {ind.BUY_DESC.get(s, s)} | {c} | {int(sig_cnt.get(s, 0))} |")
    L.append("\n最佳 horizon 分佈：" + "，".join(f"{int(h)} 日 {c} 檔" for h, c in h_cnt.sort_index().items()))
    sell_cnt = pd.Series([p["sell_rule"]["signal"] for p in params]).value_counts()
    L.append("\n出場規則分佈：" + "，".join(f"{s} {c}" for s, c in sell_cnt.items()))
    so = sell_trades
    if len(so):
        L.append(f"；OOS 出場訊號後 h 日平均「避開的報酬」{_fmt(so['avoid'].mean())}，相對大盤 {_fmt(so['xavoid'].mean())}"
                 f"（n={len(so)}，正值代表出場有利）")
    cov = prep["coverage"]
    L.append("\n## 還原權值覆蓋率\n")
    L.append(f"- FinMind 狀態：{cov['finmind_status'].value_counts().to_dict()}")
    L.append(f"- 套用事件 {int(cov['events_applied'].sum())}（FinMind {int(cov['events_finmind'].sum())}，"
             f"僅由參考價推得 {int(cov['events_implied_only'].sum())}，FinMind/參考價不一致 {int(cov['factor_disagree'].sum())}）")
    L.append(f"- 除權息標記未被解釋：{int(cov['markers_unexplained'].sum())} 筆 / {int((cov['markers_unexplained'] > 0).sum())} 檔；"
             f"未解釋跳空(>10.5%)：{int(cov['unexplained_gaps'].sum())} 筆 / {int((cov['unexplained_gaps'] > 0).sum())} 檔；"
             f"還原後仍 >10.5% 單日變動 {int(cov['adj_gaps_remaining'].sum())} 筆")
    bad = cov[(cov["unexplained_gaps"] > 0) | (cov["markers_unexplained"] > 0)]
    if len(bad):
        L.append("- 需人工確認：" + "；".join(f"{i} {r['unexplained_gap_dates'] or ''} {r['marker_unexplained_dates'] or ''}".strip()
                                         for i, r in bad.head(20).iterrows()))
    q = prep["quality"]
    L.append(f"- 價格清理：刪除複製列 {int(q['rows_dropped_copies'].sum())}、非交易日 {int(q['rows_dropped_noncalendar'].sum())}、"
             f"舊零散段 {int(q['rows_dropped_old_segment'].sum())}；僅 yfinance 的日期 {int(q['yf_only_rows'].sum())} 列"
             f"（無官方錨點 {int(q['yf_unanchored_rows'].sum())}）")
    L.append("\n## 前 20 名（依 confidence，再依 OOS 淨超額；n 小者僅供參考）\n")
    L.append("| 代號 | 名稱 | level | 訊號 | h | OOS n | OOS 平均淨 | 淨超額 | 勝率 | conf |\n|---|---|---|---|---:|---:|---:|---:|---:|---:|")
    xs = lambda p: p["oos_xnet"] if p["oos_xnet"] is not None and np.isfinite(p["oos_xnet"]) else -1.0  # noqa: E731
    for p in sorted([p for p in params if p["oos_n"]], key=lambda p: (-p["confidence"], -xs(p)))[:20]:
        L.append(f"| {p['stock_id']} | {p['name']} | {p['level']} | {', '.join(s['signal'] for s in p['buy_signals'])} | "
                 f"{p['best_h'] or '–'} | {p['oos_n']} | {_fmt(p['oos_mean'])} | {_fmt(p['oos_xnet'])} | {_fmt(p['oos_win'], d=0)} | {p['confidence']:.2f} |")
    L.append("\n## 限制\n")
    L.append("- 歷史長度不足時（目前約 1.4 年）OOS 只有少數 fold，所有數字僅供驗證流程，**不可作為投資依據**。")
    L.append("- 存活者偏差：universe 用「今天」的成交值前 N 名，下市/冷門股不在樣本內，回測報酬偏高。")
    L.append("- 還原權值依賴 FinMind 公開資料與交易所參考價；未解釋缺口僅標記不修補。yfinance 補值列可能有誤。")
    L.append("- 籌碼資料來源混雜（local_backfill / T86 / TPEx），已做基本檢查（|買超| ≤ 成交量、去除重複值）。")
    with open(os.path.join(args.out_dir, "tuning_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log(f"[out] wrote {args.out_dir}/stock_params.json|parquet, tuning_report.md ({runtime:.0f}s)")


if __name__ == "__main__":
    run(parse_args())
