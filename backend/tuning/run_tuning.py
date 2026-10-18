"""run_tuning.py — per-stock signal selection with walk-forward OOS validation.

  cd backend && python tuning/run_tuning.py --source parquet --universe 500 --workers 4
  python backend/tuning/run_tuning.py --source db --universe 30 --finmind-budget 80

Evaluation population is POINT-IN-TIME: every --rebalance-days sessions the universe is re-ranked
(top N 4-digit non-ETF stocks by trailing 60-session turnover known at that time). An event counts only
if its stock was in the universe on the signal day; the equal-weight benchmark uses the same set.
Holdings are scored for output (SELL alerts, levels) but never enter selection pools, benchmark or tests.

Per fold: level (stock / group / uniform) is chosen on TRAIN data only (inner validation = last
--test-days of the train window); the test window is untouched until the final OOS track record,
which is gated by BH-FDR across all stocks.

Outputs (history/out/ by default):
  stock_params.parquet / stock_params.json   per-stock choice, OOS metrics, confidence, flags
  tuning_report.md                           method summary + strategy comparison
  oos_trades.parquet                         every OOS trade of every strategy (audit)
  deployed_trades.parquet                    event-driven deployed-policy OOS record (reliability gates)
  adjust_coverage.csv / data_quality.csv     adjustment + cleaning stats per stock
Nothing is written to the database (all DB access is READ ONLY).
"""
import argparse
import json
import math
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
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

LEVELS = ("stock", "group", "uniform")
LEVEL_W = {"stock": 1.0, "group": 0.8, "uniform": 0.6, "none": 0.0}  # confidence discount by evidence level
DEFAULT_LEVEL = "group"  # train-only prior when the inner validation window has too few trades
MIN_VAL_TRADES = 2       # a level competes in the inner validation only with at least this many trades


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
    min_train_events: int = 10
    # Phase B done and confirmed by the Codex cross-review (round 7, 2026-10-19): reliability labels allowed.
    # Set True again to suppress "reliable" claims (gates stay as diagnostics).
    exploratory: bool = False
    holdout_days: int = 126     # last ~6 months: excluded from every fold, evaluated once at the end
    stable_frac: float = 2 / 3
    rebalance_days: int = 21
    turnover_lookback: int = 60


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["parquet", "db"], required=True)
    ap.add_argument("--universe", type=int, default=500, help="top-N 4-digit stocks by 60d avg turnover (point-in-time)")
    ap.add_argument("--rebalance-days", type=int, default=21, help="universe re-ranking interval (sessions)")
    ap.add_argument("--years", type=float, default=5.0, help="history loaded (first ~3 months are warm-up)")
    ap.add_argument("--horizons", default="3,5,10,20", help="holding periods in TRADING ROWS of the stock")
    ap.add_argument("--train-days", type=int, default=504, help="walk-forward train window (trading days)")
    ap.add_argument("--min-train-days", type=int, default=189, help="shortest train window allowed (flagged)")
    ap.add_argument("--test-days", type=int, default=63, help="test window = inner validation window")
    ap.add_argument("--n-min", type=int, default=30, help="min OOS (non-overlapping) trades for 'reliable'")
    ap.add_argument("--min-train-events", type=int, default=10, help="candidate floor before BH / selection")
    ap.add_argument("--fdr-q", type=float, default=0.10)
    ap.add_argument("--shrink-k", type=float, default=20.0)
    ap.add_argument("--fee-discount", type=float, default=0.6)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--end-date", default=None, help="YYYYMMDD: ignore data after this date (historical as-of tuning)")
    add_common_args(ap)
    return ap.parse_args(argv)


def add_common_args(ap):
    """Data / adjustment options shared with score_today."""
    ap.add_argument("--holdings", default=None, help="comma list; default = read-only query of transactions")
    ap.add_argument("--no-holdings", action="store_true")
    ap.add_argument("--adjust-source", choices=["finmind", "exchange"], default="finmind",
                    help="dividend events: FinMind DividendResult (default) or TWSE TWT49U + TPEx exDailyQ")
    ap.add_argument("--finmind-budget", type=int, default=250, help="max FinMind requests this run")
    ap.add_argument("--finmind-interval", type=float, default=2.0, help="seconds between FinMind requests")
    ap.add_argument("--finmind-offline", action="store_true", help="FinMind cache only")
    ap.add_argument("--exchange-budget", type=int, default=150, help="max exchange requests this run")
    ap.add_argument("--exchange-interval", type=float, default=3.0)
    ap.add_argument("--out-dir", default=OUT_DIR)


# ── shared data preparation (also used by score_today) ──────────────────────
def get_holdings(args):
    """Holdings list. args._holdings_ok is set False when the query FAILED (as opposed to an empty portfolio),
    so callers that infer 'the user sold' can refuse to act on a failed lookup."""
    args._holdings_ok = True
    if args.no_holdings:
        return []
    if args.holdings:
        return [c.strip() for c in args.holdings.split(",") if c.strip()]
    try:
        return data_mod.load_holdings()
    except Exception as e:
        args._holdings_ok = False
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


def chip_cols(chips, sid, dates, volume):
    """Per-source chip columns aligned to dates: foreign / trust net buy (shares), margin / short balance (張).
    Rows failing the |buy| <= volume or stale-duplicate checks are NaN."""
    c = chips[chips["stock_id"] == sid] if len(chips) else chips
    if c is None or c.empty:
        return {}
    c = c.set_index("date").reindex(dates)
    vol = volume.to_numpy()
    out = {}
    fb, tb = c["foreign_buy"], c["investment_trust_buy"]
    # stale copy = BOTH foreign and trust identical to the previous day (same rule as chip_net);
    # a single series repeating (e.g. trust +100 張 two days in a row) is legitimate
    stale = (fb == fb.shift(1)) & (tb == tb.shift(1)) & ((fb != 0) | (tb != 0))
    for x, col in ((fb, "foreign"), (tb, "trust")):
        out[col] = x.where(~((x.abs() > vol) | stale)).to_numpy()
    for src, col in (("margin_balance", "margin"), ("short_balance", "short")):
        out[col] = c[src].to_numpy()
    return out


def attach_extras(g, sid, meta, mkt_idx, rev, sbl, tdcc=None):
    """Adds columns: mkt (index close of the stock's own market), rev_yoy / rev (latest monthly revenue
    already PUBLIC on that date — as-of join on avail_date), sbl (借券賣出餘額),
    p400 / p1000 / n_total (集保 week strictly BEFORE the row date — the week's table is published after it)."""
    dates = g["date"].astype(str)
    mk = "TPEX" if str((meta.get(sid) or {}).get("market", "")) in ("TPEx", "上櫃") else "TAIEX"
    idx = mkt_idx.get(mk)
    if idx is None:
        idx = mkt_idx.get("TAIEX")
    g["mkt"] = idx.reindex(dates.to_numpy()).ffill().to_numpy() if idx is not None else np.nan
    g["rev"], g["rev_yoy"], g["rev_ym"] = np.nan, np.nan, np.nan
    if rev is not None and len(rev):
        r = rev.dropna(subset=["avail_date"]).sort_values(["avail_date", "ym"])
        r = r.drop_duplicates("avail_date", keep="last")
        rr = pd.DataFrame({"d": r["avail_date"].astype("int64").to_numpy(),
                           "revenue": r["revenue"].astype(float).to_numpy(), "yoy": r["yoy"].astype(float).to_numpy(),
                           "ym": r["ym"].astype("int64").astype(float).to_numpy()})
        left = pd.DataFrame({"d": dates.astype("int64").to_numpy(), "_i": np.arange(len(g))})
        m = pd.merge_asof(left.sort_values("d"), rr, on="d", direction="backward").sort_values("_i")
        g["rev"], g["rev_yoy"], g["rev_ym"] = m["revenue"].to_numpy(), m["yoy"].to_numpy(), m["ym"].to_numpy()
    g["sbl"] = (sbl.set_index("date")["sbl_balance"].reindex(dates.to_numpy()).to_numpy()
                if sbl is not None and len(sbl) else np.nan)
    for col in ("p400", "p1000", "n_total", "td_d"):
        g[col] = np.nan
    if tdcc is not None and len(tdcc):
        t = tdcc.sort_values("date")
        tt = pd.DataFrame({"d": t["date"].astype("int64").to_numpy(),
                           "td_d": t["date"].astype("int64").astype(float).to_numpy(),
                           **{c: t[c].astype(float).to_numpy() for c in ("p400", "p1000", "n_total")}})
        left = pd.DataFrame({"d": dates.astype("int64").to_numpy(), "_i": np.arange(len(g))})
        m = pd.merge_asof(left.sort_values("d"), tt, on="d", direction="backward",
                          allow_exact_matches=False).sort_values("_i")
        for col in ("p400", "p1000", "n_total", "td_d"):
            g[col] = m[col].to_numpy()


def prepare(args, universe_size, years, codes=None, end_date=None, cfg=None):
    """Load → clean → universe → adjust → chips.
    codes given (score_today): exactly those + holdings.  Otherwise point-in-time membership (pit_membership)
    and the union of all historical + current members is adjusted.  end_date drops later rows."""
    t0 = time.time()
    cfg = cfg or Cfg()
    holdings = get_holdings(args)
    start = (date.today() - timedelta(days=int(years * 365.25))).strftime("%Y%m%d")
    raw = data_mod.load_prices(args.source, start, holdings + list(codes or []))
    if end_date:
        raw = raw[raw["date"] <= end_date]
    clean, quality, calendar = data_mod.clean_prices(raw)
    cal = sorted(clean["date"].unique())
    inu, schedule, current = None, [], []
    if codes is not None:
        have = set(clean["stock_id"])
        current, rank = data_mod.select_universe(clean, universe_size, [])
        todo = [c for c in dict.fromkeys(list(codes) + holdings) if c in have]
    else:
        inu, schedule, current = data_mod.pit_membership(clean, cal, universe_size, cfg.turnover_lookback,
                                                         cfg.rebalance_days)
        rank = {c: i + 1 for i, c in enumerate(current)}
        have = set(clean["stock_id"])
        todo = list(dict.fromkeys(current + sorted(set(inu["stock_id"])) + [h for h in holdings if h in have]))
    meta = data_mod.stock_meta(clean[clean["stock_id"].isin(set(todo))])
    log(f"[prepare] {len(raw):,} raw rows → {len(clean):,} clean, calendar {cal[0]}..{cal[-1]} ({len(cal)} days), "
        f"{len(todo)} stocks to adjust ({len(holdings)} holdings, {len(schedule)} rebalances) in {time.time() - t0:.0f}s")
    fetcher = adj_mod.FinMindFetcher(budget=args.finmind_budget, interval=args.finmind_interval,
                                     offline=args.finmind_offline)
    exchange = adj_mod.ExchangeFetcher(budget=args.exchange_budget, interval=args.exchange_interval)
    adjusted, coverage = adj_mod.adjust_all(clean, todo, fetcher, source=args.adjust_source, exchange=exchange)
    log(f"[prepare] adjusted {len(adjusted)} stocks ({args.adjust_source}), FinMind requests {fetcher.fetched}"
        f"{' (stopped)' if fetcher.blocked else ''}, exchange requests {exchange.fetched} in {time.time() - t0:.0f}s")
    chips = data_mod.load_chips(args.source, start, list(adjusted))
    val = data_mod.load_valuation(start, list(adjusted))
    val_by = dict(tuple(val.groupby("stock_id"))) if len(val) else {}
    mkt_idx = data_mod.load_market_index()
    rev = data_mod.load_revenue(list(adjusted))
    rev_by = dict(tuple(rev.groupby("stock_id"))) if len(rev) else {}
    sbl = data_mod.load_sbl(start, list(adjusted))
    sbl_by = dict(tuple(sbl.groupby("stock_id"))) if len(sbl) else {}
    td = data_mod.load_tdcc(list(adjusted))
    td_by = dict(tuple(td.groupby("stock_id"))) if len(td) else {}
    log(f"[prepare] extras: valuation {len(val_by)} stocks, market {list(mkt_idx)}, revenue {len(rev_by)}, "
        f"sbl {len(sbl_by)}, tdcc {len(td_by)}")
    chip_rows = {}
    for sid, g in adjusted.items():
        nb = chip_net(chips, sid, g["date"].to_numpy(), g["volume"])
        g["net_buy"] = nb if nb is not None else np.nan
        cc = chip_cols(chips, sid, g["date"].to_numpy(), g["volume"])
        for col in ("foreign", "trust", "margin", "short"):
            g[col] = cc.get(col, np.nan)
        v = val_by.get(sid)
        vv = v.set_index("date").reindex(g["date"].to_numpy()) if v is not None else None
        for col in ("pe", "pb", "dy"):
            g[col] = vv[col].to_numpy() if vv is not None else np.nan
        attach_extras(g, sid, meta, mkt_idx, rev_by.get(sid), sbl_by.get(sid), td_by.get(sid))
        chip_rows[sid] = int(pd.notna(g["net_buy"]).sum())
    quality = quality.reindex(list(adjusted)).assign(chip_valid_rows=pd.Series(chip_rows))
    return {"adjusted": adjusted, "coverage": coverage, "quality": quality, "meta": meta, "rank": rank,
            "holdings": [h for h in holdings if h in adjusted], "holdings_all": list(holdings),
            "holdings_ok": getattr(args, "_holdings_ok", True), "calendar": cal, "inu": inu,
            "schedule": schedule, "current": [c for c in current if c in adjusted]}


def _stock_job(a):
    sid, g, horizons, costs, etf = a
    ev, fwd, *_ = ev_mod.stock_events(sid, g, horizons, costs, etf)
    fwd["stock_id"] = sid
    return ev, fwd


def build_events(prep, cfg, costs, workers):
    """All events + point-in-time flag `in_u`; benchmark = equal-weight mean over the in-universe stocks."""
    jobs = [(sid, g, cfg.horizons, costs, prep["meta"][sid]["etf"]) for sid, g in prep["adjusted"].items()]
    if workers > 1:
        with ProcessPoolExecutor(workers) as ex:
            res = list(ex.map(_stock_job, jobs, chunksize=8))
    else:
        res = [_stock_job(j) for j in jobs]
    events = pd.concat([r[0] for r in res], ignore_index=True)
    fwd = pd.concat([r[1] for r in res], ignore_index=True)
    inu = prep["inu"]
    if inu is not None:
        key = inu.assign(in_u=True)
        fwd = fwd.merge(key, on=["date", "stock_id"], how="inner")
        events = events.merge(key, on=["date", "stock_id"], how="left")
        events["in_u"] = events["in_u"].fillna(False).astype(bool)
    else:
        events["in_u"] = True
    bench = fwd.groupby(["date", "h"])["gross"].mean().rename("bench")
    events = events.join(bench, on=["date", "h"])
    events["excess"] = events["gross"] - events["bench"]
    return ev_mod.add_scores(events), bench


# ── walk-forward ─────────────────────────────────────────────────────────────
def make_folds(prep, cfg):
    cal = prep["calendar"]
    pos = {d: i for i, d in enumerate(cal)}
    firsts = sorted(pos[g["date"].iloc[0]] for g in prep["adjusted"].values())
    start = max(firsts[len(firsts) // 2], cfg.turnover_lookback)   # median first-data position, after 1st rebalance
    # the last holdout_days sessions are never used by a fold (final check, see holdout_eval): size the
    # train window from the PRE-holdout history only
    end = len(cal) - cfg.holdout_days
    avail = end - start
    train = min(cfg.train_days, max(cfg.min_train_days, avail - 2 * cfg.test_days))
    if avail - train < max(cfg.horizons) + 5 or train <= cfg.test_days:
        raise SystemExit(f"not enough pre-holdout history: {avail} trading days available, train {train} "
                         f"(holdout {cfg.holdout_days})")
    folds, s = [], start + train
    while s < end - min(cfg.horizons):
        folds.append({"k": len(folds), "train_lo": s - train, "test_lo": s, "test_hi": min(s + cfg.test_days, end),
                      "end": end})
        s += cfg.test_days
    if not folds:
        raise SystemExit("no walk-forward fold fits before the holdout")
    return cal, pos, folds, train, avail


def level_choices(sel, groups, sids):
    return {"stock": {sid: sel["stock"].get(sid, []) for sid in sids},
            "group": {sid: sel["group"].get(groups.get(sid), []) for sid in sids},
            "uniform": {sid: sel["uniform"] for sid in sids}}


def nested_choose(events, lo, hi, groups_at, cfg, sids):
    """Train-only level choice. Train = in-universe events with p in [lo, hi) and exit < hi.
    Inner split at v = hi - test_days: select on [lo, v) (exit < v), validate on [v, hi).
    Each stock takes the level with the best validation xnet (DEFAULT_LEVEL if no validation trades),
    then that level's choices are re-selected on the full train window.  Never looks at p >= hi."""
    ev = events[events["in_u"]]
    tr = ev[(ev["p"] >= lo) & (ev["p"] < hi) & (ev["xp"] < hi)]
    v = hi - cfg.test_days
    g_in = groups_at(v)
    s_in = ev_mod.select(tr[(tr["p"] < v) & (tr["xp"] < v)], g_in, cfg, "buy")
    val = tr[tr["p"] >= v]
    # same objective as candidate selection (excess per holding day, xpd) and a minimum of validation
    # evidence: a level needs >= MIN_VAL_TRADES validation trades to compete, otherwise DEFAULT_LEVEL
    vscore = {}
    for lv, ch in level_choices(s_in, g_in, sids).items():
        vt = ev_mod.strategy_trades(val, ch, "buy").groupby("stock_id")["xpd"].agg(["mean", "size"])
        vscore[lv] = vt.loc[vt["size"] >= MIN_VAL_TRADES, "mean"]
    g_full = groups_at(hi)
    s_full = ev_mod.select(tr, g_full, cfg, "buy")
    full = level_choices(s_full, g_full, sids)
    levels, choices = {}, {}
    for sid in sids:
        cands = [(vscore[lv].get(sid), lv) for lv in LEVELS if pd.notna(vscore[lv].get(sid, np.nan))]
        lv = max(cands)[1] if cands else DEFAULT_LEVEL
        if not full[lv][sid]:                       # chosen level has nothing on the full window → next level
            lv = next((x for x in LEVELS if full[x][sid]), lv)
        levels[sid], choices[sid] = lv, full[lv][sid]
    return levels, choices, s_full, g_full, tr


def run(args):
    t0 = time.time()
    cfg = Cfg(horizons=tuple(int(x) for x in args.horizons.split(",")), train_days=args.train_days,
              min_train_days=args.min_train_days, test_days=args.test_days, n_min=args.n_min,
              fdr_q=args.fdr_q, shrink_k=args.shrink_k, min_train_events=args.min_train_events,
              rebalance_days=args.rebalance_days)
    costs = ev_mod.Costs(discount=args.fee_discount)
    prep = prepare(args, args.universe, args.years, end_date=args.end_date, cfg=cfg)
    events, _ = build_events(prep, cfg, costs, args.workers)
    log(f"[events] {len(events):,} events ({int(events['in_u'].sum()):,} point-in-time in universe) in {time.time() - t0:.0f}s")
    cal, pos, folds, train_days, avail = make_folds(prep, cfg)
    events["p"] = events["date"].map(pos)
    events["xp"] = events["exit_date"].map(pos)
    ret_panel = pd.DataFrame({sid: g.set_index("date")["aclose"].pct_change()
                              for sid, g in prep["adjusted"].items()}).reindex(cal)
    meta = prep["meta"]
    groups_at = lambda p: ev_mod.assign_groups(ret_panel, meta, p, train_days)  # noqa: E731
    log(f"[folds] {len(folds)} folds, train {train_days}d (inner validation {cfg.test_days}d), "
        f"test {cfg.test_days}d, history {avail}d")
    all_sids = list(prep["adjusted"])
    trades, sell_trades, hold_trades = [], [], []
    busy = {}                     # (strategy) → {stock: exit position of its last OOS trade}, carried across folds
    sell_by_fold = {}             # fold → {stock: (sell signal, h)} chosen on that fold's train window
    choices_by_fold = {}          # fold → (adaptive choices {stock: [(signal, h), …]}, fold dict)
    for fd in folds:
        lo, hi, th = fd["train_lo"], fd["test_lo"], fd["test_hi"]
        levels, choices, s_full, groups, tr = nested_choose(events, lo, hi, groups_at, cfg, all_sids)
        choices_by_fold[fd["k"]] = (choices, fd)
        # a fold trade must also EXIT before the holdout starts (otherwise it would consume holdout data)
        test = events[(events["p"] >= hi) & (events["p"] < th) & (events["xp"] < fd["end"])]
        te = test[test["in_u"]]
        strategies = level_choices(s_full, groups, all_sids)
        strategies["adaptive"] = choices
        for name, ch in strategies.items():
            t = ev_mod.strategy_trades(te, ch, "buy", busy.setdefault(name, {}))
            trades.append(t.assign(strategy=name, fold=fd["k"], group=t["stock_id"].map(groups),
                                   level=t["stock_id"].map(levels) if name == "adaptive" else name))
        hs = [h for h in prep["holdings"]]
        ht = ev_mod.strategy_trades(test[~test["in_u"] & test["stock_id"].isin(hs)], {h: choices[h] for h in hs},
                                    "buy", busy.setdefault("_hold", {}))
        hold_trades.append(ht.assign(fold=fd["k"]))
        ssel = ev_mod.select(tr, groups, cfg, "sell")
        sell_by_fold[fd["k"]] = {sid: (ssel["stock"].get(sid) or ssel["group"].get(groups.get(sid))
                                       or [DEFAULT_SELL])[0] for sid in all_sids}
        st = ev_mod.strategy_trades(te, ssel["stock"], "sell", busy.setdefault("_sell", {}))
        sell_trades.append(st.assign(fold=fd["k"], avoid=-st["gross"], xavoid=-st["excess"]))
        lv_cnt = pd.Series(levels).loc[lambda s: s.index.isin(set(te["stock_id"]))].value_counts().to_dict()
        log(f"[fold {fd['k']}] train {cal[lo]}..{cal[hi - 1]} test {cal[hi]}..{cal[th - 1]}: "
            f"uniform={s_full['uniform']} levels={lv_cnt}")
    trades = pd.concat(trades, ignore_index=True)
    sell_trades = pd.concat(sell_trades, ignore_index=True)
    hold_trades = pd.concat(hold_trades, ignore_index=True)
    # the deployed policy as an event-driven simulation (re-entries after early exits, state across folds,
    # bounded at the holdout start) — the reliability gates are judged on THIS record
    dep_trades = deployed_trades(events, choices_by_fold, sell_by_fold, prep, cal, costs)
    holdout = holdout_eval(events, cal, groups_at, cfg, all_sids, train_days, prep, costs)

    # production choice: same nested procedure on the most recent window (all completed events)
    n = len(cal)
    out_sids = list(dict.fromkeys(prep["current"] + prep["holdings"]))
    flevels, fchoices, fsel, groups, final_tr = nested_choose(events, n - train_days, n, groups_at, cfg, out_sids)
    fsell = ev_mod.select(final_tr, groups, cfg, "sell")
    params = decide(prep, cfg, trades, hold_trades, sell_trades, flevels, fchoices, fsel, fsell, groups,
                    events, train_days, dep_trades)
    write_outputs(args, cfg, prep, params, trades, sell_trades, folds, cal, train_days, avail, fsel, time.time() - t0,
                  holdout, dep_trades)
    return params, trades


DEFAULT_SELL = ("s_atr_25", 10)
DEPLOY_STOP_ATR = 2.5   # initial stop = entry − 2.5 × ATR (same as score_today's stop_pct)


def deployed_trades(events, choices_by_fold, sell_by_fold, prep, cal, costs):
    """Event-driven OOS record of the deployed policy (what the tracker does live), per stock and chronologically:
    on each fold's test days take the best-ranked chosen buy signal (in universe) whenever no position is open,
    enter at the next open and exit by simulate.py (ATR stop, ATR trailing, the fold's sell signal, planned
    days, next-open execution); the position state carries across folds; bars from the holdout on are cut off,
    and a trade that has not closed before the holdout is dropped. Excess = net − equal-weight universe
    open-to-open return over the same window."""
    from tuning.simulate import simulate_position
    if not choices_by_fold:
        return pd.DataFrame()
    end = next(iter(choices_by_fold.values()))[1]["end"]
    cut = cal[end] if end < len(cal) else None
    pos = {d: i for i, d in enumerate(cal)}
    sids = list(prep["adjusted"])
    col = {s: j for j, s in enumerate(sids)}
    pa = pd.DataFrame({sid: g.set_index("date")["aopen"] for sid, g in prep["adjusted"].items()}) \
        .reindex(cal)[sids].to_numpy(float)
    inu = prep.get("inu")
    if inu is not None:                    # point-in-time universe: members known on each signal date
        mem = {d: np.array([col[s] for s in grp["stock_id"] if s in col], int)
               for d, grp in inu.groupby("date")}
        member = set(zip(inu["date"], inu["stock_id"]))
    else:
        mem, member = None, None
    rows = []
    for sid in sids:
        if not any(ch.get(sid) for ch, _ in choices_by_fold.values()):
            continue
        g = prep["adjusted"][sid]
        g = g[g["date"] < cut].reset_index(drop=True) if cut else g.reset_index(drop=True)
        if len(g) < 60:
            continue
        # candidates from the CAUSAL buy signals (not from labelled events, which need a future fixed exit)
        f = ind.features(g)
        buys, sells = ind.buy_signals(f), ind.sell_signals(f)
        dates = g["date"].astype(str).to_numpy()
        gpos = np.array([pos.get(d, -1) for d in dates])
        atr, ac = f["atr"].to_numpy(float), g["aclose"].to_numpy(float)
        etf = bool(prep["meta"][sid]["etf"])
        busy = -1                         # calendar position of the last exit (open of that session)
        for k in sorted(choices_by_fold):
            ch, fd = choices_by_fold[k]
            chosen = [(s, h) for s, h in (ch.get(sid) or []) if s in buys]
            if not chosen:
                continue
            rule, _ = sell_by_fold.get(k, {}).get(sid, DEFAULT_SELL)
            mult = ind.ATR_MULT.get(rule, DEPLOY_STOP_ATR)       # = params stop_atr_mult used by score_today
            fired = np.column_stack([buys[s].to_numpy(bool) for s, _ in chosen])
            window = (gpos >= fd["test_lo"]) & (gpos < fd["test_hi"])
            for t in np.flatnonzero(window & fired.any(axis=1)):
                # same rule as the live tracker: a stock exited on session X is not picked again on X's close
                if gpos[t] <= busy:
                    continue
                if member is not None and (dates[t], sid) not in member:
                    continue
                s, h = chosen[int(np.argmax(fired[t]))]          # best-ranked chosen signal that fired
                stop_pct = -mult * atr[t] / ac[t] if np.isfinite(atr[t]) and ac[t] > 0 else -0.08
                sim = simulate_position(g, f, sells, t + 1, int(h), stop_pct, None, rule, mult)
                if sim["status"] == "skipped":
                    # live: the skipped pick is recorded with exit_date = that session → not re-picked on its close
                    busy = gpos[sim["exit_i"]] if sim.get("exit_i") is not None else gpos[min(t + 1, len(g) - 1)]
                    continue
                if sim["status"] != "closed":             # still open when the holdout starts → not counted
                    busy = 10 ** 9
                    break
                e_p, x_p = gpos[sim["entry_i"]], gpos[sim["exit_i"]]
                cols = mem.get(dates[t]) if mem is not None else None
                with np.errstate(invalid="ignore", divide="ignore"):
                    r_all = pa[x_p] / pa[e_p] - 1
                    bench = float(np.nanmean(r_all[cols] if cols is not None and len(cols) else r_all))
                net = float(costs.net(sim["ret"], etf))
                rows.append({"stock_id": sid, "fold": k, "date": dates[t], "signal": s, "h": int(h),
                             "p": int(gpos[t]), "entry_date": dates[sim["entry_i"]], "exit_date": dates[sim["exit_i"]],
                             "xp": int(x_p), "dep_net": net, "bench": bench, "dep_x": net - bench,
                             "dep_reason": sim["reason"]})
                busy = x_p
    return pd.DataFrame(rows)


def holdout_eval(events, cal, groups_at, cfg, sids, train_days, prep, costs):
    """Choose on the train window that ends where the holdout starts, then trade the untouched holdout once."""
    n, H = len(cal), cfg.holdout_days
    if H <= 0 or n - H - train_days < 0:
        return None
    levels, choices, s_full, groups, tr = nested_choose(events, n - H - train_days, n - H, groups_at, cfg, sids)
    te = events[(events["p"] >= n - H) & events["in_u"]]
    out = {}
    for name, ch in {**level_choices(s_full, groups, sids), "adaptive": choices}.items():
        t = ev_mod.strategy_trades(te, ch, "buy")
        out[name] = ev_mod.summarize(t) if len(t) else None
    return {"start": cal[n - H], "end": cal[-1], "by_strategy": out}


def fold_stability(tr, col="xnet"):
    fm = tr.groupby("fold")[col].mean()
    return int(len(fm)), int((fm > 0).sum())


def one_sided_p(t):
    return 0.5 * math.erfc(t / math.sqrt(2)) if np.isfinite(t) else 1.0


def decide(prep, cfg, trades, hold_trades, sell_trades, flevels, fchoices, fsel, fsell, groups, events, train_days,
           dep_trades=None):
    """Reliability is judged on the DEPLOYED-policy OOS record (deployed_trades: event-driven, the exits the
    tracker uses; level choice used train data only): n >= n_min trades, mean net > 0, mean excess > 0,
    >= stable_frac of folds with positive excess, and the stock's one-sided block-bootstrap p-value of the excess
    passes Benjamini–Hochberg across ALL evaluated stocks. Fixed-horizon OOS stats are kept as diagnostics."""
    cand = fsel["cand"]
    as_of = prep["calendar"][-1]
    cur = set(prep["current"])
    ad = trades[trades["strategy"] == "adaptive"]
    by_stock = dict(tuple(ad.groupby("stock_id"))) if len(ad) else {}
    dep = dep_trades if dep_trades is not None and len(dep_trades) else pd.DataFrame(
        columns=["stock_id", "fold", "date", "dep_net", "dep_x"])
    dep_by = dict(tuple(dep.groupby("stock_id"))) if len(dep) else {}
    hold_by = dict(tuple(hold_trades.groupby("stock_id"))) if len(hold_trades) else {}
    summ = {}
    for sid in prep["adjusted"]:
        t = by_stock.get(sid, ad.iloc[:0])
        s = ev_mod.summarize(t)                         # fixed-horizon diagnostics
        dt = dep_by.get(sid)
        dn = int(len(dt)) if dt is not None else 0
        dfm = dt.groupby("fold")["dep_x"].mean() if dn else pd.Series(dtype=float)
        if dn >= cfg.n_min:
            xs = dt.sort_values("date")["dep_x"].to_numpy()
            p_oos = ev_mod.block_boot_p(xs, seed=int(sid) if sid.isdigit() else 0)
        else:
            p_oos = 1.0
        s.update(dep_n=dn, dep_mean=float(dt["dep_net"].mean()) if dn else float("nan"),
                 dep_x=float(dt["dep_x"].mean()) if dn else float("nan"),
                 dep_win=float((dt["dep_net"] > 0).mean()) if dn else float("nan"),
                 folds=int(len(dfm)), folds_pos=int((dfm > 0).sum()), p_oos=p_oos)
        summ[sid] = s
    # BH family = every stock evaluated under the deployed policy (current members + any stock with deployed or
    # fixed-horizon OOS trades); insufficient samples stay in the family with p = 1
    tested = [sid for sid in summ if sid in cur or sid in dep_by or by_stock.get(sid) is not None]
    rej = ev_mod.bh_reject([summ[s]["p_oos"] for s in tested], cfg.fdr_q)
    bh = dict(zip(tested, rej))
    by_events = dict(tuple(events[events["side"] == "buy"].groupby("stock_id")))
    sell_by = dict(tuple(sell_trades.groupby("stock_id"))) if len(sell_trades) else {}
    out = []
    for sid in dict.fromkeys(prep["current"] + prep["holdings"]):
        g = prep["adjusted"][sid]
        m = prep["meta"][sid]
        grp = groups.get(sid)
        s = summ[sid]
        in_u = sid in cur
        chosen = fchoices.get(sid, [])
        dep_mean = s["dep_mean"]
        gates = {"in_universe": in_u, "n_min": s["dep_n"] >= cfg.n_min,
                 "mean_pos": bool(np.isfinite(s["dep_mean"]) and s["dep_mean"] > 0),
                 "xnet_pos": bool(np.isfinite(s["dep_x"]) and s["dep_x"] > 0),
                 "stable": s["folds"] > 0 and s["folds_pos"] / s["folds"] >= cfg.stable_frac,
                 "oos_bh": bool(bh.get(sid, False)), "has_signals": bool(chosen)}
        gates_all = all(gates.values())
        # exploratory runs (inference not yet calibrated, deployed exits not yet simulated — see TUNING.md)
        # never claim reliability; the gate outcome is kept as a diagnostic (gates_all)
        reliable = gates_all and not cfg.exploratory
        level = flevels.get(sid, "none") if chosen else "none"
        conf = 0.0
        if reliable:
            conf = (LEVEL_W[level] * (s["dep_n"] / (s["dep_n"] + cfg.shrink_k)) * (s["folds_pos"] / s["folds"])
                    * (1 - s["p_oos"]))
        c_rows = cand[cand["stock_id"] == sid].set_index(["signal", "h"]) if len(cand) else None
        sigs = []
        for sg, h in chosen:
            r = c_rows.loc[(sg, h)] if c_rows is not None and (sg, h) in c_rows.index else None
            sigs.append({"signal": sg, "h": int(h), "desc": ind.BUY_DESC.get(sg, sg), "group": ind.SIGNAL_GROUP.get(sg),
                         "train_n": int(r["n"]) if r is not None else None,
                         "train_mean_xpd": float(r["mean"]) if r is not None else None,
                         "train_t": float(r["t"]) if r is not None else None,
                         "train_fdr_pass": bool(r["fdr_pass"]) if r is not None else None})
        evs = by_events.get(sid)
        ce = evs[[k in set(chosen) for k in zip(evs["signal"], evs["h"])]] if chosen and evs is not None else None
        tp = float(ce["mfe"].quantile(0.6)) if ce is not None and len(ce) >= 8 else None
        mae20 = float(ce["mae"].quantile(0.2)) if ce is not None and len(ce) >= 8 else None
        sell_choice = (fsell["stock"].get(sid) or fsell["group"].get(grp) or [("s_atr_25", 10)])[0]
        so = sell_by.get(sid, sell_trades.iloc[:0])
        ht = hold_by.get(sid)
        q, cv = prep["quality"].loc[sid], prep["coverage"].loc[sid]
        out.append({
            "stock_id": sid, "name": m["name"], "market": m["market"], "etf": m["etf"], "group": grp,
            "universe_rank": prep["rank"].get(sid), "in_universe": in_u, "holding": sid in prep["holdings"],
            "level": level, "reliable": reliable, "gates_all": gates_all, "gates": gates,
            "group_fallback": level in ("group", "uniform"),
            "label": {"stock": "個股專屬訊號", "group": "同組訊號", "uniform": "全市場統一訊號",
                      "none": "no reliable signal"}[level] if reliable else "no reliable signal",
            "buy_signals": sigs, "best_h": int(chosen[0][1]) if chosen else None,
            "oos_n": s["n"], "oos_n_eff": s["n_eff"], "oos_mean": s["mean"], "oos_median": s["median"],
            "oos_win": s["win"], "oos_t": s["t"], "oos_excess": s["excess"], "oos_xnet": s["xnet"], "oos_xt": s["xt"],
            "oos_p": s["p_oos"], "oos_mae_med": s["mae_med"], "oos_mfe_med": s["mfe_med"],
            "oos_deployed_mean": dep_mean if np.isfinite(dep_mean) else None,
            "oos_deployed_n": s["dep_n"], "oos_deployed_x": s["dep_x"] if np.isfinite(s["dep_x"]) else None,
            "oos_deployed_win": s["dep_win"] if np.isfinite(s["dep_win"]) else None,
            "oos_folds": s["folds"], "oos_folds_pos": s["folds_pos"],
            "holding_only_oos": ev_mod.summarize(ht) if ht is not None else None,
            "confidence": round(conf, 4), "tp_pct": tp, "mae_p20": mae20,
            "sell_rule": {"signal": sell_choice[0], "h": int(sell_choice[1]), "desc": ind.SELL_DESC[sell_choice[0]],
                          "oos_n": int(len(so)), "oos_avoid_mean": float(so["avoid"].mean()) if len(so) else None,
                          "oos_xavoid_mean": float(so["xavoid"].mean()) if len(so) else None},
            "stop_atr_mult": ind.ATR_MULT.get(sell_choice[0], 2.5),
            "data_quality": {"history_days": int(len(g)), "first_date": g["date"].iloc[0],
                             "yf_only_rows": int(q["yf_only_rows"]), "yf_unanchored_rows": int(q["yf_unanchored_rows"]),
                             "dropped_copies": int(q["rows_dropped_copies"]), "chip_valid_rows": int(q["chip_valid_rows"]),
                             "adj_events": int(cv["events_applied"]), "adj_source": cv["finmind_status"],
                             "unexplained_gaps": int(cv["unexplained_gaps"]),
                             "markers_unexplained": int(cv["markers_unexplained"])},
            "short_history": train_days < 504 or len(g) < 504 + 63,
            "n_min_lowered": cfg.n_min < 30, "train_days": train_days, "as_of": as_of,
        })
    return out


def json_safe(o):
    """Recursively convert numpy scalars and NaN/inf → JSON-safe values (NaN/inf → None)."""
    if isinstance(o, dict):
        return {str(k): json_safe(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [json_safe(v) for v in o]
    if isinstance(o, (np.bool_, bool)):
        return bool(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (float, np.floating)):
        return float(o) if np.isfinite(o) else None
    if o is None or isinstance(o, (int, str)):
        return o
    return str(o)


def _fmt(x, pct=True, d=2):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return "–"
    return f"{x * 100:.{d}f}%" if pct else f"{x:.{d}f}"


def write_outputs(args, cfg, prep, params, trades, sell_trades, folds, cal, train_days, avail, fsel, runtime,
                  holdout=None, dep_trades=None):
    os.makedirs(args.out_dir, exist_ok=True)
    doc = json_safe({"generated_at": datetime.now().isoformat(timespec="seconds"),
                     "config": asdict(cfg) | {"source": args.source, "universe": args.universe, "years": args.years,
                                              "fee_discount": args.fee_discount, "adjust_source": args.adjust_source,
                                              "execution": "entry open t+1, exit open t+h+1"},
                     "stocks": params})
    with open(os.path.join(args.out_dir, "stock_params.json"), "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1, allow_nan=False)
    flat = pd.DataFrame(doc["stocks"])
    for c in ("buy_signals", "sell_rule", "data_quality", "gates", "holding_only_oos"):
        flat[c] = flat[c].map(lambda v: json.dumps(v, ensure_ascii=False, allow_nan=False))
    flat.to_parquet(os.path.join(args.out_dir, "stock_params.parquet"), index=False)
    trades.to_parquet(os.path.join(args.out_dir, "oos_trades.parquet"), index=False)
    if dep_trades is not None and len(dep_trades):          # the record the reliability gates are based on
        dep_trades.to_parquet(os.path.join(args.out_dir, "deployed_trades.parquet"), index=False)
    prep["coverage"].to_csv(os.path.join(args.out_dir, "adjust_coverage.csv"), encoding="utf-8-sig")
    prep["quality"].to_csv(os.path.join(args.out_dir, "data_quality.csv"), encoding="utf-8-sig")

    L = [f"# 個股訊號調校報告 (as of {cal[-1]})\n"]
    L.append(f"- 產生時間 {datetime.now():%Y-%m-%d %H:%M}，執行 {runtime:.0f} 秒；來源 `{args.source}`；還原 `{args.adjust_source}`；"
             f"輸出 {len(params)} 檔（目前前 {args.universe} 成交值 + 持股 {len(prep['holdings'])} 檔）")
    L.append(f"- 選股池為 point-in-time：每 {cfg.rebalance_days} 個交易日依前 {cfg.turnover_lookback} 日平均成交值重排，"
             f"共 {len(prep['schedule'])} 次、歷來入選 {len(set(prep['inu']['stock_id']))} 檔；持股不參與評估")
    L.append(f"- 交易日 {cal[0]}..{cal[-1]}（{len(cal)} 日，可用 {avail} 日）；walk-forward：train {train_days} 日"
             f"（內層驗證 {cfg.test_days} 日）、test {cfg.test_days} 日、{len(folds)} folds；horizons {list(cfg.horizons)}（交易列數）")
    flags = []
    if train_days < 504:
        flags.append(f"**SHORT HISTORY**：train window 只有 {train_days} 日（< 2 年），結果不可靠")
    if cfg.n_min < 30:
        flags.append(f"**n_min 已調降為 {cfg.n_min}**（預設 30）")
    if len(folds) < 4:
        flags.append(f"**只有 {len(folds)} 個 OOS fold**，穩定度判斷（≥2/3 folds 為正）統計力很弱")
    L += [f"- ⚠ {fl}" for fl in flags] + [""]
    L.append("## OOS 比較（point-in-time 選股池；同一檔股票不重疊持有）\n")
    L.append("| 策略 | 交易數 | 有效 n(叢集) | 平均淨報酬 | 中位數 | 勝率 | 淨超額 | 超額 t(叢集) | 各 fold 淨超額 |")
    L.append("|---|---:|---:|---:|---:|---:|---:|---:|---|")
    names = {"adaptive": "adaptive（train 內選層級）", "stock": "個股專屬", "group": "同組", "uniform": "全市場統一 baseline"}
    for name in ("adaptive", "stock", "group", "uniform"):
        t = trades[trades["strategy"] == name]
        s = ev_mod.summarize(t, pooled=True)
        fm = t.groupby("fold")["xnet"].mean()
        L.append(f"| {names[name]} | {s['n']} | {s['n_eff']} | {_fmt(s['mean'])} | {_fmt(s['median'])} | {_fmt(s['win'], d=1)} | "
                 f"{_fmt(s['xnet'])} | {_fmt(s['xt'], False)} | {' / '.join(_fmt(v) for v in fm)} |")
    per = trades.groupby(["strategy", "stock_id"])["xnet"].mean().unstack(0)
    if {"stock", "uniform"} <= set(per.columns):
        both = per[["stock", "uniform"]].dropna()
        L.append(f"\n個股層級配對（兩者皆有 OOS 交易 {len(both)} 檔）：個股專屬勝過 baseline 比例 "
                 f"{_fmt((both['stock'] > both['uniform']).mean(), d=1)}，平均差 {_fmt((both['stock'] - both['uniform']).mean())}")
    st_mean = ev_mod.summarize(trades[trades["strategy"] == "stock"])["xnet"]
    un_mean = ev_mod.summarize(trades[trades["strategy"] == "uniform"])["xnet"]
    ad_mean = ev_mod.summarize(trades[trades["strategy"] == "adaptive"])["xnet"]
    better = np.isfinite(st_mean) and np.isfinite(un_mean) and st_mean > un_mean
    L.append(f"\n結論（本次資料）：個股專屬選擇 **{'優於' if better else '未優於'}** 全市場統一 baseline"
             f"（OOS 每筆淨超額 {_fmt(st_mean)} vs {_fmt(un_mean)}；adaptive {_fmt(ad_mean)}）。")
    ad = trades[trades["strategy"] == "adaptive"]
    if len(ad):
        L.append("adaptive 各層級 OOS：" + "，".join(
            f"{lv} n={len(d)} 淨超額 {_fmt(d['xnet'].mean())}" for lv, d in ad.groupby("level")))
    if dep_trades is not None and len(dep_trades):
        dp = dep_trades
        fm = dp.groupby("fold")["dep_x"].mean()
        L.append("\n## 實際部署規則的 OOS（事件驅動：初始停損 ATR、移動停損、該 fold 出場訊號、到期；隔日開盤執行；"
                 "提早出場後可再進場；可靠度閘門以此為準）\n")
        L.append(f"- 交易 {len(dp)} 筆：平均淨報酬 {_fmt(dp['dep_net'].mean())}，勝率 {_fmt((dp['dep_net'] > 0).mean(), d=1)}，"
                 f"淨超額 {_fmt(dp['dep_x'].mean())}（固定持有天數版 adaptive 淨超額 {_fmt(ad_mean)}）")
        L.append("- 各 fold 淨超額：" + " / ".join(_fmt(v) for v in fm))
        why = dp["dep_reason"].fillna("").str.replace(r"（.*", "", regex=True).str.replace(r"持有 \d+ 日.*", "到期", regex=True)
        L.append("- 出場原因：" + "，".join(f"{k or '?'} {v}" for k, v in why.value_counts().items()))
    if holdout:
        L.append(f"\n## 保留驗證期（{holdout['start']}..{holdout['end']}，不參與任何 fold，只在這裡用一次）\n")
        L.append("| 策略 | 交易數 | 平均淨報酬 | 勝率 | 淨超額 |\n|---|---:|---:|---:|---:|")
        for name in ("adaptive", "stock", "group", "uniform"):
            s = holdout["by_strategy"].get(name)
            L.append(f"| {names[name]} | {s['n'] if s else 0} | {_fmt(s['mean']) if s else '–'} | "
                     f"{_fmt(s['win'], d=1) if s else '–'} | {_fmt(s['xnet']) if s else '–'} |")
    df = pd.DataFrame(params)
    gate_df = pd.DataFrame(list(df["gates"]))
    L.append("\n## 可靠度閘門（只用 OOS；BH 跨全部股票）\n")
    L.append("| 閘門 | 通過檔數 |\n|---|---:|")
    for c in gate_df.columns:
        L.append(f"| {c} | {int(gate_df[c].sum())} |")
    L.append(f"\n可靠（reliable）{int(df['reliable'].sum())} / {len(df)} 檔；持股 {int(df['holding'].sum())} 檔中可靠 "
             f"{int((df['holding'] & df['reliable']).sum())} 檔。最終 level 分佈：{df['level'].value_counts().to_dict()}")
    L.append(f"\n最終全市場統一訊號：{fsel['uniform']}")
    all_cnt = pd.Series([s["signal"] for p in params for s in p["buy_signals"]]).value_counts()
    L.append("\n## 最常被選中的訊號（最終 train window，含不可靠者）\n")
    L.append("| 訊號 | 說明 | 被選次數 |\n|---|---|---:|")
    for s, c in all_cnt.head(15).items():
        L.append(f"| {s} | {ind.BUY_DESC.get(s, s)} | {c} |")
    L.append("\n最佳 horizon 分佈：" + "，".join(f"{int(h)} 日 {c} 檔" for h, c in df["best_h"].dropna().value_counts().sort_index().items()))
    sell_cnt = pd.Series([p["sell_rule"]["signal"] for p in params]).value_counts()
    L.append("\n出場規則分佈：" + "，".join(f"{s} {c}" for s, c in sell_cnt.items()))
    if len(sell_trades):
        L.append(f"；OOS 出場後 h 日平均避開報酬 {_fmt(sell_trades['avoid'].mean())}，相對大盤 {_fmt(sell_trades['xavoid'].mean())}"
                 f"（n={len(sell_trades)}，正值代表出場有利）")
    cov = prep["coverage"]
    L.append("\n## 還原權值覆蓋率\n")
    L.append(f"- 事件來源狀態：{cov['finmind_status'].value_counts().to_dict()}")
    L.append(f"- 套用事件 {int(cov['events_applied'].sum())}（外部來源 {int(cov['events_finmind'].sum())}，"
             f"僅由參考價推得 {int(cov['events_implied_only'].sum())}，外部/參考價不一致 {int(cov['factor_disagree'].sum())}）")
    L.append(f"- 除權息標記未被解釋：{int(cov['markers_unexplained'].sum())} 筆 / {int((cov['markers_unexplained'] > 0).sum())} 檔；"
             f"未解釋跳空(>10.5%)：{int(cov['unexplained_gaps'].sum())} 筆 / {int((cov['unexplained_gaps'] > 0).sum())} 檔；"
             f"還原後仍 >10.5% 單日變動 {int(cov['adj_gaps_remaining'].sum())} 筆")
    bad = cov[(cov["unexplained_gaps"] > 0) | (cov["markers_unexplained"] > 0)]
    if len(bad):
        L.append("- 需人工確認：" + "；".join(
            f"{i} {r['unexplained_gap_dates'] if isinstance(r['unexplained_gap_dates'], str) else ''} "
            f"{r['marker_unexplained_dates'] if isinstance(r['marker_unexplained_dates'], str) else ''}".strip()
            for i, r in bad.head(20).iterrows()))
    q = prep["quality"]
    L.append(f"- 價格清理：刪除複製列 {int(q['rows_dropped_copies'].sum())}、非交易日 {int(q['rows_dropped_noncalendar'].sum())}、"
             f"舊零散段 {int(q['rows_dropped_old_segment'].sum())}；僅 yfinance 的日期 {int(q['yf_only_rows'].sum())} 列"
             f"（無官方錨點 {int(q['yf_unanchored_rows'].sum())}）")
    L.append("\n## 前 20 名（依 confidence，再依 OOS 淨超額；n 小者僅供參考）\n")
    L.append("| 代號 | 名稱 | level | 訊號 | h | OOS n | OOS 平均淨 | 淨超額 | p | conf |\n|---|---|---|---|---:|---:|---:|---:|---:|---:|")
    xs = lambda p: p["oos_xnet"] if p["oos_xnet"] is not None and np.isfinite(p["oos_xnet"]) else -1.0  # noqa: E731
    for p in sorted([p for p in params if p["oos_n"]], key=lambda p: (-p["confidence"], -xs(p)))[:20]:
        L.append(f"| {p['stock_id']} | {p['name']} | {p['level']} | {', '.join(s['signal'] for s in p['buy_signals'])} | "
                 f"{p['best_h'] or '–'} | {p['oos_n']} | {_fmt(p['oos_mean'])} | {_fmt(p['oos_xnet'])} | "
                 f"{_fmt(p['oos_p'], False, 3)} | {p['confidence']:.2f} |")
    L.append("\n## 限制\n")
    L.append("- 歷史長度不足時 OOS 只有少數 fold，數字僅供驗證流程，**不可作為投資依據**。")
    L.append("- 選股池雖為 point-in-time，但資料只含目前仍有報價紀錄的代號；已下市股票若不在資料中，仍有存活者偏差。")
    L.append("- 還原權值依賴外部除權息資料與交易所參考價；未解釋缺口僅標記不修補。yfinance 補值列可能有誤。")
    L.append("- 籌碼資料來源混雜（local_backfill / T86 / TPEx），已做基本檢查（|買超| ≤ 成交量、去除重複值）。")
    with open(os.path.join(args.out_dir, "tuning_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log(f"[out] wrote {args.out_dir}/stock_params.json|parquet, tuning_report.md ({runtime:.0f}s)")


if __name__ == "__main__":
    run(parse_args())
