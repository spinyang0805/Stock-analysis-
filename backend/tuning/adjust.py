"""adjust.py — dividend / split / capital-change adjustment (backward: latest price = raw price).

Event sources (FinMind public API, no token needed; free tier ~300 req/hour/IP, optional FINMIND_TOKEN):
  TaiwanStockDividendResult                  per stock, always    before_price → after_price
  TaiwanStockParValueChange                  whole market, 1 call before_close → after_ref_close
  TaiwanStockSplitPrice                      per stock, lazy*     before_price → after_price
  TaiwanStockCapitalReductionReferencePrice  per stock, lazy*     ClosingPriceonTheLastTradingDay → PostReductionReferencePrice
  (*) fetched only for stocks that still show an unexplained gap / ex-day marker → keeps request count low.
Responses cached in history/cache/finmind/<dataset>/<code>.json (TTL default 7 days), throttled; a
quota/level error stops further fetches for the run (cached + implied events are still used).

Also from the price rows themselves:
  implied : ref = close - change differs from the previous close on official rows → factor = ref / prev
  marker  : official row with change = NULL after a normal row (collector stores NULL on 除權息 days);
            used only to VERIFY coverage (a marker with no event is flagged).

factor_e = after / before;  adjusted_t = raw_t × Π factor_e (events with date_e > t)
Unexplained gaps (|raw close-to-close| > 10.5% with no event that day; TW daily limit is 10%) are
flagged in the coverage table, never patched.
"""
import json
import os
import time

import numpy as np
import pandas as pd
import requests

from .data import CACHE_DIR, log

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_CACHE = os.path.join(CACHE_DIR, "finmind")
GAP_LIMIT = 0.105
DATASETS = {  # dataset → (date field, before field, after field)
    "TaiwanStockDividendResult": ("date", "before_price", "after_price"),
    "TaiwanStockSplitPrice": ("date", "before_price", "after_price"),
    "TaiwanStockCapitalReductionReferencePrice": ("date", "ClosingPriceonTheLastTradingDay",
                                                  "PostReductionReferencePrice"),
    "TaiwanStockParValueChange": ("date", "before_close", "after_ref_close"),
}


class FinMindFetcher:
    def __init__(self, budget=300, interval=2.0, ttl_days=7, offline=False):
        self.budget, self.interval, self.ttl = budget, interval, ttl_days * 86400
        self.offline, self.blocked, self.fetched, self._last = offline, False, 0, 0.0

    def _path(self, dataset, code):
        d = os.path.join(FINMIND_CACHE, dataset)
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{code or '_all'}.json")

    def events(self, dataset, code=None, start_date="2010-01-01"):
        """(events DataFrame[stock_id, date, factor], status: cached|fetched|unavailable)."""
        rows, status = self._rows(dataset, code, start_date)
        dk, bk, ak = DATASETS[dataset]
        ev = []
        for d in rows:
            try:
                b, a = float(d.get(bk)), float(d.get(ak))
            except (TypeError, ValueError):
                continue
            if b > 0 and a > 0 and abs(a / b - 1) > 1e-6:
                ev.append({"stock_id": str(d.get("stock_id", code)), "date": str(d[dk]).replace("-", ""),
                           "factor": a / b})
        df = pd.DataFrame(ev, columns=["stock_id", "date", "factor"])
        return df.drop_duplicates(["stock_id", "date"], keep="last"), status

    def _rows(self, dataset, code, start_date):
        p = self._path(dataset, code)
        cached = None
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                cached = json.load(f)
            if time.time() - cached.get("_fetched_at", 0) < self.ttl or not self._can_fetch():
                return cached.get("data", []), "cached"
        if not self._can_fetch():
            return [], "unavailable"
        wait = self.interval - (time.time() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.time()
        params = {"dataset": dataset, "start_date": start_date}
        if code:
            params["data_id"] = code
        headers = {}
        if os.environ.get("FINMIND_TOKEN"):
            headers["Authorization"] = "Bearer " + os.environ["FINMIND_TOKEN"]
        fallback = (cached.get("data", []), "cached") if cached else ([], "unavailable")
        try:
            j = requests.get(FINMIND_URL, params=params, headers=headers, timeout=30).json()
        except Exception as e:
            log(f"[adjust] FinMind {dataset} {code} error {type(e).__name__}")
            return fallback
        if j.get("status") != 200:
            self.blocked = True
            log(f"[adjust] FinMind stopped after {self.fetched} fetches (status={j.get('status')})")
            return fallback
        self.fetched += 1
        dk, bk, ak = DATASETS[dataset]
        data = [{k: d.get(k) for k in ("stock_id", dk, bk, ak)} for d in j.get("data", [])]
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"_fetched_at": time.time(), "data": data}, f)
        return data, "fetched"

    def _can_fetch(self):
        return not (self.offline or self.blocked or self.fetched >= self.budget)


def implied_events(g):
    """Events from official rows: reference price (close - change) != previous raw close."""
    prev = g["close"].shift(1)
    ref = g["close"] - g["change"]
    ok = (~g["yf"]) & (~g["yf"].shift(1, fill_value=True)) & g["change"].notna() & prev.notna()
    hit = ok & ((ref - prev).abs() > np.maximum(0.011, prev * 0.0005)) & (ref > 0)
    return pd.DataFrame({"date": g.loc[hit, "date"].to_numpy(), "factor": (ref / prev)[hit].to_numpy()})


def adjust_stock(g, fin):
    """g: clean rows of one stock sorted by date; fin: FinMind events [date, factor] for this stock.
    Returns (g + adj columns, coverage dict)."""
    g = g.copy()
    fin = fin[(fin["date"] > g["date"].iloc[0]) & (fin["date"] <= g["date"].iloc[-1])]
    fin = fin.groupby("date", as_index=False)["factor"].prod()
    imp = implied_events(g)
    fin_d, imp_d = set(fin["date"]), set(imp["date"])
    # yfinance rows are Yahoo-adjusted for every event after them; clean_prices rescaled each block onto
    # the next official row's reference price (anchor A, post-event if A is an ex-date).
    # Raw level of a yf row at t = rescaled / Π(f_e for FinMind events with t < e <= A).
    yf_fixed = 0
    if "yf_anchor" in g and len(fin) and g["yf_rescaled"].any():
        for anchor, idx in g.index[g["yf_rescaled"]].to_series().groupby(g.loc[g["yf_rescaled"], "yf_anchor"]):
            dates = g.loc[idx, "date"]
            evs = fin[(fin["date"] > dates.min()) & (fin["date"] <= anchor)]
            for d, fct in zip(evs["date"], evs["factor"]):
                m = idx[(dates < d).to_numpy()]
                g.loc[m, ["open", "high", "low", "close"]] = g.loc[m, ["open", "high", "low", "close"]] / fct
                yf_fixed += 1
    marker = (~g["yf"]) & g["change"].isna() & g["close"].shift(1).notna() & g["change"].shift(1).notna()
    marker_d = set(g.loc[marker, "date"])
    matched = fin_d & imp_d
    disagree = 0
    if matched:
        diff = fin.set_index("date")["factor"].reindex(sorted(matched)) - \
            imp.set_index("date")["factor"].reindex(sorted(matched))
        disagree = int((diff.abs() > 0.01).sum())
    ev = pd.concat([fin[["date", "factor"]], imp[~imp["date"].isin(fin_d)]], ignore_index=True)
    # factor attached to the first row on/after the event date (ex-date may fall on a halt day)
    pos = np.searchsorted(g["date"].to_numpy(), ev["date"].to_numpy(), side="left")
    f = np.ones(len(g))
    for p_, fct in zip(pos, ev["factor"].astype(float)):
        if 0 < p_ < len(g):
            f[p_] *= fct
    cum = np.cumprod(f[::-1])[::-1]          # Π factors at index >= i
    g["adj_factor"] = np.append(cum[1:], 1.0)  # Π factors at index > i
    # unanchored yfinance rows (no usable next official row) stay on Yahoo's adjusted scale, which
    # already embeds every later event → adjusted = Yahoo value, raw = Yahoo value / adj_factor
    if "yf_unanchored" in g and g["yf_unanchored"].any():
        m = g["yf_unanchored"].to_numpy()
        for c in ["open", "high", "low", "close"]:
            g.loc[m, c] = g.loc[m, c] / g.loc[m, "adj_factor"]
    for c in ["open", "high", "low", "close"]:
        g["a" + c] = g[c] * g["adj_factor"]
    raw_ret = g["close"] / g["close"].shift(1) - 1
    gaps = (raw_ret.abs() > GAP_LIMIT) & (f == 1.0)
    adj_ret = g["aclose"] / g["aclose"].shift(1) - 1
    unexpl_m = sorted(marker_d - set(g["date"].iloc[pos[(pos > 0) & (pos < len(g))]]))
    cov = {
        "events_finmind": len(fin), "events_implied": len(imp), "events_matched": len(matched),
        "events_implied_only": len(imp_d - fin_d), "events_finmind_only": len(fin_d - imp_d),
        "factor_disagree": disagree, "yf_event_fixes": yf_fixed, "events_applied": int((f != 1.0).sum()),
        "exday_markers": len(marker_d), "markers_unexplained": len(unexpl_m),
        "marker_unexplained_dates": ",".join(unexpl_m[:10]),
        "unexplained_gaps": int(gaps.sum()), "unexplained_gap_dates": ",".join(g.loc[gaps, "date"].tolist()[:10]),
        "adj_gaps_remaining": int((adj_ret.abs() > GAP_LIMIT).sum()),
    }
    return g, cov


def adjust_all(clean, codes, fetcher):
    """Returns (dict code → adjusted DataFrame indexed 0..n-1, coverage DataFrame)."""
    out, covs = {}, []
    par, _ = fetcher.events("TaiwanStockParValueChange")
    par = par.groupby("stock_id")
    groups = {k: v for k, v in clean[clean["stock_id"].isin(set(codes))].groupby("stock_id")}
    for i, sid in enumerate(codes):
        if sid not in groups:
            continue
        g = groups[sid].sort_values("date").reset_index(drop=True)
        div, st = fetcher.events("TaiwanStockDividendResult", sid)
        fin = pd.concat([div, par.get_group(sid) if sid in par.groups else div.iloc[:0]], ignore_index=True)
        a, cov = adjust_stock(g, fin)
        extra = []
        if cov["unexplained_gaps"] or cov["markers_unexplained"]:
            for ds in ("TaiwanStockSplitPrice", "TaiwanStockCapitalReductionReferencePrice"):
                e, _ = fetcher.events(ds, sid)
                if len(e):
                    extra.append(e)
            if extra:
                a, cov = adjust_stock(g, pd.concat([fin] + extra, ignore_index=True))
        out[sid] = a
        cov.update({"stock_id": sid, "finmind_status": st, "extra_datasets_used": len(extra)})
        covs.append(cov)
        if (i + 1) % 100 == 0:
            log(f"[adjust] {i + 1}/{len(codes)} (FinMind requests this run: {fetcher.fetched})")
    return out, pd.DataFrame(covs).set_index("stock_id")
