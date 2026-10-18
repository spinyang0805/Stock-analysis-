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
import re
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


def read_cache(path):
    """Cached JSON or None (missing / corrupt → None, caller re-fetches)."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) and isinstance(obj.get("data"), list) else None
    except (OSError, ValueError):
        log(f"[adjust] corrupt cache ignored: {os.path.basename(path)}")
        return None


def write_cache(path, data):
    """Atomic write: temp file in the same directory + os.replace."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"_fetched_at": time.time(), "data": data}, f, ensure_ascii=False)
    os.replace(tmp, path)


class _Throttle:
    """Shared request policy: interval, budget, stop on quota error or after N consecutive network errors."""

    def __init__(self, budget, interval, offline, max_errors=3):
        self.budget, self.interval, self.offline, self.max_errors = budget, interval, offline, max_errors
        self.blocked, self.fetched, self.errors, self._last = False, 0, 0, 0.0

    def can_fetch(self):
        return not (self.offline or self.blocked or self.fetched >= self.budget)

    def wait(self):
        w = self.interval - (time.time() - self._last)
        if w > 0:
            time.sleep(w)
        self._last = time.time()

    def network_error(self, what, e):
        self.errors += 1
        log(f"[adjust] {what} error {type(e).__name__} ({self.errors} consecutive)")
        if self.errors >= self.max_errors:
            self.blocked = True
            log("[adjust] too many consecutive network errors → no more fetches this run")

    def ok(self):
        self.errors = 0
        self.fetched += 1


class FinMindFetcher(_Throttle):
    def __init__(self, budget=300, interval=2.0, ttl_days=7, offline=False):
        super().__init__(budget, interval, offline)
        self.ttl = ttl_days * 86400

    def _path(self, dataset, code):
        return os.path.join(FINMIND_CACHE, dataset, f"{code or '_all'}.json")

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
        cached = read_cache(p)
        if cached is not None and (time.time() - cached.get("_fetched_at", 0) < self.ttl or not self.can_fetch()):
            return cached["data"], "cached"
        fallback = (cached["data"], "cached") if cached is not None else ([], "unavailable")
        if not self.can_fetch():
            return fallback
        self.wait()
        params = {"dataset": dataset, "start_date": start_date}
        if code:
            params["data_id"] = code
        headers = {}
        if os.environ.get("FINMIND_TOKEN"):
            headers["Authorization"] = "Bearer " + os.environ["FINMIND_TOKEN"]
        try:
            j = requests.get(FINMIND_URL, params=params, headers=headers, timeout=30).json()
        except Exception as e:
            self.network_error(f"FinMind {dataset} {code}", e)
            return fallback
        if j.get("status") != 200:
            self.blocked = True
            log(f"[adjust] FinMind stopped after {self.fetched} fetches (status={j.get('status')})")
            return fallback
        self.ok()
        dk, bk, ak = DATASETS[dataset]
        data = [{k: d.get(k) for k in ("stock_id", dk, bk, ak)} for d in j.get("data", [])]
        write_cache(p, data)
        return data, "fetched"


EXCHANGE_CACHE = os.path.join(CACHE_DIR, "exchange")
TWSE_TWT49U = "https://www.twse.com.tw/rwd/zh/exRight/TWT49U"
TPEX_EXDAILYQ = "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ"


def roc_to_ymd(s):
    """'114年06月02日' or '114/06/02' → '20250602'."""
    digits = [int(x) for x in re.findall(r"\d+", str(s))]
    if len(digits) < 3:
        return None
    return f"{digits[0] + 1911:04d}{digits[1]:02d}{digits[2]:02d}"


class ExchangeFetcher(_Throttle):
    """Official ex-rights/ex-dividend results for all stocks, one request per market-month:
    TWSE TWT49U (除權除息計算結果表) and TPEx exDailyQ (除權除息計算結果). factor = 參考價 / 前收盤價.
    Cached under history/cache/exchange/<market>/<YYYYMM>.json; closed months are never re-fetched, the
    current month is refreshed after 1 day. Does NOT cover splits / par-value changes / capital reductions."""

    def __init__(self, budget=200, interval=3.0, offline=False):
        super().__init__(budget, interval, offline)

    def _month(self, market, ym):
        p = os.path.join(EXCHANGE_CACHE, market, f"{ym}.json")
        cached = read_cache(p)
        closed = ym < time.strftime("%Y%m")
        if cached is not None and (closed or time.time() - cached.get("_fetched_at", 0) < 86400
                                   or not self.can_fetch()):
            return cached["data"], "cached"
        fallback = (cached["data"], "cached") if cached is not None else ([], "unavailable")
        if not self.can_fetch():
            return fallback
        self.wait()
        y, m = int(ym[:4]), int(ym[4:])
        last = (pd.Timestamp(y, m, 1) + pd.offsets.MonthEnd(0)).day
        hdr = {"User-Agent": "Mozilla/5.0"}
        try:
            if market == "twse":
                j = requests.get(TWSE_TWT49U, params={"startDate": f"{ym}01", "endDate": f"{ym}{last:02d}",
                                                      "response": "json"}, headers=hdr, timeout=30).json()
                stat = str(j.get("stat", ""))
                raw = j.get("data", []) if stat == "OK" else ([] if "沒有符合" in stat else None)
            else:
                j = requests.get(TPEX_EXDAILYQ, params={"startDate": f"{y}/{m:02d}/01",
                                                        "endDate": f"{y}/{m:02d}/{last:02d}", "response": "json"},
                                 headers=hdr, timeout=30).json()
                tables = j.get("tables") or []
                raw = tables[0].get("data", []) if tables else []
                stat = "OK"
        except Exception as e:
            self.network_error(f"exchange {market} {ym}", e)
            return fallback
        if raw is None:
            self.network_error(f"exchange {market} {ym}", ValueError(stat[:40]))
            return fallback
        self.ok()
        data = []
        for row in raw:  # positional on both exchanges: 0 date, 1 code, 3 前收盤價, 4 參考價
            try:
                data.append({"stock_id": str(row[1]).strip(), "date": roc_to_ymd(row[0]),
                             "before": float(str(row[3]).replace(",", "")),
                             "after": float(str(row[4]).replace(",", ""))})
            except (ValueError, IndexError, TypeError):
                continue
        write_cache(p, data)
        return data, "fetched"

    def load(self, start_ymd, end_ymd):
        """Events of both markets between the dates → (DataFrame[stock_id, date, factor], months unavailable)."""
        months = pd.period_range(pd.Timestamp(start_ymd), pd.Timestamp(end_ymd), freq="M").strftime("%Y%m")
        rows, missing = [], 0
        for ym in months:
            for mk in ("twse", "tpex"):
                d, st = self._month(mk, ym)
                missing += st == "unavailable"
                rows.extend(d)
        df = pd.DataFrame(rows, columns=["stock_id", "date", "before", "after"]).dropna()
        df = df[(df["before"] > 0) & (df["after"] > 0)].copy()
        df["factor"] = df["after"] / df["before"]
        df = df[(df["factor"] - 1).abs() > 1e-6]
        return df[["stock_id", "date", "factor"]].drop_duplicates(["stock_id", "date"], keep="last"), int(missing)


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


def adjust_all(clean, codes, fetcher, source="finmind", exchange=None):
    """Returns (dict code → adjusted DataFrame indexed 0..n-1, coverage DataFrame).
    source='finmind': per-stock TaiwanStockDividendResult (default).
    source='exchange': dividend events from ExchangeFetcher (TWSE TWT49U + TPEx exDailyQ), one request per
    market-month. In both modes ParValueChange (1 call) and the lazy split / capital-reduction lookups use FinMind."""
    out, covs = {}, []
    ex_ev = None
    if source == "exchange":
        exchange = exchange or ExchangeFetcher()
        ex_df, ex_missing = exchange.load(clean["date"].min(), clean["date"].max())
        ex_ev = ex_df.groupby("stock_id")
        log(f"[adjust] exchange events {len(ex_df)} (months unavailable: {ex_missing}, requests {exchange.fetched})")
    par, _ = fetcher.events("TaiwanStockParValueChange")
    par = par.groupby("stock_id")
    groups = {k: v for k, v in clean[clean["stock_id"].isin(set(codes))].groupby("stock_id")}
    for i, sid in enumerate(codes):
        if sid not in groups:
            continue
        g = groups[sid].sort_values("date").reset_index(drop=True)
        if ex_ev is not None:
            div = ex_ev.get_group(sid) if sid in ex_ev.groups else pd.DataFrame(columns=["stock_id", "date", "factor"])
            st = "exchange" if not ex_missing else "exchange_partial"
        else:
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
