"""data.py — load & clean daily prices / chips for the tuning engine.

Sources (explicit, never a silent fallback):
  parquet : history/prices_YYYY.parquet + history/chips_YYYY.parquet (backend/history_store.py layout)
  db      : Supabase stock_daily / chip_daily, READ ONLY session (development while history/ is thin)

Holdings always come from Supabase `transactions` (read-only) unless --holdings / --no-holdings is given.

Cleaning (documented in docs/TUNING.md):
  1. trading calendar = weekdays, minus manifest `nontrading_dates`, minus "stale" dates whose rows
     are copies of the previous date (STOCK_DAY_ALL fetched on a holiday/weekend is re-labelled).
  2. per stock: drop rows identical (OHLCV) to the previous row (stale copies).
  3. one row per (stock_id, date); official exchange rows win over yfinance rows.
  4. yfinance-only rows are auto-adjusted by Yahoo → rescaled per contiguous block onto the raw scale
     using the next official row's reference price (close - change). Unanchored blocks are flagged.
"""
import glob
import json
import os
import re
import sys

import numpy as np
import pandas as pd

TUNING_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(TUNING_DIR)
REPO_ROOT = os.path.dirname(BACKEND_DIR)
HISTORY_DIR = os.environ.get("HISTORY_DIR", os.path.join(REPO_ROOT, "history"))
OUT_DIR = os.path.join(HISTORY_DIR, "out")
CACHE_DIR = os.path.join(HISTORY_DIR, "cache")

PRICE_COLS = ["stock_id", "date", "open", "high", "low", "close", "volume", "turnover",
              "change", "market", "product_type", "name", "source"]
CHIP_COLS = ["stock_id", "date", "foreign_buy", "investment_trust_buy", "dealer_buy",
             "margin_balance", "short_balance", "source"]
FOUR_DIGIT = re.compile(r"^\d{4}$")


def log(msg):
    print(msg, flush=True)


# ── connections (read-only) ──────────────────────────────────────────────────
def connect_readonly():
    """psycopg2 connection in a READ ONLY session. Never prints the URL."""
    import psycopg2
    if BACKEND_DIR not in sys.path:
        sys.path.insert(0, BACKEND_DIR)
    from export_static_json import load_env_file
    load_env_file()
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL not set (backend/.env or env var)")
    conn = psycopg2.connect(url)
    conn.set_session(readonly=True, autocommit=False)
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SET statement_timeout = '15min'")
    return conn


def _query_df(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def is_etf(code):
    return str(code).startswith("00")


def wanted_code(code, extra):
    return bool(FOUR_DIGIT.match(code)) or code in extra


# ── raw loaders ──────────────────────────────────────────────────────────────
def _read_parquet_kind(kind, start_date, cols):
    import pyarrow.parquet as pq
    start_year = int(start_date[:4])
    frames = []
    for p in sorted(glob.glob(os.path.join(HISTORY_DIR, f"{kind}_*.parquet"))):
        m = re.search(r"_(\d{4})\.parquet$", p)
        if not m or int(m.group(1)) < start_year:
            continue
        t = pq.read_table(p)
        use = [c for c in cols if c in t.column_names]
        frames.append(t.select(use).to_pandas())
    if not frames:
        raise SystemExit(f"no {kind}_*.parquet under {HISTORY_DIR} (use --source db or run history_store.py pull)")
    df = pd.concat(frames, ignore_index=True)
    return df[df["date"] >= start_date].reindex(columns=cols)


def load_prices(source, start_date, extra_codes=()):
    extra = set(extra_codes)
    if source == "parquet":
        df = _read_parquet_kind("prices", start_date, PRICE_COLS)
    elif source == "db":
        conn = connect_readonly()
        try:
            df = _query_df(conn, f"SELECT {','.join(PRICE_COLS)} FROM stock_daily "
                                 "WHERE date >= %s AND (stock_id ~ '^[0-9]{4}$' OR stock_id = ANY(%s))",
                           (start_date, list(extra)))
        finally:
            conn.rollback()
            conn.close()
    else:
        raise ValueError(source)
    df["stock_id"] = df["stock_id"].astype(str)
    df = df[df["stock_id"].map(lambda c: wanted_code(c, extra))].copy()
    for c in ["open", "high", "low", "close", "volume", "turnover", "change"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    df["source"] = df["source"].fillna("").astype(str)
    return df


def load_chips(source, start_date, codes):
    codes = list(codes)
    if source == "parquet":
        try:
            df = _read_parquet_kind("chips", start_date, CHIP_COLS)
        except SystemExit:
            return pd.DataFrame(columns=CHIP_COLS)
    else:
        conn = connect_readonly()
        try:
            df = _query_df(conn, f"SELECT {','.join(CHIP_COLS)} FROM chip_daily "
                                 "WHERE date >= %s AND stock_id = ANY(%s)", (start_date, codes))
        finally:
            conn.rollback()
            conn.close()
    df["stock_id"] = df["stock_id"].astype(str)
    df = df[df["stock_id"].isin(set(codes))].copy()
    for c in ["foreign_buy", "investment_trust_buy", "dealer_buy", "margin_balance", "short_balance"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    return df.drop_duplicates(["stock_id", "date"], keep="last")


def load_holdings():
    """Codes with net shares > 0 in Supabase `transactions`. Read-only.
    side enum (CHECK constraint): buy | sell | cash_dividend | stock_dividend.
    Whitelist: buy, stock_dividend → +shares; sell → -shares; anything else (cash_dividend) ignored."""
    conn = connect_readonly()
    try:
        df = _query_df(conn, """
            SELECT stock_id,
                   SUM(CASE WHEN side IN ('buy', 'stock_dividend') THEN shares
                            WHEN side = 'sell' THEN -shares ELSE 0 END) AS net
            FROM transactions GROUP BY stock_id""")
    finally:
        conn.rollback()
        conn.close()
    df["net"] = pd.to_numeric(df["net"], errors="coerce")
    return sorted(df.loc[df["net"] > 0, "stock_id"].astype(str))


def load_industry():
    """product_universe.industry (TWSE numeric industry code; TPEx rows mostly '股票'). Optional."""
    try:
        conn = connect_readonly()
    except Exception as e:  # pragma: no cover - network
        log(f"[data] product_universe unavailable: {type(e).__name__}")
        return {}
    try:
        df = _query_df(conn, "SELECT code, industry, market FROM product_universe")
    finally:
        conn.rollback()
        conn.close()
    return {r.code: (r.industry or "") for r in df.itertuples()}


# ── cleaning ─────────────────────────────────────────────────────────────────
def _manifest_nontrading():
    p = os.path.join(HISTORY_DIR, "manifest.json")
    try:
        with open(p, encoding="utf-8") as f:
            return set(json.load(f).get("nontrading_dates") or [])
    except Exception:
        return set()


BULK_SOURCES = {"TWSE STOCK_DAY_ALL", "TPEx dailyCloseQuotes"}


def drop_copies(df):
    """Drop rows that are verbatim copies (same stock, OHLC and volume>0) of a row on another date.
    Keeps the per-date official source first, then bulk snapshots, then yfinance; ties → earliest date.
    Bulk snapshots (STOCK_DAY_ALL) fetched on weekends/holidays are re-labelled copies of other days."""
    df = df.copy()
    df["yf"] = df["source"].str.startswith("yfinance")
    df["bulk"] = df["source"].isin(BULK_SOURCES)
    key = ["stock_id", "open", "high", "low", "close", "volume"]
    s = df[df["volume"] > 0].sort_values(["bulk", "yf", "date"])
    dup = s.index[s.duplicated(key, keep="first")]
    dropped = df.loc[dup].groupby("stock_id").size()
    return df.drop(index=dup), dropped


def trading_calendar(df, min_share=0.3):
    """Sorted trading dates: weekdays, not in manifest nontrading_dates, and with at least
    min_share x (90th-percentile rows per date) (after drop_copies). df must already be passed through drop_copies."""
    cnt = df.groupby("date").size()
    wd = pd.to_datetime(cnt.index, format="%Y%m%d").weekday
    bad = _manifest_nontrading()
    ok = (wd < 5) & (cnt.to_numpy() >= min_share * cnt.quantile(0.9)) & ~cnt.index.isin(list(bad))
    return sorted(cnt.index[ok])


def clean_prices(df, max_gap=20):
    """Return (clean long df, per-stock quality DataFrame, calendar). See module docstring."""
    n_in = df.groupby("stock_id").size()
    df, copies = drop_copies(df)
    calendar = trading_calendar(df)
    cal = set(calendar)
    noncal = df[~df["date"].isin(cal)].groupby("stock_id").size()
    df = df[df["date"].isin(cal) & (df["close"] > 0)]
    # keep only each stock's last contiguous segment (no hole > max_gap business days) — sparse old rows
    # (single probe days years ago) would otherwise create fake multi-year "daily" returns
    df = df.sort_values(["stock_id", "date"])
    dt = pd.to_datetime(df["date"], format="%Y%m%d").to_numpy().astype("datetime64[D]")
    prev = pd.Series(dt).groupby(df["stock_id"].to_numpy()).shift(1).to_numpy().astype("datetime64[D]")
    ok = ~np.isnat(prev)
    bd = np.zeros(len(df), dtype=np.int64)
    bd[ok] = np.busday_count(prev[ok], dt[ok])
    hole = pd.Series(bd > max_gap, index=df.index)
    seg = hole.astype(int).groupby(df["stock_id"]).cumsum()
    last_seg = seg.groupby(df["stock_id"]).transform("max")
    old = df[seg < last_seg].groupby("stock_id").size()
    df = df[seg == last_seg]
    # official beats yfinance on the same date
    df = df.sort_values(["stock_id", "date", "yf", "bulk"]).drop_duplicates(["stock_id", "date"], keep="first")
    df = df.reset_index(drop=True)
    for c in ["open", "high", "low"]:
        df[c] = df[c].where(df[c] > 0, df["close"])
    df["yf_rescaled"] = False
    df["yf_unanchored"] = False
    df["yf_anchor"] = ""
    suspicious = {}
    yf_ids = df.loc[df["yf"], "stock_id"].unique()
    pieces = [df[~df["stock_id"].isin(set(yf_ids))]]
    for sid, g in df[df["stock_id"].isin(set(yf_ids))].groupby("stock_id", sort=False):
        g = g.reset_index(drop=True)
        yf = g["yf"].to_numpy()
        block = (pd.Series(yf) != pd.Series(yf).shift()).cumsum()
        sus = 0
        for _, idx in pd.Series(np.arange(len(g)))[yf].groupby(block[yf]):
            last = idx.iloc[-1]
            nxt = last + 1
            if nxt < len(g) and not yf[nxt] and pd.notna(g.at[nxt, "change"]) and g.at[last, "close"] > 0:
                scale = (g.at[nxt, "close"] - g.at[nxt, "change"]) / g.at[last, "close"]
                if np.isfinite(scale) and scale > 0:
                    sus += int(abs(scale - 1) > 0.3)
                    for c in ["open", "high", "low", "close"]:
                        g.loc[idx, c] = g.loc[idx, c] * scale
                    g.loc[idx, "yf_rescaled"] = True
                    g.loc[idx, "yf_anchor"] = g.at[nxt, "date"]
                    continue
            g.loc[idx, "yf_unanchored"] = True
        suspicious[sid] = sus
        pieces.append(g)
    clean = pd.concat(pieces, ignore_index=True).sort_values(["stock_id", "date"]).reset_index(drop=True)
    grp = clean.groupby("stock_id")
    q = pd.DataFrame({
        "rows_in": n_in, "rows": grp.size(), "first_date": grp["date"].min(), "last_date": grp["date"].max(),
        "rows_dropped_copies": copies, "rows_dropped_noncalendar": noncal, "rows_dropped_old_segment": old,
        "yf_only_rows": grp["yf"].sum(), "yf_unanchored_rows": grp["yf_unanchored"].sum(),
        "yf_suspicious_blocks": pd.Series(suspicious, dtype="float64"),
    })
    q = q[q["rows"].notna()].fillna(0)
    for c in q.columns:
        if c not in ("first_date", "last_date"):
            q[c] = q[c].astype(int)
    return clean, q, calendar


def select_universe(clean, size, holdings, lookback=60):
    """Top `size` 4-digit non-ETF stocks by average turnover over the last `lookback` trading dates,
    plus all holdings present in the data. Returns (codes, rank dict)."""
    dates = sorted(clean["date"].unique())[-lookback:]
    rec = clean[clean["date"].isin(set(dates))].copy()
    tv = rec["turnover"].where(rec["turnover"] > 0, rec["close"] * rec["volume"])
    rec["tv"] = tv
    avg = rec.groupby("stock_id")["tv"].sum() / len(dates)  # missing days count as 0 turnover
    avg = avg[[bool(FOUR_DIGIT.match(c)) and not is_etf(c) for c in avg.index]]
    top = list(avg.sort_values(ascending=False).index[:size])
    have = set(clean["stock_id"].unique())
    extra = [h for h in holdings if h in have and h not in top]
    return top + extra, {c: i + 1 for i, c in enumerate(top)}


def pit_membership(clean, cal, size, lookback=60, every=21):
    """Point-in-time universe. Every `every` trading days (rebalance at calendar position r) take the top
    `size` 4-digit non-ETF stocks by average turnover over the `lookback` sessions BEFORE r (positions
    r-lookback..r-1, so only data known at the close of r-1). Membership holds until the next rebalance.
    Missing days count as zero turnover (a stock not yet listed cannot rank).
    Returns (long DataFrame[date, stock_id] of in-universe days, list of (rebalance date, members),
             current members ranked with data up to the last date)."""
    rec = clean[clean["stock_id"].map(lambda c: bool(FOUR_DIGIT.match(c)) and not is_etf(c))]
    tv = rec["turnover"].where(rec["turnover"] > 0, rec["close"] * rec["volume"])
    piv = (rec.assign(tv=tv).pivot_table(index="date", columns="stock_id", values="tv", aggfunc="last")
           .reindex(cal).fillna(0.0))
    roll = piv.rolling(lookback, min_periods=lookback).mean()
    rows, schedule = [], []
    for r in range(lookback, len(cal), every):
        members = list(roll.iloc[r - 1].sort_values(ascending=False).index[:size])
        schedule.append((cal[r], members))
        block = cal[r:r + every]
        rows.append(pd.DataFrame({"date": np.repeat(block, len(members)),
                                  "stock_id": np.tile(members, len(block))}))
    inu = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["date", "stock_id"])
    current = list(roll.iloc[-1].sort_values(ascending=False).index[:size]) if len(roll) else []
    return inu, schedule, current


def stock_meta(clean):
    last = clean.sort_values("date").groupby("stock_id").tail(1).set_index("stock_id")
    return {sid: {"name": r["name"] if isinstance(r["name"], str) else "",
                  "market": r["market"] if isinstance(r["market"], str) else "",
                  "etf": is_etf(sid)} for sid, r in last.iterrows()}
