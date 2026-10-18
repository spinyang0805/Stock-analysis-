-- PROPOSED ONLY — NOT APPLIED. Compact tables for the tuning engine (DB is ~472/500 MB → keep tiny).
-- Size estimate: stock_params ≈ 550 rows × ~400 B ≈ 0.2 MB (overwritten weekly/monthly, no history);
-- daily_recommendations ≈ 10–30 rows/day × ~300 B, keep 90 days ≈ 1 MB.
-- Full detail (OOS trades, coverage, reports) stays in Parquet/JSON under history/out (GitHub Release).

CREATE TABLE IF NOT EXISTS stock_params (
    stock_id      TEXT PRIMARY KEY,
    as_of         TEXT NOT NULL,              -- YYYYMMDD of the data used
    level         TEXT NOT NULL,              -- stock | group | uniform | none
    signals       TEXT[],                     -- up to 3 signal ids, best first (e.g. {brk_55,chip_p90})
    best_h        SMALLINT,                   -- suggested holding days
    sell_rule     TEXT,                       -- e.g. s_atr_25
    stop_atr_mult REAL,
    tp_pct        REAL,                       -- take-profit (historical MFE p60)
    oos_n         INTEGER,
    oos_mean      REAL,                       -- mean net return per trade (after costs)
    oos_xnet      REAL,                       -- mean net excess vs equal-weight universe
    oos_win       REAL,
    confidence    REAL,
    flags         TEXT,                       -- short_history / yf / adj gaps (comma list)
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_recommendations (
    date          TEXT NOT NULL,              -- YYYYMMDD signal date (entry = next session open)
    stock_id      TEXT NOT NULL,
    kind          TEXT NOT NULL,              -- buy | sell_alert
    rank          SMALLINT,
    signals       TEXT[],
    trigger_price REAL,                       -- raw price
    stop_loss     REAL,
    take_profit   REAL,
    hold_days     SMALLINT,
    score         REAL,
    confidence    REAL,
    levels        JSONB,                      -- next-day intraday trigger levels (small object)
    flags         TEXT,
    PRIMARY KEY (date, stock_id, kind)
);
-- retention (run from the daily job once applied):
-- DELETE FROM daily_recommendations WHERE date < to_char(now() - interval '90 days', 'YYYYMMDD');
