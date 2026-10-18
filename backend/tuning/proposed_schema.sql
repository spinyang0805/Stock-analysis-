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
-- Every short-term BUY pick becomes a tracked position, re-evaluated EVERY trading day until it closes.
-- Closes only when: (a) the stock's exit rule / stop / take-profit / max hold fires (system exit),
-- (b) the user's transactions show the position fully sold, or (c) the user bought it and still holds →
-- stays open even past the system exit until sold (then status 'exit_signalled' keeps alerting daily).
-- Never deleted by retention while open; closed rows double as the live track record of the recommender.
CREATE TABLE IF NOT EXISTS tracked_picks (
    id            BIGSERIAL PRIMARY KEY,
    stock_id      TEXT NOT NULL,
    pick_date     TEXT NOT NULL,              -- YYYYMMDD recommendation date
    entry_date    TEXT,                       -- next session (virtual entry at open)
    entry_price   REAL,
    signals       TEXT[],
    plan_hold     SMALLINT,                   -- tuned horizon h
    exit_rules    TEXT[],                     -- the stock's tuned sell signals, e.g. {s_atr_25,s_tl_up_break}
    stop_loss     REAL,                       -- updated daily (trailing)
    take_profit   REAL,
    user_holds    BOOLEAN DEFAULT FALSE,      -- from transactions (net shares > 0)
    status        TEXT NOT NULL DEFAULT 'open',  -- open | exit_signalled | closed
    last_eval     TEXT,                       -- YYYYMMDD of latest daily evaluation
    last_action   TEXT,                       -- 續抱 | 減碼 | 出場 + reason
    exit_date     TEXT,
    exit_price    REAL,
    exit_reason   TEXT,                       -- stop | take_profit | signal:<id> | max_hold | user_sold
    ret_net       REAL,                       -- after costs (virtual, or user's actual if sold)
    UNIQUE (stock_id, pick_date)
);

-- retention (run from the daily job once applied):
-- DELETE FROM daily_recommendations WHERE date < to_char(now() - interval '90 days', 'YYYYMMDD');
