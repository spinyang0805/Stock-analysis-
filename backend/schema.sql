-- Run once in Supabase SQL Editor to create all tables

CREATE TABLE IF NOT EXISTS stock_daily (
    stock_id     TEXT NOT NULL,
    date         TEXT NOT NULL,
    open         FLOAT,
    high         FLOAT,
    low          FLOAT,
    close        FLOAT,
    volume       FLOAT,
    turnover     FLOAT,
    change       FLOAT,
    trades       FLOAT,
    market       TEXT,
    product_type TEXT,
    name         TEXT,
    source       TEXT,
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, date)
);
CREATE INDEX IF NOT EXISTS idx_stock_daily_lookup ON stock_daily (stock_id, date DESC);

CREATE TABLE IF NOT EXISTS chip_daily (
    stock_id              TEXT NOT NULL,
    date                  TEXT NOT NULL,
    name                  TEXT,
    market                TEXT,
    foreign_buy           BIGINT,
    investment_trust_buy  BIGINT,
    dealer_buy            BIGINT,
    institution_total_buy BIGINT,
    margin_balance        BIGINT,
    short_balance         BIGINT,
    source                TEXT,
    chip_date             TEXT,
    updated_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_id, date)
);
CREATE INDEX IF NOT EXISTS idx_chip_daily_lookup ON chip_daily (stock_id, date DESC);

CREATE TABLE IF NOT EXISTS analysis_cache (
    stock_id          TEXT PRIMARY KEY,
    latest_date       TEXT,
    data_rows         INTEGER,
    perspective_cards JSONB,
    signals           JSONB,
    trade_plan        JSONB,
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_logs (
    job_id     TEXT PRIMARY KEY,
    payload    JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_queue (
    job_id     TEXT PRIMARY KEY,
    payload    JSONB,
    status     TEXT DEFAULT 'pending',
    control    TEXT DEFAULT 'run',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS product_universe (
    code       TEXT PRIMARY KEY,
    name       TEXT,
    market     TEXT,
    type       TEXT,
    industry   TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 基本面資料：每股一筆，儲存最新估值 + 最新月營收
CREATE TABLE IF NOT EXISTS fundamentals (
    stock_id       TEXT PRIMARY KEY,
    pe_ratio       FLOAT,
    dividend_yield FLOAT,
    pb_ratio       FLOAT,
    eps            FLOAT,
    revenue        BIGINT,
    revenue_mom    FLOAT,
    revenue_yoy    FLOAT,
    revenue_date   TEXT,
    valuation_date TEXT,
    source         TEXT,
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);
