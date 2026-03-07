-- ============================================
-- MARKET INTERNALS TABLE
-- ============================================
-- Daily market sentiment/volatility indicators
-- sourced from FRED (VIX, HY spread) and CBOE.
-- Separate from economy tables (macro fundamentals)
-- because these are daily market-derived indicators.

CREATE TABLE IF NOT EXISTS market_internals (
    date DATE PRIMARY KEY,
    vix_close NUMERIC(8, 4),
    vix3m NUMERIC(8, 4),
    hy_spread NUMERIC(8, 4),
    put_call_ratio NUMERIC(8, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_market_internals_date
    ON market_internals (date DESC);

-- VIX intraday bars from Polygon indices API (I:VIX)
-- Separate from stock_prices_intraday because VIX is not a stock
-- and has no FK to companies table.
CREATE TABLE IF NOT EXISTS vix_intraday (
    timestamp TIMESTAMP NOT NULL PRIMARY KEY,
    open NUMERIC(8, 4),
    high NUMERIC(8, 4),
    low NUMERIC(8, 4),
    close NUMERIC(8, 4),
    bar_size_minutes INTEGER DEFAULT 5,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_vix_intraday_timestamp
    ON vix_intraday (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_vix_intraday_date
    ON vix_intraday ((timestamp::date));
