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
    vix_high NUMERIC(8, 4),
    vix_low NUMERIC(8, 4),
    vix3m NUMERIC(8, 4),
    hy_spread NUMERIC(8, 4),
    put_call_ratio NUMERIC(8, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_market_internals_date
    ON market_internals (date DESC);
