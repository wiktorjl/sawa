-- ============================================
-- INTRADAY STOCK PRICES
-- ============================================

-- 5-minute intraday bars from WebSocket stream
CREATE TABLE IF NOT EXISTS stock_prices_intraday (
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(20, 8) NOT NULL,
    high NUMERIC(20, 8) NOT NULL,
    low NUMERIC(20, 8) NOT NULL,
    close NUMERIC(20, 8) NOT NULL,
    volume BIGINT NOT NULL,
    bar_size_minutes INTEGER NOT NULL DEFAULT 5
        CHECK (bar_size_minutes IN (1, 5, 15, 30, 60)),
    source_minute_count INTEGER NOT NULL DEFAULT 1
        CHECK (source_minute_count BETWEEN 1 AND bar_size_minutes),
    source_minute_mask BIGINT NOT NULL DEFAULT 1
        CHECK (
            source_minute_mask > 0
            AND source_minute_mask < (1::BIGINT << bar_size_minutes)
            AND pg_catalog.length(
                pg_catalog.replace(
                    (source_minute_mask::BIT(64))::TEXT,
                    '0',
                    ''
                )
            ) = source_minute_count
        ),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, timestamp, bar_size_minutes)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_intraday_timestamp ON stock_prices_intraday(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_intraday_ticker_timestamp ON stock_prices_intraday(ticker, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_intraday_date
    ON stock_prices_intraday(((timestamp AT TIME ZONE 'America/New_York')::date));

COMMENT ON TABLE stock_prices_intraday IS 'Real-time configurable-size bars from WebSocket (15-min delayed)';
COMMENT ON COLUMN stock_prices_intraday.timestamp IS 'Bar timestamp in UTC';
COMMENT ON COLUMN stock_prices_intraday.bar_size_minutes IS 'Bar interval (5, 15, etc.)';
