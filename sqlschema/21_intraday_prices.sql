-- ============================================
-- INTRADAY STOCK PRICES
-- ============================================

-- 5-minute intraday bars from WebSocket stream
CREATE TABLE stock_prices_intraday (
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL,
    open NUMERIC(12, 4),
    high NUMERIC(12, 4),
    low NUMERIC(12, 4),
    close NUMERIC(12, 4),
    volume BIGINT,
    bar_size_minutes INTEGER DEFAULT 5,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, timestamp)
);

-- Indexes for performance
CREATE INDEX idx_intraday_timestamp ON stock_prices_intraday(timestamp DESC);
CREATE INDEX idx_intraday_ticker_timestamp ON stock_prices_intraday(ticker, timestamp DESC);
CREATE INDEX idx_intraday_date ON stock_prices_intraday((timestamp::date));

COMMENT ON TABLE stock_prices_intraday IS 'Real-time 5-minute bars from WebSocket (15-min delayed)';
COMMENT ON COLUMN stock_prices_intraday.timestamp IS 'Bar timestamp in ET timezone';
COMMENT ON COLUMN stock_prices_intraday.bar_size_minutes IS 'Bar interval (5, 15, etc.)';
