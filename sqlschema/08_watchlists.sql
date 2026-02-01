-- Watchlist tables for TUI application
-- Stores user-defined watchlists and their symbols

-- Watchlists table
CREATE TABLE IF NOT EXISTS watchlists (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Watchlist symbols junction table
CREATE TABLE IF NOT EXISTS watchlist_symbols (
    watchlist_id INTEGER NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sort_order INTEGER DEFAULT 0,
    PRIMARY KEY (watchlist_id, ticker)
);

-- User settings table
CREATE TABLE IF NOT EXISTS user_settings (
    key VARCHAR(50) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_watchlist_symbols_ticker ON watchlist_symbols(ticker);
CREATE INDEX IF NOT EXISTS idx_watchlists_default ON watchlists(is_default) WHERE is_default = TRUE;

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_watchlist_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS watchlists_updated_at ON watchlists;
CREATE TRIGGER watchlists_updated_at
    BEFORE UPDATE ON watchlists
    FOR EACH ROW
    EXECUTE FUNCTION update_watchlist_timestamp();

DROP TRIGGER IF EXISTS user_settings_updated_at ON user_settings;
CREATE TRIGGER user_settings_updated_at
    BEFORE UPDATE ON user_settings
    FOR EACH ROW
    EXECUTE FUNCTION update_watchlist_timestamp();

-- Insert default watchlist if not exists
INSERT INTO watchlists (name, is_default)
VALUES ('Default', TRUE)
ON CONFLICT (name) DO NOTHING;

-- Insert default symbols (only if they exist in companies table)
INSERT INTO watchlist_symbols (watchlist_id, ticker, sort_order)
SELECT w.id, c.ticker, t.sort_order
FROM watchlists w
CROSS JOIN (
    VALUES ('AAPL', 1), ('GOOGL', 2), ('AMZN', 3)
) AS t(ticker, sort_order)
JOIN companies c ON c.ticker = t.ticker
WHERE w.name = 'Default'
  AND w.is_default = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM watchlist_symbols ws WHERE ws.watchlist_id = w.id
  )
ON CONFLICT (watchlist_id, ticker) DO NOTHING;

-- Insert default settings
INSERT INTO user_settings (key, value) VALUES
    ('chart_period_days', '60'),
    ('auto_refresh', 'false'),
    ('refresh_interval_seconds', '60'),
    ('number_format', 'compact'),
    ('fundamentals_timeframe', 'quarterly'),
    ('table_rows', '25')
ON CONFLICT (key) DO NOTHING;
