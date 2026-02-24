-- ============================================
-- MARKET INDICES
-- ============================================
-- Tracks market index definitions and their constituents

-- Market indices table
CREATE TABLE IF NOT EXISTS indices (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) UNIQUE NOT NULL,       -- 'sp500', 'nasdaq5000'
    name VARCHAR(100) NOT NULL,             -- 'S&P 500', 'NASDAQ-100'
    description TEXT,
    source_url VARCHAR(255),                -- Data source URL (e.g., Wikipedia)
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index constituents (many-to-many with companies)
CREATE TABLE IF NOT EXISTS index_constituents (
    index_id INTEGER NOT NULL REFERENCES indices(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (index_id, ticker)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_index_constituents_ticker ON index_constituents(ticker);
CREATE INDEX IF NOT EXISTS idx_index_constituents_index_id ON index_constituents(index_id);

-- Seed initial indices
INSERT INTO indices (code, name, description, source_url) VALUES
('sp500', 'S&P 500', 'Standard & Poor''s 500 Index - 500 large-cap US stocks', 
 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'),
('nasdaq5000', 'NASDAQ-5000', 'NASDAQ listed stocks',
 NULL)
ON CONFLICT (code) DO NOTHING;
