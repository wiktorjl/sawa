-- ============================================
-- CORPORATE ACTIONS: SPLITS, DIVIDENDS, EARNINGS
-- ============================================
-- Tracks stock splits, dividend history, and earnings calendar

-- Stock splits
CREATE TABLE IF NOT EXISTS stock_splits (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    execution_date DATE NOT NULL,
    split_from INTEGER NOT NULL,  -- e.g., 1
    split_to INTEGER NOT NULL,    -- e.g., 4 (for 4:1 split)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, execution_date)
);

-- Dividend history
CREATE TABLE IF NOT EXISTS dividends (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    ex_dividend_date DATE NOT NULL,
    record_date DATE,
    pay_date DATE,
    cash_amount NUMERIC(10, 4),
    declaration_date DATE,
    dividend_type VARCHAR(20),  -- CD=cash, SC=special cash, LT=long-term cap gain, ST=short-term cap gain
    frequency INTEGER,          -- 0=one-time, 1=annual, 2=semi-annual, 4=quarterly, 12=monthly
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, ex_dividend_date, dividend_type)
);

-- Earnings calendar and history
CREATE TABLE IF NOT EXISTS earnings (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    report_date DATE NOT NULL,
    fiscal_quarter VARCHAR(10),       -- Q1, Q2, Q3, Q4
    fiscal_year INTEGER,
    timing VARCHAR(10),               -- BMO (before market open), AMC (after market close), DMH (during)
    eps_estimate NUMERIC(10, 4),
    eps_actual NUMERIC(10, 4),
    revenue_estimate BIGINT,
    revenue_actual BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, fiscal_year, fiscal_quarter)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_splits_ticker ON stock_splits(ticker);
CREATE INDEX IF NOT EXISTS idx_splits_date ON stock_splits(execution_date);
CREATE INDEX IF NOT EXISTS idx_dividends_ticker ON dividends(ticker);
CREATE INDEX IF NOT EXISTS idx_dividends_ex_date ON dividends(ex_dividend_date);
CREATE INDEX IF NOT EXISTS idx_dividends_pay_date ON dividends(pay_date);
CREATE INDEX IF NOT EXISTS idx_earnings_ticker ON earnings(ticker);
CREATE INDEX IF NOT EXISTS idx_earnings_report_date ON earnings(report_date);
CREATE INDEX IF NOT EXISTS idx_earnings_fiscal ON earnings(fiscal_year, fiscal_quarter);

-- Add table comments
COMMENT ON TABLE stock_splits IS 'Stock split history from Polygon';
COMMENT ON TABLE dividends IS 'Dividend declarations and payments from Polygon';
COMMENT ON TABLE earnings IS 'Earnings calendar and actuals from Polygon';

COMMENT ON COLUMN dividends.dividend_type IS 'CD=cash, SC=special cash, LT=long-term cap gain, ST=short-term cap gain';
COMMENT ON COLUMN dividends.frequency IS '0=one-time, 1=annual, 2=semi-annual, 4=quarterly, 12=monthly';
COMMENT ON COLUMN earnings.timing IS 'BMO=before market open, AMC=after market close, DMH=during market hours';
