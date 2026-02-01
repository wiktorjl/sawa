-- ============================================
-- MARKET DATA TABLES
-- ============================================

-- Daily stock prices
CREATE TABLE stock_prices (
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    date DATE NOT NULL,
    open NUMERIC(12, 4),
    high NUMERIC(12, 4),
    low NUMERIC(12, 4),
    close NUMERIC(12, 4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);

-- Financial ratios (time-series)
CREATE TABLE financial_ratios (
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    date DATE NOT NULL,
    average_volume BIGINT,
    cash NUMERIC(10, 4),
    current NUMERIC(10, 4),
    debt_to_equity NUMERIC(10, 4),
    dividend_yield NUMERIC(10, 6),
    earnings_per_share NUMERIC(12, 4),
    enterprise_value NUMERIC(20, 2),
    ev_to_ebitda NUMERIC(10, 4),
    ev_to_sales NUMERIC(10, 4),
    free_cash_flow NUMERIC(20, 2),
    market_cap NUMERIC(20, 2),
    price NUMERIC(12, 4),
    price_to_book NUMERIC(10, 4),
    price_to_cash_flow NUMERIC(10, 4),
    price_to_earnings NUMERIC(10, 4),
    price_to_free_cash_flow NUMERIC(10, 4),
    price_to_sales NUMERIC(10, 4),
    quick NUMERIC(10, 4),
    return_on_assets NUMERIC(10, 6),
    return_on_equity NUMERIC(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);
