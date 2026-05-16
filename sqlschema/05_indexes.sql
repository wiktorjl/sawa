-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

-- Stock prices indexes
CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON stock_prices(date);
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date ON stock_prices(ticker, date DESC);

-- Financial ratios indexes
CREATE INDEX IF NOT EXISTS idx_financial_ratios_date ON financial_ratios(date);
CREATE INDEX IF NOT EXISTS idx_financial_ratios_ticker_date ON financial_ratios(ticker, date DESC);

-- Fundamentals indexes
CREATE INDEX IF NOT EXISTS idx_balance_sheets_period ON balance_sheets(period_end);
CREATE INDEX IF NOT EXISTS idx_balance_sheets_ticker_period ON balance_sheets(ticker, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_cash_flows_period ON cash_flows(period_end);
CREATE INDEX IF NOT EXISTS idx_cash_flows_ticker_period ON cash_flows(ticker, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_income_statements_period ON income_statements(period_end);
CREATE INDEX IF NOT EXISTS idx_income_statements_ticker_period ON income_statements(ticker, period_end DESC);

-- Company lookup indexes
CREATE INDEX IF NOT EXISTS idx_companies_cik ON companies(cik);
CREATE INDEX IF NOT EXISTS idx_companies_sic ON companies(sic_code);
CREATE INDEX IF NOT EXISTS idx_companies_active ON companies(active) WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_companies_exchange ON companies(primary_exchange);

-- Economy data indexes (for time-series queries)
CREATE INDEX IF NOT EXISTS idx_treasury_yields_date ON treasury_yields(date DESC);
CREATE INDEX IF NOT EXISTS idx_inflation_date ON inflation(date DESC);
CREATE INDEX IF NOT EXISTS idx_inflation_expectations_date ON inflation_expectations(date DESC);
CREATE INDEX IF NOT EXISTS idx_labor_market_date ON labor_market(date DESC);
