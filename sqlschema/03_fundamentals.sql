-- ============================================
-- FUNDAMENTALS TABLES
-- ============================================

-- Balance sheets
CREATE TABLE balance_sheets (
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    period_end DATE NOT NULL,
    filing_date DATE,
    fiscal_quarter INTEGER CHECK (fiscal_quarter BETWEEN 1 AND 4),
    fiscal_year INTEGER,
    timeframe VARCHAR(10) CHECK (timeframe IN ('quarterly', 'annual')),
    
    -- Assets
    cash_and_equivalents NUMERIC(20, 2),
    short_term_investments NUMERIC(20, 2),
    receivables NUMERIC(20, 2),
    inventories NUMERIC(20, 2),
    other_current_assets NUMERIC(20, 2),
    total_current_assets NUMERIC(20, 2),
    property_plant_equipment_net NUMERIC(20, 2),
    goodwill NUMERIC(20, 2),
    intangible_assets_net NUMERIC(20, 2),
    other_assets NUMERIC(20, 2),
    total_assets NUMERIC(20, 2),
    
    -- Liabilities
    accounts_payable NUMERIC(20, 2),
    accrued_and_other_current_liabilities NUMERIC(20, 2),
    debt_current NUMERIC(20, 2),
    deferred_revenue_current NUMERIC(20, 2),
    total_current_liabilities NUMERIC(20, 2),
    long_term_debt_and_capital_lease_obligations NUMERIC(20, 2),
    other_noncurrent_liabilities NUMERIC(20, 2),
    total_liabilities NUMERIC(20, 2),
    
    -- Equity
    common_stock NUMERIC(20, 2),
    additional_paid_in_capital NUMERIC(20, 2),
    retained_earnings_deficit NUMERIC(20, 2),
    accumulated_other_comprehensive_income NUMERIC(20, 2),
    treasury_stock NUMERIC(20, 2),
    other_equity NUMERIC(20, 2),
    noncontrolling_interest NUMERIC(20, 2),
    total_equity NUMERIC(20, 2),
    total_equity_attributable_to_parent NUMERIC(20, 2),
    total_liabilities_and_equity NUMERIC(20, 2),
    
    commitments_and_contingencies TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period_end, timeframe)
);

-- Cash flow statements
CREATE TABLE cash_flows (
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    period_end DATE NOT NULL,
    filing_date DATE,
    fiscal_quarter INTEGER CHECK (fiscal_quarter BETWEEN 1 AND 4),
    fiscal_year INTEGER,
    timeframe VARCHAR(30) CHECK (timeframe IN ('quarterly', 'annual', 'trailing_twelve_months')),
    
    -- Operating activities
    net_income NUMERIC(20, 2),
    depreciation_depletion_and_amortization NUMERIC(20, 2),
    change_in_other_operating_assets_and_liabilities_net NUMERIC(20, 2),
    other_operating_activities NUMERIC(20, 2),
    other_cash_adjustments NUMERIC(20, 2),
    net_cash_from_operating_activities NUMERIC(20, 2),
    cash_from_operating_activities_continuing_operations NUMERIC(20, 2),
    
    -- Investing activities
    purchase_of_property_plant_and_equipment NUMERIC(20, 2),
    sale_of_property_plant_and_equipment NUMERIC(20, 2),
    other_investing_activities NUMERIC(20, 2),
    net_cash_from_investing_activities NUMERIC(20, 2),
    net_cash_from_investing_activities_continuing_operations NUMERIC(20, 2),
    
    -- Financing activities
    long_term_debt_issuances_repayments NUMERIC(20, 2),
    short_term_debt_issuances_repayments NUMERIC(20, 2),
    dividends NUMERIC(20, 2),
    other_financing_activities NUMERIC(20, 2),
    net_cash_from_financing_activities NUMERIC(20, 2),
    net_cash_from_financing_activities_continuing_operations NUMERIC(20, 2),
    
    -- Summary
    effect_of_currency_exchange_rate NUMERIC(20, 2),
    change_in_cash_and_equivalents NUMERIC(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period_end, timeframe)
);

-- Income statements
CREATE TABLE income_statements (
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    period_end DATE NOT NULL,
    filing_date DATE,
    fiscal_quarter INTEGER CHECK (fiscal_quarter BETWEEN 1 AND 4),
    fiscal_year INTEGER,
    timeframe VARCHAR(30) CHECK (timeframe IN ('quarterly', 'annual', 'trailing_twelve_months')),
    
    -- Revenue
    revenue NUMERIC(20, 2),
    cost_of_revenue NUMERIC(20, 2),
    gross_profit NUMERIC(20, 2),
    
    -- Operating expenses
    research_development NUMERIC(20, 2),
    selling_general_administrative NUMERIC(20, 2),
    total_operating_expenses NUMERIC(20, 2),
    operating_income NUMERIC(20, 2),
    
    -- Non-operating
    interest_income NUMERIC(20, 2),
    interest_expense NUMERIC(20, 2),
    other_income_expense NUMERIC(20, 2),
    income_before_income_taxes NUMERIC(20, 2),
    
    -- Tax and net income
    income_taxes NUMERIC(20, 2),
    consolidated_net_income_loss NUMERIC(20, 2),
    net_income_loss_attributable_common_shareholders NUMERIC(20, 2),
    
    -- Per share
    basic_earnings_per_share NUMERIC(12, 4),
    diluted_earnings_per_share NUMERIC(12, 4),
    basic_shares_outstanding NUMERIC(20, 2),
    diluted_shares_outstanding NUMERIC(20, 2),
    
    -- Other
    ebitda NUMERIC(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period_end, timeframe)
);
