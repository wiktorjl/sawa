-- ============================================
-- VIEWS FOR COMMON QUERIES
-- ============================================

-- Latest company overview with current metrics
CREATE OR REPLACE VIEW v_company_summary AS
SELECT 
    c.*,
    sp.close as latest_price,
    sp.date as price_date,
    fr.price_to_earnings as latest_pe,
    fr.debt_to_equity as latest_debt_equity,
    fr.return_on_equity as latest_roe,
    fr.dividend_yield as latest_dividend_yield
FROM companies c
LEFT JOIN LATERAL (
    SELECT close, date 
    FROM stock_prices 
    WHERE ticker = c.ticker 
    ORDER BY date DESC 
    LIMIT 1
) sp ON true
LEFT JOIN LATERAL (
    SELECT price_to_earnings, debt_to_equity, return_on_equity, dividend_yield
    FROM financial_ratios
    WHERE ticker = c.ticker
    ORDER BY date DESC
    LIMIT 1
) fr ON true
WHERE c.active = TRUE;

-- Economy dashboard view (most recent data with all indicators)
CREATE OR REPLACE VIEW v_economy_dashboard AS
SELECT 
    ty.date,
    ty.yield_1_month,
    ty.yield_3_month,
    ty.yield_10_year,
    ty.yield_30_year,
    i.cpi,
    i.cpi_year_over_year as inflation_yoy,
    ie.market_5_year as inflation_expectation_5y,
    ie.market_10_year as inflation_expectation_10y,
    lm.unemployment_rate,
    lm.job_openings
FROM treasury_yields ty
LEFT JOIN inflation i ON ty.date = i.date
LEFT JOIN inflation_expectations ie ON ty.date = ie.date
LEFT JOIN labor_market lm ON ty.date = lm.date
ORDER BY ty.date DESC;

-- Latest fundamentals summary for each company
CREATE OR REPLACE VIEW v_latest_fundamentals AS
SELECT 
    c.ticker,
    c.name,
    bs.period_end as balance_sheet_date,
    bs.total_assets,
    bs.total_liabilities,
    bs.total_equity,
    cf.period_end as cash_flow_date,
    cf.net_cash_from_operating_activities,
    cf.change_in_cash_and_equivalents,
    inc.period_end as income_statement_date,
    inc.revenue,
    inc.consolidated_net_income_loss as net_income,
    inc.basic_earnings_per_share as basic_eps
FROM companies c
LEFT JOIN LATERAL (
    SELECT period_end, total_assets, total_liabilities, total_equity
    FROM balance_sheets
    WHERE ticker = c.ticker AND timeframe = 'quarterly'
    ORDER BY period_end DESC
    LIMIT 1
) bs ON true
LEFT JOIN LATERAL (
    SELECT period_end, net_cash_from_operating_activities, change_in_cash_and_equivalents
    FROM cash_flows
    WHERE ticker = c.ticker AND timeframe = 'quarterly'
    ORDER BY period_end DESC
    LIMIT 1
) cf ON true
LEFT JOIN LATERAL (
    SELECT period_end, revenue, consolidated_net_income_loss, basic_earnings_per_share
    FROM income_statements
    WHERE ticker = c.ticker AND timeframe = 'quarterly'
    ORDER BY period_end DESC
    LIMIT 1
) inc ON true
WHERE c.active = TRUE;

-- Sector performance view (by SIC code)
CREATE OR REPLACE VIEW v_sector_summary AS
SELECT 
    c.sic_code,
    c.sic_description,
    COUNT(DISTINCT c.ticker) as company_count,
    AVG(c.market_cap) as avg_market_cap,
    SUM(c.market_cap) as total_market_cap,
    AVG(c.total_employees) as avg_employees
FROM companies c
WHERE c.active = TRUE
GROUP BY c.sic_code, c.sic_description
ORDER BY total_market_cap DESC;
