-- ============================================
-- COMPLETE DATABASE SETUP
-- ============================================
-- Run this file to create the entire schema
-- Or run individual files in order:
--   01_companies.sql
--   02_market_data.sql
--   03_fundamentals.sql
--   04_economy.sql
--   05_indexes.sql
--   06_views.sql
--   07_procedures.sql
-- ============================================

-- Note: Ensure you're connected to the correct database before running
-- Example: \c stock_data

-- Verify all tables were created
SELECT 
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

-- Expected output should show:
--   companies
--   stock_prices
--   financial_ratios
--   balance_sheets
--   cash_flows
--   income_statements
--   treasury_yields
--   inflation
--   inflation_expectations
--   labor_market
