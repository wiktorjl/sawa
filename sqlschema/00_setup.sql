-- ============================================
-- DATABASE SCHEMA DOCUMENTATION
-- ============================================
-- This file documents the complete schema structure.
-- To create the database, run files in numeric order (01-22).
--
-- AUTOMATED SETUP:
--   python -m sawa.database.schema --database-url postgresql://...
--
-- MANUAL SETUP:
--   Run files 01-22 in order using psql:
--   for f in sqlschema/*.sql; do psql $DATABASE_URL -f $f; done
--
-- FILE ORDER:
--   Core:     01-07 (companies, prices, fundamentals, economy, indexes, views, procedures)
--   Extended: 08-15 (sic/gics, news, technical indicators, indices, 52wk extremes)
--   Migrate:  16-21 (cleanup, extensions, corporate actions, intraday)
--   Advanced: 22 (views with dependencies on later tables)
-- ============================================

-- Note: Ensure you're connected to the correct database before running
-- Example: psql $DATABASE_URL

-- Verify all tables and views were created
SELECT
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

-- Expected tables (22 total):
--   balance_sheets
--   cash_flows
--   companies
--   dividends
--   earnings
--   financial_ratios
--   income_statements
--   index_constituents
--   indices
--   inflation
--   inflation_expectations
--   labor_market
--   news_article_tickers
--   news_articles
--   news_sentiment
--   sic_gics_mapping
--   stock_prices
--   stock_prices_intraday
--   stock_splits
--   technical_indicator_metadata
--   technical_indicators
--   treasury_yields
--
-- Expected views (7 total):
--   mv_52week_extremes (materialized)
--   stock_prices_live
--   v_company_summary
--   v_company_with_indices
--   v_economy_dashboard
--   v_latest_fundamentals
--   v_sector_summary
