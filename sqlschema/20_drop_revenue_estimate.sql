-- ============================================
-- DROP REVENUE_ESTIMATE COLUMN
-- ============================================
-- Remove revenue_estimate column as yfinance only provides estimates
-- for the upcoming quarter, making historical data incomplete.

ALTER TABLE earnings DROP COLUMN IF EXISTS revenue_estimate;

COMMENT ON TABLE earnings IS 'Earnings calendar and actuals - EPS and revenue data';
