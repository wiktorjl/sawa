-- ============================================
-- DEPRECATE REVENUE_ESTIMATE IN PLACE
-- ============================================
-- Historical versions dropped this column. Because schema files are replayed
-- by the data-preserving no-drop upgrade path, retain any existing values and
-- simply stop reading/writing the deprecated field in application code.
DO $$
BEGIN
    RAISE NOTICE 'Preserving deprecated earnings.revenue_estimate values';
END $$;

COMMENT ON TABLE earnings IS 'Earnings calendar and actuals - EPS and revenue data';
