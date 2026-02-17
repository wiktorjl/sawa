-- ============================================
-- Add missing cpi_year_over_year column to inflation table
-- ============================================

-- Check if column exists before adding it
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'inflation'
        AND column_name = 'cpi_year_over_year'
    ) THEN
        ALTER TABLE inflation ADD COLUMN cpi_year_over_year NUMERIC(8, 6);
        RAISE NOTICE 'Added cpi_year_over_year column to inflation table';
    ELSE
        RAISE NOTICE 'Column cpi_year_over_year already exists in inflation table';
    END IF;
END $$;

-- Recreate the v_economy_dashboard view to ensure it uses the column
DROP VIEW IF EXISTS v_economy_dashboard;

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
