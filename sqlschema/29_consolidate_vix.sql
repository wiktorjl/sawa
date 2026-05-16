-- ============================================
-- Consolidate VIX to a single source: market_internals
-- ============================================
-- Previously VIX lived in two places:
--   (a) market_internals.vix_close (FRED VIXCLS, daily close)
--   (b) stock_prices(^VIX), stock_prices(^VIX3M), and the related
--       technical_indicators rows (originally fetched from Polygon's
--       ^VIX caret-prefix tickers, which Polygon does not actually serve;
--       briefly kept in sync via a mirror function).
--
-- This migration:
--   1. Renames market_internals.vix_close -> market_internals.vix
--      (the row is already a daily snapshot; the _close suffix was noise).
--   2. Recreates v_economy_dashboard against the new column.
--   3. Adds v_market_internals_enriched with VIX-native metrics
--      (term structure, 20-day SMA/stddev, 252-day percentile rank).
--   4. Deletes the duplicate ^VIX / ^VIX3M data from stock_prices,
--      technical_indicators, and companies.

-- Step 1: drop the view that depends on vix_close so we can rename the column.
DROP VIEW IF EXISTS v_economy_dashboard;

-- Step 2: rename the column when upgrading databases that still have the
-- legacy name. Fresh schemas already create market_internals.vix.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'market_internals'
          AND column_name = 'vix_close'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'market_internals'
          AND column_name = 'vix'
    ) THEN
        ALTER TABLE market_internals RENAME COLUMN vix_close TO vix;
    END IF;
END $$;

-- Step 3: recreate v_economy_dashboard against mi.vix.
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
    lm.job_openings,
    mi.vix,
    mi.vix3m,
    mi.hy_spread
FROM treasury_yields ty
LEFT JOIN inflation i ON ty.date = i.date
LEFT JOIN inflation_expectations ie ON ty.date = ie.date
LEFT JOIN labor_market lm ON ty.date = lm.date
LEFT JOIN market_internals mi ON ty.date = mi.date
ORDER BY ty.date DESC;

-- Step 4: enriched view with VIX-native metrics.
-- Window functions compute trailing aggregates over trading days in the
-- table. Dataset is small (~1.3K rows), so unmaterialized is fine.
CREATE OR REPLACE VIEW v_market_internals_enriched AS
WITH base AS (
    SELECT
        mi.date,
        mi.vix,
        mi.vix3m,
        mi.hy_spread,
        -- Term structure: <1 = backwardation (stress), >1 = contango (normal).
        mi.vix3m / NULLIF(mi.vix, 0) AS term_structure,
        -- Trailing 20-day mean and stddev for regime detection / z-scoring.
        AVG(mi.vix) OVER (
            ORDER BY mi.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vix_sma_20,
        STDDEV(mi.vix) OVER (
            ORDER BY mi.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vix_std_20
    FROM market_internals mi
)
SELECT
    b.date,
    b.vix,
    b.vix3m,
    b.hy_spread,
    b.term_structure,
    b.vix_sma_20,
    b.vix_std_20,
    vr.vix_pct_rank_252d,
    hr.hy_pct_rank_252d
FROM base b
LEFT JOIN LATERAL (
    SELECT
        CASE
            WHEN b.vix IS NULL OR COUNT(w.vix) <= 1 THEN NULL
            ELSE (COUNT(*) FILTER (WHERE w.vix < b.vix))::numeric
                / NULLIF(COUNT(w.vix) - 1, 0)
        END AS vix_pct_rank_252d
    FROM (
        SELECT mi2.vix
        FROM market_internals mi2
        WHERE mi2.date <= b.date
          AND mi2.vix IS NOT NULL
        ORDER BY mi2.date DESC
        LIMIT 252
    ) w
) vr ON true
LEFT JOIN LATERAL (
    SELECT
        CASE
            WHEN b.hy_spread IS NULL OR COUNT(w.hy_spread) <= 1 THEN NULL
            ELSE (COUNT(*) FILTER (WHERE w.hy_spread < b.hy_spread))::numeric
                / NULLIF(COUNT(w.hy_spread) - 1, 0)
        END AS hy_pct_rank_252d
    FROM (
        SELECT mi2.hy_spread
        FROM market_internals mi2
        WHERE mi2.date <= b.date
          AND mi2.hy_spread IS NOT NULL
        ORDER BY mi2.date DESC
        LIMIT 252
    ) w
) hr ON true
ORDER BY b.date DESC;

-- Step 5: delete the duplicate stock-like VIX rows.
-- Order matters: technical_indicators and stock_prices both reference
-- companies via foreign key, so children first.
DELETE FROM technical_indicators WHERE ticker IN ('^VIX', '^VIX3M');
DELETE FROM stock_prices         WHERE ticker IN ('^VIX', '^VIX3M');
DELETE FROM companies            WHERE ticker IN ('^VIX', '^VIX3M');

-- Step 6: drop the empty vix_intraday table. It was sourced from Polygon
-- I:VIX (requires paid Indices plan; returns 403 on ours) and never
-- received data. No code references it.
DROP TABLE IF EXISTS vix_intraday;
