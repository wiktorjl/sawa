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

-- Step 2: rename the column.
ALTER TABLE market_internals RENAME COLUMN vix_close TO vix;

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
SELECT
    date,
    vix,
    vix3m,
    hy_spread,
    -- Term structure: <1 = backwardation (stress), >1 = contango (normal).
    vix3m / NULLIF(vix, 0) AS term_structure,
    -- Trailing 20-day mean and stddev for regime detection / z-scoring.
    AVG(vix) OVER (
        ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS vix_sma_20,
    STDDEV(vix) OVER (
        ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS vix_std_20,
    -- Where does today's VIX sit in the trailing 252-day distribution?
    PERCENT_RANK() OVER (
        ORDER BY vix
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS vix_pct_rank_252d,
    -- Same for HY spread (credit stress regime).
    PERCENT_RANK() OVER (
        ORDER BY hy_spread
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS hy_pct_rank_252d
FROM market_internals
ORDER BY date DESC;

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
