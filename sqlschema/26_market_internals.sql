-- ============================================
-- MARKET INTERNALS TABLE
-- ============================================
-- Daily market sentiment/volatility indicators
-- sourced from FRED (VIX, HY spread) and CBOE.
-- Separate from economy tables (macro fundamentals)
-- because these are daily market-derived indicators.

CREATE TABLE IF NOT EXISTS market_internals (
    date DATE PRIMARY KEY,
    vix NUMERIC(8, 4),
    vix3m NUMERIC(8, 4),
    hy_spread NUMERIC(8, 4),
    put_call_ratio NUMERIC(8, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_market_internals_date
    ON market_internals (date DESC);

-- Economy dashboard view (most recent data with all indicators).
-- Includes market_internals (VIX, VIX3M, HY spread) from FRED so volatility
-- regime shows up alongside rates, inflation, and labor data.
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

-- Enriched view: VIX-native derivations layered on top of the raw FRED
-- columns. Dataset is small (~1.3K rows), so windowed aggregates are cheap
-- enough to compute on read.
CREATE OR REPLACE VIEW v_market_internals_enriched AS
SELECT
    date,
    vix,
    vix3m,
    hy_spread,
    vix3m / NULLIF(vix, 0) AS term_structure,
    AVG(vix) OVER (
        ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS vix_sma_20,
    STDDEV(vix) OVER (
        ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS vix_std_20,
    PERCENT_RANK() OVER (
        ORDER BY vix
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS vix_pct_rank_252d,
    PERCENT_RANK() OVER (
        ORDER BY hy_spread
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS hy_pct_rank_252d
FROM market_internals
ORDER BY date DESC;
