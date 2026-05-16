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
WITH base AS (
    SELECT
        mi.date,
        mi.vix,
        mi.vix3m,
        mi.hy_spread,
        mi.vix3m / NULLIF(mi.vix, 0) AS term_structure,
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
