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
