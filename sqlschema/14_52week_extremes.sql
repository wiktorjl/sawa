-- ============================================
-- MATERIALIZED VIEW: 52-WEEK EXTREMES
-- ============================================
-- Pre-computes 52-week high/low per ticker per date using window functions.
-- Eliminates expensive per-query recalculation of 52-week highs/lows.
--
-- Refresh strategy:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_52week_extremes;
-- Should be run after daily data load (sawa update).

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_52week_extremes AS
SELECT
    ticker,
    date,
    MAX(high) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS high_52w,
    MIN(low) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS low_52w
FROM stock_prices;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_52week_extremes_ticker_date
    ON mv_52week_extremes (ticker, date);

-- Index for date-only lookups (e.g. latest date across all tickers)
CREATE INDEX IF NOT EXISTS idx_mv_52week_extremes_date
    ON mv_52week_extremes (date);

COMMENT ON MATERIALIZED VIEW mv_52week_extremes IS
    'Pre-computed 52-week high/low per ticker using 252-trading-day window. '
    'Refresh after daily data load with REFRESH MATERIALIZED VIEW CONCURRENTLY.';
