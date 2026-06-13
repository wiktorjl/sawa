-- ============================================
-- Economy dashboard date spine + data-integrity constraints
-- ============================================
-- Addresses three review findings:
--   1. v_economy_dashboard was driven FROM treasury_yields, so its newest row
--      was bounded by the latest treasury date — hiding VIX/VIX3M/HY-spread
--      days that market_internals already has (treasury lags ~a week). And the
--      monthly series (inflation, inflation_expectations, labor_market) were
--      equality-joined on the daily date, so they were NULL on almost every row.
--      Rebuilt below on a full date spine with as-of joins for the monthly
--      series (most recent value on or before each date).
--   2. stock_prices admitted zero/negative OHLC and inverted high<low rows.
--   3. news_sentiment.sentiment had no domain constraint, so out-of-spec values
--      could be stored.
-- Also drops BRIN indexes on uncorrelated technical_indicator value columns:
-- BRIN only prunes on physically-clustered columns (here, date), so these
-- indexes never prune and only add write cost / mislead the planner.
--
-- Idempotent: safe to re-run and applied by both fresh coldstart and upgrades.

-- ── 1. Rebuild v_economy_dashboard on a full date spine ──────────────────────
CREATE OR REPLACE VIEW v_economy_dashboard AS
WITH date_spine AS (
    SELECT date FROM treasury_yields
    UNION SELECT date FROM market_internals
    UNION SELECT date FROM inflation
    UNION SELECT date FROM inflation_expectations
    UNION SELECT date FROM labor_market
)
SELECT
    d.date,
    ty.yield_1_month,
    ty.yield_3_month,
    ty.yield_10_year,
    ty.yield_30_year,
    i.cpi,
    i.cpi_year_over_year AS inflation_yoy,
    ie.market_5_year AS inflation_expectation_5y,
    ie.market_10_year AS inflation_expectation_10y,
    lm.unemployment_rate,
    lm.job_openings,
    mi.vix,
    mi.vix3m,
    mi.hy_spread
FROM date_spine d
-- Daily series: exact-date join (NULL when that series has no row yet — honest
-- about treasury lagging rather than implying fresher data than exists).
LEFT JOIN treasury_yields ty ON ty.date = d.date
LEFT JOIN market_internals mi ON mi.date = d.date
-- Monthly series: as-of join — the most recent published value on or before the
-- spine date, carried forward until the next release.
LEFT JOIN LATERAL (
    SELECT cpi, cpi_year_over_year
    FROM inflation i2
    WHERE i2.date <= d.date
    ORDER BY i2.date DESC
    LIMIT 1
) i ON true
LEFT JOIN LATERAL (
    SELECT market_5_year, market_10_year
    FROM inflation_expectations ie2
    WHERE ie2.date <= d.date
    ORDER BY ie2.date DESC
    LIMIT 1
) ie ON true
LEFT JOIN LATERAL (
    SELECT unemployment_rate, job_openings
    FROM labor_market lm2
    WHERE lm2.date <= d.date
    ORDER BY lm2.date DESC
    LIMIT 1
) lm ON true
ORDER BY d.date DESC;

-- ── 2. stock_prices OHLC integrity ───────────────────────────────────────────
-- Remove the handful of existing junk rows (sub-penny microcaps that collapse to
-- 0.0000 at NUMERIC(16,4), plus any non-positive/inverted rows) so the CHECK can
-- be validated, then constrain. The daily loader already filters these before
-- upsert; the constraint is the backstop.
DELETE FROM stock_prices
WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
   OR high < low
   OR volume < 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'stock_prices_ohlcv_sane'
    ) THEN
        ALTER TABLE stock_prices
            ADD CONSTRAINT stock_prices_ohlcv_sane
            CHECK (
                open > 0 AND high > 0 AND low > 0 AND close > 0
                AND high >= low
                AND volume >= 0
            );
    END IF;
END $$;

-- ── 3. news_sentiment domain constraint ──────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'news_sentiment_value_valid'
    ) THEN
        ALTER TABLE news_sentiment
            ADD CONSTRAINT news_sentiment_value_valid
            CHECK (sentiment IS NULL OR sentiment IN
                ('positive', 'negative', 'neutral', 'mixed'));
    END IF;
END $$;

-- ── 4. Drop no-op BRIN indexes on uncorrelated TA value columns ──────────────
DROP INDEX IF EXISTS idx_ta_rsi_14;
DROP INDEX IF EXISTS idx_ta_rsi_21;
DROP INDEX IF EXISTS idx_ta_atr_14;
DROP INDEX IF EXISTS idx_ta_macd_line;
DROP INDEX IF EXISTS idx_ta_volume_ratio;
DROP INDEX IF EXISTS idx_ta_sma_100;
DROP INDEX IF EXISTS idx_ta_sma_150;
DROP INDEX IF EXISTS idx_ta_sma_200;
DROP INDEX IF EXISTS idx_ta_adx_14;
DROP INDEX IF EXISTS idx_ta_bb_width_pct;
DROP INDEX IF EXISTS idx_ta_dollar_volume_sma_20;
