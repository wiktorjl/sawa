-- ============================================
-- ADVANCED VIEWS WITH DEPENDENCIES
-- ============================================
-- These views depend on tables created in later schema files:
-- - indices, index_constituents (from 12_indices.sql)
-- - stock_prices_intraday (from 21_intraday_prices.sql)
--
-- This file must be run after those dependencies are created.

-- Company with index membership view
CREATE OR REPLACE VIEW v_company_with_indices AS
SELECT
    c.ticker,
    c.name,
    c.market_cap,
    c.sic_description as sector,
    c.primary_exchange as exchange,
    c.active,
    COALESCE(
        (SELECT array_agg(i.code ORDER BY i.name)
         FROM index_constituents ic
         JOIN indices i ON ic.index_id = i.id
         WHERE ic.ticker = c.ticker),
        ARRAY[]::varchar[]
    ) as indices,
    (EXISTS (
        SELECT 1 FROM index_constituents ic
        JOIN indices i ON ic.index_id = i.id
        WHERE ic.ticker = c.ticker AND i.code = 'sp500'
    )) as in_sp500,
    (EXISTS (
        SELECT 1 FROM index_constituents ic
        JOIN indices i ON ic.index_id = i.id
        WHERE ic.ticker = c.ticker AND i.code = 'nasdaq5000'
    )) as in_nasdaq5000
FROM companies c;

-- ============================================
-- LIVE STOCK PRICES VIEW
-- Combines historical EOD + today's intraday
-- ============================================

CREATE OR REPLACE VIEW stock_prices_live AS
WITH market_clock AS (
  SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date AS market_date
)
  -- Historical EOD (all days before today)
  SELECT
    sp.ticker, sp.date, sp.open, sp.high, sp.low, sp.close, sp.volume,
    'historical'::text as data_source
  FROM stock_prices sp
  CROSS JOIN market_clock mc
  WHERE sp.date < mc.market_date

  UNION ALL

  -- Today's EOD (preferred when available)
  SELECT
    sp.ticker, sp.date, sp.open, sp.high, sp.low, sp.close, sp.volume,
    'eod'::text as data_source
  FROM stock_prices sp
  CROSS JOIN market_clock mc
  WHERE sp.date = mc.market_date

  UNION ALL

  -- Today's intraday aggregated (only when EOD not available)
  -- Filter to regular market hours in America/New_York to handle DST.
  SELECT
    spi.ticker,
    (spi.timestamp AT TIME ZONE 'America/New_York')::date as date,
    (array_agg(spi.open ORDER BY spi.timestamp))[1] as open,
    MAX(spi.high) as high,
    MIN(spi.low) as low,
    (array_agg(spi.close ORDER BY spi.timestamp DESC))[1] as close,
    SUM(spi.volume) as volume,
    'intraday'::text as data_source
  FROM stock_prices_intraday spi
  CROSS JOIN market_clock mc
  WHERE (spi.timestamp AT TIME ZONE 'America/New_York')::date = mc.market_date
    AND (spi.timestamp AT TIME ZONE 'America/New_York')::time >= TIME '09:30:00'
    AND (spi.timestamp AT TIME ZONE 'America/New_York')::time < TIME '16:00:00'
    AND NOT EXISTS (
      SELECT 1 FROM stock_prices sp
      WHERE sp.ticker = spi.ticker
        AND sp.date = mc.market_date
    )
  GROUP BY spi.ticker, (spi.timestamp AT TIME ZONE 'America/New_York')::date;

COMMENT ON VIEW stock_prices_live IS
  'Live prices: historical EOD + today intraday (switches to EOD when available)';
