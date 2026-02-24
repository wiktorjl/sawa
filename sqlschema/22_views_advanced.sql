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
  -- Historical EOD (all days before today)
  SELECT
    ticker, date, open, high, low, close, volume,
    'historical'::text as data_source
  FROM stock_prices
  WHERE date < CURRENT_DATE

  UNION ALL

  -- Today's EOD (preferred when available)
  SELECT
    ticker, date, open, high, low, close, volume,
    'eod'::text as data_source
  FROM stock_prices
  WHERE date = CURRENT_DATE

  UNION ALL

  -- Today's intraday aggregated (only when EOD not available)
  -- Filter to regular market hours: 14:30-21:00 UTC (9:30 AM - 4:00 PM ET)
  SELECT DISTINCT ON (ticker)
    ticker,
    timestamp::date as date,
    (array_agg(open ORDER BY timestamp))[1] as open,
    MAX(high) as high,
    MIN(low) as low,
    (array_agg(close ORDER BY timestamp DESC))[1] as close,
    SUM(volume) as volume,
    'intraday'::text as data_source
  FROM stock_prices_intraday
  WHERE timestamp::date = CURRENT_DATE
    AND timestamp::time >= '14:30:00'
    AND timestamp::time < '21:00:00'
    AND NOT EXISTS (
      SELECT 1 FROM stock_prices sp
      WHERE sp.ticker = stock_prices_intraday.ticker
        AND sp.date = CURRENT_DATE
    )
  GROUP BY ticker, timestamp::date;

COMMENT ON VIEW stock_prices_live IS
  'Live prices: historical EOD + today intraday (switches to EOD when available)';
