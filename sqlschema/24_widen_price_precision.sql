-- Widen NUMERIC precision on price columns to accommodate
-- split-adjusted prices that may exceed NUMERIC(12,4) range.
-- NUMERIC(16,4) supports values up to 999,999,999,999.9999.

-- Drop all dependent views first
DROP VIEW IF EXISTS stock_prices_live;
DROP VIEW IF EXISTS v_company_summary;
DROP MATERIALIZED VIEW IF EXISTS mv_52week_extremes;

-- Widen stock_prices columns
ALTER TABLE stock_prices
    ALTER COLUMN open TYPE NUMERIC(16, 4),
    ALTER COLUMN high TYPE NUMERIC(16, 4),
    ALTER COLUMN low TYPE NUMERIC(16, 4),
    ALTER COLUMN close TYPE NUMERIC(16, 4);

-- Widen stock_prices_intraday columns
ALTER TABLE stock_prices_intraday
    ALTER COLUMN open TYPE NUMERIC(16, 4),
    ALTER COLUMN high TYPE NUMERIC(16, 4),
    ALTER COLUMN low TYPE NUMERIC(16, 4),
    ALTER COLUMN close TYPE NUMERIC(16, 4);

-- Recreate stock_prices_live view
CREATE OR REPLACE VIEW stock_prices_live AS
WITH market_clock AS (
  SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date AS market_date
)
  SELECT
    sp.ticker, sp.date, sp.open, sp.high, sp.low, sp.close, sp.volume,
    'historical'::text as data_source
  FROM stock_prices sp
  CROSS JOIN market_clock mc
  WHERE sp.date < mc.market_date

  UNION ALL

  SELECT
    sp.ticker, sp.date, sp.open, sp.high, sp.low, sp.close, sp.volume,
    'eod'::text as data_source
  FROM stock_prices sp
  CROSS JOIN market_clock mc
  WHERE sp.date = mc.market_date

  UNION ALL

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

-- Recreate v_company_summary view
CREATE OR REPLACE VIEW v_company_summary AS
SELECT
    c.*,
    sp.close AS latest_price,
    sp.date AS price_date,
    fr.price_to_earnings AS latest_pe,
    fr.debt_to_equity AS latest_debt_equity,
    fr.return_on_equity AS latest_roe,
    fr.dividend_yield AS latest_dividend_yield
FROM companies c
LEFT JOIN LATERAL (
    SELECT close, date FROM stock_prices
    WHERE ticker = c.ticker ORDER BY date DESC LIMIT 1
) sp ON true
LEFT JOIN LATERAL (
    SELECT price_to_earnings, debt_to_equity, return_on_equity, dividend_yield
    FROM financial_ratios
    WHERE ticker = c.ticker ORDER BY date DESC LIMIT 1
) fr ON true
WHERE c.active = true;

-- Recreate materialized view
CREATE MATERIALIZED VIEW mv_52week_extremes AS
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

CREATE UNIQUE INDEX idx_mv_52week_extremes_ticker_date
    ON mv_52week_extremes (ticker, date);

CREATE INDEX idx_mv_52week_extremes_date
    ON mv_52week_extremes (date);
