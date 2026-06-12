-- ============================================
-- Repair schema drift: re-apply 33's intent in a safe order
-- ============================================
-- Migration 33 could never apply to a database where stock_prices_live
-- already existed: its ALTER COLUMN ... TYPE runs while the view still
-- references stock_prices_intraday.timestamp, which PostgreSQL rejects,
-- and schema.py executes each file as a single transaction, so the whole
-- file rolled back every run. Deployed databases were left with a stale
-- pre-22 view whose DISTINCT ON arm blocks qual pushdown into the UNION
-- ALL branches — every date-keyed query against the view materializes all
-- ~9.5M rows, and the market-wide MCP tools exceed the 30s statement
-- timeout. The stale view also filters intraday bars to a fixed
-- 14:30–21:00 UTC window (EST market hours), which is shifted by an hour
-- during EDT.
--
-- This file restates 33 idempotently with the view dropped FIRST so the
-- column conversion can proceed.

DROP VIEW IF EXISTS stock_prices_live;

-- The old expression index (timestamp::date) must go before the column
-- conversion: rebuilding it against TIMESTAMPTZ would use the session-
-- timezone-dependent (non-immutable) timestamptz->date cast and fail.
DROP INDEX IF EXISTS idx_intraday_date;

-- Treat existing naive intraday timestamps as UTC and store as TIMESTAMPTZ
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'stock_prices_intraday'
          AND column_name = 'timestamp'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE stock_prices_intraday
            ALTER COLUMN timestamp TYPE TIMESTAMPTZ
            USING timestamp AT TIME ZONE 'UTC';
    END IF;
END $$;

-- Index the America/New_York market date used by the view's intraday arm
CREATE INDEX idx_intraday_date
    ON stock_prices_intraday(((timestamp AT TIME ZONE 'America/New_York')::date));

CREATE VIEW stock_prices_live AS
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
  'Live prices: historical EOD + today intraday by America/New_York market date (switches to EOD when available)';

-- Widen stock-character price-shaped fields (from 33)
ALTER TABLE stock_character_flags
    ALTER COLUMN value TYPE NUMERIC(20, 6),
    ALTER COLUMN threshold TYPE NUMERIC(20, 6);

ALTER TABLE stock_character_scorecard
    ALTER COLUMN current_price TYPE NUMERIC(16, 4);

-- Stock-character company FKs as NOT VALID for existing rows (from 33)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'stock_character_classification_ticker_fkey'
    ) THEN
        ALTER TABLE stock_character_classification
            ADD CONSTRAINT stock_character_classification_ticker_fkey
            FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'stock_character_baseline_ticker_fkey'
    ) THEN
        ALTER TABLE stock_character_baseline
            ADD CONSTRAINT stock_character_baseline_ticker_fkey
            FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'stock_character_flags_ticker_fkey'
    ) THEN
        ALTER TABLE stock_character_flags
            ADD CONSTRAINT stock_character_flags_ticker_fkey
            FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'stock_character_scorecard_ticker_fkey'
    ) THEN
        ALTER TABLE stock_character_scorecard
            ADD CONSTRAINT stock_character_scorecard_ticker_fkey
            FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE
            NOT VALID;
    END IF;
END $$;

-- Volatility percentile ranks over trailing 252-row windows (from 33).
-- Drop first: 29's PERCENT_RANK() columns are double precision and CREATE
-- OR REPLACE cannot change a view column's type to numeric.
DROP VIEW IF EXISTS v_market_internals_enriched;
CREATE VIEW v_market_internals_enriched AS
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
