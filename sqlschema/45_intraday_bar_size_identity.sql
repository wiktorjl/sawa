-- ============================================
-- Make the stored intraday interval part of row identity
-- ============================================
-- Migration 21 originally keyed rows only by (ticker, timestamp). That causes
-- a 5-minute bar and a 15-minute bar beginning at the same timestamp to
-- overwrite/conflict with one another. This forward migration preserves every
-- row, assigns the historical default of 5 minutes where needed, and changes
-- the identity to (ticker, timestamp, bar_size_minutes).
--
-- Deployment note: replacing a primary/unique constraint takes an
-- ACCESS EXCLUSIVE table lock. Apply this atomic migration during a normal
-- schema-maintenance window; no row is deleted, truncated, or discarded.

ALTER TABLE public.stock_prices_intraday
    ADD COLUMN IF NOT EXISTS bar_size_minutes INTEGER;

ALTER TABLE public.stock_prices_intraday
    ALTER COLUMN bar_size_minutes SET DEFAULT 5;

-- Preserve legacy rows whose pre-migration column was nullable.
UPDATE public.stock_prices_intraday
SET bar_size_minutes = 5
WHERE bar_size_minutes IS NULL;

ALTER TABLE public.stock_prices_intraday
    ALTER COLUMN bar_size_minutes SET NOT NULL;

ALTER TABLE public.stock_prices_intraday
    ADD COLUMN IF NOT EXISTS source_minute_count INTEGER;

-- Legacy aggregates have no minute lineage. Treat them as complete for their
-- stored interval so a shorter partial replay cannot overwrite them; a full
-- equal-count provider correction can still replace them authoritatively.
UPDATE public.stock_prices_intraday
SET source_minute_count = bar_size_minutes
WHERE source_minute_count IS NULL;

ALTER TABLE public.stock_prices_intraday
    ALTER COLUMN source_minute_count SET DEFAULT 1,
    ALTER COLUMN source_minute_count SET NOT NULL;

ALTER TABLE public.stock_prices_intraday
    ADD COLUMN IF NOT EXISTS source_minute_mask BIGINT;

-- Conservatively mark every legacy interval minute present. This prevents an
-- incomparable partial replay after restart from replacing a historical row;
-- an equal full-mask provider correction remains authoritative.
UPDATE public.stock_prices_intraday
SET source_minute_mask = CASE
    WHEN bar_size_minutes BETWEEN 1 AND 60
    THEN (1::BIGINT << bar_size_minutes) - 1
    ELSE 1
END
WHERE source_minute_mask IS NULL;

ALTER TABLE public.stock_prices_intraday
    ALTER COLUMN source_minute_mask SET DEFAULT 1,
    ALTER COLUMN source_minute_mask SET NOT NULL;

-- Reconcile any lineage rows written by an intermediate deployment where the
-- bitmap existed but the redundant count was not constrained to match it.
UPDATE public.stock_prices_intraday
SET source_minute_count =
    pg_catalog.length(
        pg_catalog.replace((source_minute_mask::BIT(64))::TEXT, '0', '')
    )
WHERE source_minute_mask > 0
  AND bar_size_minutes BETWEEN 1 AND 60
  AND source_minute_mask < (1::BIGINT << bar_size_minutes)
  AND source_minute_count IS DISTINCT FROM
      pg_catalog.length(
          pg_catalog.replace((source_minute_mask::BIT(64))::TEXT, '0', '')
      );

DO $migration$
DECLARE
    current_primary_key TEXT;
    current_primary_columns SMALLINT[];
    expected_primary_columns SMALLINT[];
    legacy_unique RECORD;
    legacy_unique_index RECORD;
BEGIN
    SELECT pg_catalog.array_agg(a.attnum ORDER BY wanted.ordinality)::SMALLINT[]
    INTO expected_primary_columns
    FROM pg_catalog.unnest(ARRAY['ticker', 'timestamp', 'bar_size_minutes'])
         WITH ORDINALITY AS wanted(column_name, ordinality)
    JOIN pg_catalog.pg_attribute a
      ON a.attrelid = 'public.stock_prices_intraday'::pg_catalog.regclass
     AND a.attname = wanted.column_name
     AND NOT a.attisdropped;

    SELECT c.conname, c.conkey
    INTO current_primary_key, current_primary_columns
    FROM pg_catalog.pg_constraint c
    WHERE c.conrelid = 'public.stock_prices_intraday'::pg_catalog.regclass
      AND c.contype = 'p';

    IF current_primary_columns IS DISTINCT FROM expected_primary_columns THEN
        IF current_primary_key IS NOT NULL THEN
            EXECUTE pg_catalog.format(
                'ALTER TABLE public.stock_prices_intraday DROP CONSTRAINT %I',
                current_primary_key
            );
        END IF;

        ALTER TABLE public.stock_prices_intraday
            ADD CONSTRAINT stock_prices_intraday_pkey
            PRIMARY KEY (ticker, timestamp, bar_size_minutes);
    END IF;

    -- A legacy two-column UNIQUE constraint would still reject a second bar
    -- size even after the primary key is correct. Remove only that obsolete
    -- exact identity; no rows or columns are removed.
    FOR legacy_unique IN
        SELECT c.conname
        FROM pg_catalog.pg_constraint c
        WHERE c.conrelid = 'public.stock_prices_intraday'::pg_catalog.regclass
          AND c.contype = 'u'
          AND (
              SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname)
              FROM pg_catalog.unnest(c.conkey) AS key_column(attnum)
              JOIN pg_catalog.pg_attribute a
                ON a.attrelid = c.conrelid
               AND a.attnum = key_column.attnum
          ) = ARRAY['ticker', 'timestamp']::NAME[]
    LOOP
        EXECUTE pg_catalog.format(
            'ALTER TABLE public.stock_prices_intraday DROP CONSTRAINT %I',
            legacy_unique.conname
        );
    END LOOP;

    -- Also remove an equivalent standalone unique index. Constraint-backed
    -- indexes were handled above and are excluded here.
    FOR legacy_unique_index IN
        SELECT index_namespace.nspname AS schema_name, index_class.relname AS index_name
        FROM pg_catalog.pg_index index_metadata
        JOIN pg_catalog.pg_class index_class
          ON index_class.oid = index_metadata.indexrelid
        JOIN pg_catalog.pg_namespace index_namespace
          ON index_namespace.oid = index_class.relnamespace
        WHERE index_metadata.indrelid =
              'public.stock_prices_intraday'::pg_catalog.regclass
          AND index_metadata.indisunique
          AND NOT index_metadata.indisprimary
          AND index_metadata.indexprs IS NULL
          AND index_metadata.indpred IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_constraint constraint_metadata
              WHERE constraint_metadata.conindid = index_metadata.indexrelid
          )
          AND (
              SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname)
              FROM pg_catalog.unnest(index_metadata.indkey)
                   WITH ORDINALITY AS key_column(attnum, position)
              JOIN pg_catalog.pg_attribute a
                ON a.attrelid = index_metadata.indrelid
               AND a.attnum = key_column.attnum
              WHERE key_column.attnum > 0
                AND key_column.position <= index_metadata.indnkeyatts
          ) = ARRAY['ticker', 'timestamp']::NAME[]
    LOOP
        EXECUTE pg_catalog.format(
            'DROP INDEX %I.%I',
            legacy_unique_index.schema_name,
            legacy_unique_index.index_name
        );
    END LOOP;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.stock_prices_intraday'::pg_catalog.regclass
          AND conname = 'stock_prices_intraday_bar_size_minutes_check'
    ) THEN
        ALTER TABLE public.stock_prices_intraday
            ADD CONSTRAINT stock_prices_intraday_bar_size_minutes_check
            CHECK (bar_size_minutes IN (1, 5, 15, 30, 60))
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.stock_prices_intraday
        WHERE bar_size_minutes NOT IN (1, 5, 15, 30, 60)
    ) THEN
        ALTER TABLE public.stock_prices_intraday
            VALIDATE CONSTRAINT stock_prices_intraday_bar_size_minutes_check;
    ELSE
        RAISE NOTICE
            'unsupported legacy intraday bar sizes preserved; allowed-size check left NOT VALID';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.stock_prices_intraday'::pg_catalog.regclass
          AND conname = 'stock_prices_intraday_source_minute_count_check'
    ) THEN
        ALTER TABLE public.stock_prices_intraday
            ADD CONSTRAINT stock_prices_intraday_source_minute_count_check
            CHECK (
                source_minute_count BETWEEN 1 AND bar_size_minutes
            )
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.stock_prices_intraday
        WHERE source_minute_count NOT BETWEEN 1 AND bar_size_minutes
    ) THEN
        ALTER TABLE public.stock_prices_intraday
            VALIDATE CONSTRAINT stock_prices_intraday_source_minute_count_check;
    ELSE
        RAISE NOTICE
            'unsupported legacy intraday source counts preserved; completeness check left NOT VALID';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.stock_prices_intraday'::pg_catalog.regclass
          AND conname = 'stock_prices_intraday_source_minute_mask_check'
    ) THEN
        ALTER TABLE public.stock_prices_intraday
            ADD CONSTRAINT stock_prices_intraday_source_minute_mask_check
            CHECK (
                source_minute_mask > 0
                AND source_minute_mask < (1::BIGINT << bar_size_minutes)
                AND pg_catalog.length(
                    pg_catalog.replace(
                        (source_minute_mask::BIT(64))::TEXT,
                        '0',
                        ''
                    )
                ) = source_minute_count
            )
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.stock_prices_intraday
        WHERE source_minute_mask <= 0
           OR source_minute_mask >= (1::BIGINT << bar_size_minutes)
           OR pg_catalog.length(
               pg_catalog.replace(
                   (source_minute_mask::BIT(64))::TEXT,
                   '0',
                   ''
               )
           ) <> source_minute_count
    ) THEN
        ALTER TABLE public.stock_prices_intraday
            VALIDATE CONSTRAINT stock_prices_intraday_source_minute_mask_check;
    ELSE
        RAISE NOTICE
            'unsupported legacy intraday source masks preserved; lineage check left NOT VALID';
    END IF;

    -- Separate name intentionally upgrades intermediate deployments that had
    -- the range-only mask constraint under the canonical mask-check name.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.stock_prices_intraday'::pg_catalog.regclass
          AND conname = 'stock_prices_intraday_lineage_consistent'
    ) THEN
        ALTER TABLE public.stock_prices_intraday
            ADD CONSTRAINT stock_prices_intraday_lineage_consistent
            CHECK (
                pg_catalog.length(
                    pg_catalog.replace(
                        (source_minute_mask::BIT(64))::TEXT,
                        '0',
                        ''
                    )
                ) = source_minute_count
            )
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.stock_prices_intraday
        WHERE pg_catalog.length(
            pg_catalog.replace(
                (source_minute_mask::BIT(64))::TEXT,
                '0',
                ''
            )
        ) <> source_minute_count
    ) THEN
        ALTER TABLE public.stock_prices_intraday
            VALIDATE CONSTRAINT stock_prices_intraday_lineage_consistent;
    ELSE
        RAISE NOTICE
            'inconsistent legacy intraday lineage preserved; equality check left NOT VALID';
    END IF;
END
$migration$;

CREATE INDEX IF NOT EXISTS idx_intraday_ticker_size_timestamp
    ON public.stock_prices_intraday
       (ticker, bar_size_minutes, timestamp DESC);

-- The daily live candle must never mix resolutions: doing so double-counts
-- volume and makes open/close ambiguous. For each ticker today, select the
-- regular-session resolution with the strongest observed source-minute
-- coverage, then prefer an earlier bar, the finer interval, and finally the
-- later nominal covered end. Then aggregate only that resolution. The persisted count
-- makes a newly started sparse coarse collector lose to an established fine
-- feed instead of crediting a one-minute partial 60-minute window as 60. The
-- bitmap remains the exact lineage used for safe authoritative upserts; it is
-- intentionally not expanded in this latency-sensitive view.
CREATE OR REPLACE VIEW public.stock_prices_live AS
WITH market_clock AS (
    SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date AS market_date
),
intraday_resolution_coverage AS (
    SELECT
        spi.ticker,
        spi.bar_size_minutes,
        pg_catalog.sum(spi.source_minute_count) AS covered_minutes,
        pg_catalog.min(spi.timestamp) AS first_bar,
        pg_catalog.max(
            LEAST(
                spi.timestamp + spi.bar_size_minutes * INTERVAL '1 minute',
                (mc.market_date + TIME '16:00:00')
                    AT TIME ZONE 'America/New_York'
            )
        ) AS covered_end
    FROM public.stock_prices_intraday spi
    CROSS JOIN market_clock mc
    WHERE (spi.timestamp AT TIME ZONE 'America/New_York')::date = mc.market_date
      AND (spi.timestamp AT TIME ZONE 'America/New_York')::time >= TIME '09:30:00'
      AND (spi.timestamp AT TIME ZONE 'America/New_York')::time < TIME '16:00:00'
      AND spi.bar_size_minutes IN (1, 5, 15, 30, 60)
    GROUP BY spi.ticker, spi.bar_size_minutes, mc.market_date
),
chosen_intraday_resolution AS (
    SELECT DISTINCT ON (coverage.ticker)
        coverage.ticker,
        coverage.bar_size_minutes
    FROM intraday_resolution_coverage coverage
    ORDER BY
        coverage.ticker,
        coverage.covered_minutes DESC,
        coverage.first_bar ASC,
        coverage.bar_size_minutes ASC,
        coverage.covered_end DESC
)
SELECT
    sp.ticker,
    sp.date,
    sp.open,
    sp.high,
    sp.low,
    sp.close,
    sp.volume,
    'historical'::text AS data_source
FROM public.stock_prices sp
CROSS JOIN market_clock mc
WHERE sp.date < mc.market_date

UNION ALL

SELECT
    sp.ticker,
    sp.date,
    sp.open,
    sp.high,
    sp.low,
    sp.close,
    sp.volume,
    'eod'::text AS data_source
FROM public.stock_prices sp
CROSS JOIN market_clock mc
WHERE sp.date = mc.market_date

UNION ALL

SELECT
    spi.ticker,
    (spi.timestamp AT TIME ZONE 'America/New_York')::date AS date,
    (pg_catalog.array_agg(spi.open ORDER BY spi.timestamp))[1] AS open,
    pg_catalog.max(spi.high) AS high,
    pg_catalog.min(spi.low) AS low,
    (pg_catalog.array_agg(spi.close ORDER BY spi.timestamp DESC))[1] AS close,
    pg_catalog.sum(spi.volume) AS volume,
    'intraday'::text AS data_source
FROM public.stock_prices_intraday spi
JOIN chosen_intraday_resolution chosen
  ON chosen.ticker = spi.ticker
 AND chosen.bar_size_minutes = spi.bar_size_minutes
CROSS JOIN market_clock mc
WHERE (spi.timestamp AT TIME ZONE 'America/New_York')::date = mc.market_date
  AND (spi.timestamp AT TIME ZONE 'America/New_York')::time >= TIME '09:30:00'
  AND (spi.timestamp AT TIME ZONE 'America/New_York')::time < TIME '16:00:00'
  AND NOT EXISTS (
      SELECT 1
      FROM public.stock_prices sp
      WHERE sp.ticker = spi.ticker
        AND sp.date = mc.market_date
  )
GROUP BY spi.ticker, (spi.timestamp AT TIME ZONE 'America/New_York')::date;

COMMENT ON VIEW public.stock_prices_live IS
    'Live prices: historical EOD + today intraday from one strongest-coverage stored resolution (switches to EOD when available)';
