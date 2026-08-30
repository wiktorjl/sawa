-- ============================================
-- Normalize dividend identity when dividend_type is absent
-- ============================================
-- PostgreSQL UNIQUE constraints treat NULL values as distinct before the
-- NULLS NOT DISTINCT feature introduced in PostgreSQL 15. Polygon may omit
-- dividend_type, so the legacy (ticker, ex_dividend_date, dividend_type)
-- constraint allowed an identical untyped dividend to be inserted on every
-- replay. This migration remains compatible with PostgreSQL 12 by using a
-- unique expression index over COALESCE(dividend_type, '').
--
-- Divergent legacy rows are never silently discarded. Deterministic
-- non-survivors are copied in full to dividend_identity_conflicts before they
-- are removed from the canonical table. The archive has no company foreign
-- key so later company cleanup cannot erase migration evidence.

CREATE TABLE IF NOT EXISTS public.dividend_identity_conflicts (
    original_dividend_id INTEGER PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    ex_dividend_date DATE NOT NULL,
    record_date DATE,
    pay_date DATE,
    cash_amount NUMERIC(10, 4),
    declaration_date DATE,
    dividend_type VARCHAR(20),
    frequency INTEGER,
    original_created_at TIMESTAMP,
    archive_reason TEXT NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.dividend_identity_conflicts IS
    'Recoverable archive of legacy dividend rows superseded while normalizing identity';

LOCK TABLE public.dividends IN SHARE ROW EXCLUSIVE MODE;

WITH ranked AS MATERIALIZED (
    SELECT
        d.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                d.ticker,
                d.ex_dividend_date,
                COALESCE(d.dividend_type, ''::character varying)
            ORDER BY
                (
                    (d.record_date IS NOT NULL)::INTEGER
                    + (d.pay_date IS NOT NULL)::INTEGER
                    + (d.cash_amount IS NOT NULL)::INTEGER
                    + (d.declaration_date IS NOT NULL)::INTEGER
                    + (d.frequency IS NOT NULL)::INTEGER
                ) DESC,
                d.created_at DESC NULLS LAST,
                d.id DESC
        ) AS identity_rank
    FROM public.dividends AS d
),
archived AS (
    INSERT INTO public.dividend_identity_conflicts (
        original_dividend_id,
        ticker,
        ex_dividend_date,
        record_date,
        pay_date,
        cash_amount,
        declaration_date,
        dividend_type,
        frequency,
        original_created_at,
        archive_reason
    )
    SELECT
        id,
        ticker,
        ex_dividend_date,
        record_date,
        pay_date,
        cash_amount,
        declaration_date,
        dividend_type,
        frequency,
        created_at,
        'normalized dividend identity conflict'
    FROM ranked
    WHERE identity_rank > 1
    ON CONFLICT (original_dividend_id) DO UPDATE SET
        archive_reason = EXCLUDED.archive_reason
    RETURNING original_dividend_id
)
DELETE FROM public.dividends AS d
USING archived AS a
WHERE d.id = a.original_dividend_id;

DO $migration$
DECLARE
    legacy_unique RECORD;
BEGIN
    FOR legacy_unique IN
        SELECT c.conname
        FROM pg_catalog.pg_constraint AS c
        WHERE c.conrelid = 'public.dividends'::pg_catalog.regclass
          AND c.contype = 'u'
          AND (
              SELECT pg_catalog.array_agg(a.attname ORDER BY key_column.position)
              FROM pg_catalog.unnest(c.conkey)
                   WITH ORDINALITY AS key_column(attnum, position)
              JOIN pg_catalog.pg_attribute AS a
                ON a.attrelid = c.conrelid
               AND a.attnum = key_column.attnum
          ) = ARRAY['ticker', 'ex_dividend_date', 'dividend_type']::NAME[]
    LOOP
        EXECUTE pg_catalog.format(
            'ALTER TABLE public.dividends DROP CONSTRAINT %I',
            legacy_unique.conname
        );
    END LOOP;
END
$migration$;

-- Rebuild our named index on every replay. IF NOT EXISTS alone could silently
-- accept a same-named non-unique or wrong-expression index and leave the
-- loader's ON CONFLICT target unusable.
DROP INDEX IF EXISTS public.dividends_normalized_identity_uidx;

CREATE UNIQUE INDEX dividends_normalized_identity_uidx
    ON public.dividends (
        ticker,
        ex_dividend_date,
        (COALESCE(dividend_type, ''::character varying))
    );
