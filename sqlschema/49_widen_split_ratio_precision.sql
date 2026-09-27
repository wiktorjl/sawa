-- Preserve fractional share counts as exact integer ratios without rounding.
-- AXIA's 2025-12-22 1:1.2628378881074 event normalizes to
-- 5000000000000:6314189440537, which exceeds INTEGER but fits BIGINT.
-- This additive migration preserves every row and can safely be replayed.
-- Apply in one transaction; ALTER COLUMN TYPE takes an exclusive table lock.

DO $migration$
DECLARE
    column_name TEXT;
    column_type OID;
BEGIN
    IF pg_catalog.to_regclass('public.stock_splits') IS NULL THEN
        RAISE NOTICE 'stock_splits absent; nothing to widen';
        RETURN;
    END IF;

    FOREACH column_name IN ARRAY ARRAY['split_from', 'split_to'] LOOP
        SELECT a.atttypid INTO column_type
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = 'public.stock_splits'::pg_catalog.regclass
          AND a.attname = column_name
          AND a.attnum > 0 AND NOT a.attisdropped;

        IF column_type = 'pg_catalog.int8'::pg_catalog.regtype THEN
            CONTINUE;
        END IF;
        IF column_type IS NULL OR column_type NOT IN (
            'pg_catalog.int2'::pg_catalog.regtype,
            'pg_catalog.int4'::pg_catalog.regtype
        ) THEN
            RAISE EXCEPTION 'Unexpected stock_splits.% type %; refusing conversion',
                column_name, column_type;
        END IF;
        EXECUTE pg_catalog.format(
            'ALTER TABLE public.stock_splits ALTER COLUMN %I TYPE BIGINT', column_name
        );
    END LOOP;
END
$migration$;
