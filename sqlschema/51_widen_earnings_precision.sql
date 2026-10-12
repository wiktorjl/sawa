-- Historical EPS reflects compounded reverse splits. Observed Yahoo values
-- reach 6 trillion (ASTI), overflowing NUMERIC(10,4). Keep the four-decimal
-- scale and widen integer headroom without narrowing an existing wider type.
DO $migration$
DECLARE
    item RECORD;
BEGIN
    IF pg_catalog.to_regclass('public.earnings') IS NULL THEN
        RETURN;
    END IF;
    FOR item IN
        SELECT column_name, data_type, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'earnings'
          AND column_name IN ('eps_estimate', 'eps_actual', 'surprise_pct')
    LOOP
        IF item.data_type <> 'numeric' OR
           (item.numeric_precision IS NOT NULL AND item.numeric_scale <> 4) THEN
            RAISE EXCEPTION 'Unexpected earnings.% type or scale', item.column_name;
        END IF;
        IF item.numeric_precision < 24 THEN
            EXECUTE pg_catalog.format(
                'ALTER TABLE public.earnings ALTER COLUMN %I TYPE NUMERIC(24,4)',
                item.column_name);
        END IF;
    END LOOP;
END
$migration$;
