-- ============================================
-- Non-destructive OHLCV completeness guards
-- ============================================
-- Earlier CHECK constraints compare OHLCV values but SQL CHECK treats NULL as
-- unknown/satisfied. That allowed incomplete daily and intraday bars to enter
-- the database and later fail numeric/TA calculations.
--
-- Upgrade policy:
--   * never DELETE or rewrite a legacy row;
--   * add NOT VALID completeness checks, which immediately reject incomplete
--     new/updated rows while preserving any existing NULL-bearing rows;
--   * when a table is already clean, validate the check and promote the five
--     columns to physical NOT NULL metadata;
--   * if legacy NULLs exist, emit a NOTICE and leave the check NOT VALID. After
--     an operator repairs those rows, re-running this idempotent migration will
--     validate it and set NOT NULL.

DO $migration$
BEGIN
    IF pg_catalog.to_regclass('public.stock_prices') IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_constraint
            WHERE conrelid = 'public.stock_prices'::pg_catalog.regclass
              AND conname = 'stock_prices_ohlcv_complete'
        ) THEN
            ALTER TABLE public.stock_prices
                ADD CONSTRAINT stock_prices_ohlcv_complete
                CHECK (
                    open IS NOT NULL
                    AND high IS NOT NULL
                    AND low IS NOT NULL
                    AND close IS NOT NULL
                    AND volume IS NOT NULL
                )
                NOT VALID;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM public.stock_prices
            WHERE open IS NULL
               OR high IS NULL
               OR low IS NULL
               OR close IS NULL
               OR volume IS NULL
        ) THEN
            ALTER TABLE public.stock_prices
                VALIDATE CONSTRAINT stock_prices_ohlcv_complete;
            ALTER TABLE public.stock_prices
                ALTER COLUMN open SET NOT NULL,
                ALTER COLUMN high SET NOT NULL,
                ALTER COLUMN low SET NOT NULL,
                ALTER COLUMN close SET NOT NULL,
                ALTER COLUMN volume SET NOT NULL;
        ELSE
            RAISE NOTICE
                'stock_prices contains legacy NULL OHLCV rows; rows preserved, completeness guard left NOT VALID';
        END IF;
    END IF;

    IF pg_catalog.to_regclass('public.stock_prices_intraday') IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_constraint
            WHERE conrelid = 'public.stock_prices_intraday'::pg_catalog.regclass
              AND conname = 'stock_prices_intraday_ohlcv_complete'
        ) THEN
            ALTER TABLE public.stock_prices_intraday
                ADD CONSTRAINT stock_prices_intraday_ohlcv_complete
                CHECK (
                    open IS NOT NULL
                    AND high IS NOT NULL
                    AND low IS NOT NULL
                    AND close IS NOT NULL
                    AND volume IS NOT NULL
                )
                NOT VALID;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM public.stock_prices_intraday
            WHERE open IS NULL
               OR high IS NULL
               OR low IS NULL
               OR close IS NULL
               OR volume IS NULL
        ) THEN
            ALTER TABLE public.stock_prices_intraday
                VALIDATE CONSTRAINT stock_prices_intraday_ohlcv_complete;
            ALTER TABLE public.stock_prices_intraday
                ALTER COLUMN open SET NOT NULL,
                ALTER COLUMN high SET NOT NULL,
                ALTER COLUMN low SET NOT NULL,
                ALTER COLUMN close SET NOT NULL,
                ALTER COLUMN volume SET NOT NULL;
        ELSE
            RAISE NOTICE
                'stock_prices_intraday contains legacy NULL OHLCV rows; rows preserved, completeness guard left NOT VALID';
        END IF;
    END IF;
END
$migration$;
