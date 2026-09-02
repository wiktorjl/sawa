-- ============================================
-- Widen stock_prices OHLC beyond 10^12 for compounded reverse-split history
-- ============================================
-- Migration 42 widened these columns to NUMERIC(20,8) to preserve sub-penny
-- adjusted prices. It kept 12 integer digits, which caps a storable price just
-- under 10^12 — and a deep reverse-split ticker now exceeds that at the other
-- end of the range. When Polygon back-adjusts ADTX (cumulative ~6.1M-x factor)
-- its 2021 bars reach a high of 1,327,795,200,000, so four rows were rejected
-- as malformed on every split adjustment. Those four dates kept an older,
-- stale basis roughly 27x below their re-based neighbours:
--
--   2021-10-04  close   766,411,200,000   (re-based)
--   2021-10-05  close    30,736,000,000   (rejected, stale)
--   2021-10-06  close   766,411,200,000   (re-based)
--
-- Worse, refusing those rows made the provider response look incomplete, which
-- failed the whole split adjustment for every other ticker in the batch.
--
-- NUMERIC(24,8) keeps scale 8 (migration 42's sub-penny guarantee) and raises
-- the integer part to 16 digits. stock_prices_intraday is deliberately left at
-- NUMERIC(20,8): live bars are never back-adjusted and cannot approach 10^12.
--
-- The dependent views are dropped and recreated from their own stored
-- definitions rather than from copies pasted into this file, so this migration
-- cannot drift from whichever definition is current (46/45 redefined
-- stock_prices_live; a pasted copy here would silently revert it).
--
-- Idempotent: ALTER COLUMN TYPE is a no-op when the column already matches.

DO $migration$
DECLARE
    saved_views JSONB := '[]'::JSONB;
    saved_matview TEXT;
    saved_matview_indexes TEXT[] := ARRAY[]::TEXT[];
    view_record RECORD;
    view_entry JSONB;
    index_definition TEXT;
BEGIN
    IF pg_catalog.to_regclass('public.stock_prices') IS NULL THEN
        RAISE NOTICE 'stock_prices absent; nothing to widen';
        RETURN;
    END IF;

    -- Capture ordinary views in dependency order (v_company_summary may read
    -- stock_prices_live), newest dependencies dropped first.
    FOR view_record IN
        SELECT c.relname AS view_name,
               pg_catalog.pg_get_viewdef(c.oid, true) AS definition
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind = 'v'
          AND c.relname IN ('stock_prices_live', 'v_company_summary')
        ORDER BY c.relname
    LOOP
        saved_views := saved_views || pg_catalog.jsonb_build_object(
            'name', view_record.view_name,
            'definition', view_record.definition
        );
    END LOOP;

    IF pg_catalog.to_regclass('public.mv_52week_extremes') IS NOT NULL THEN
        SELECT pg_catalog.pg_get_viewdef(
            'public.mv_52week_extremes'::pg_catalog.regclass, true
        )
        INTO saved_matview;

        SELECT pg_catalog.array_agg(indexdef)
        INTO saved_matview_indexes
        FROM pg_catalog.pg_indexes
        WHERE schemaname = 'public' AND tablename = 'mv_52week_extremes';

        DROP MATERIALIZED VIEW public.mv_52week_extremes;
    END IF;

    DROP VIEW IF EXISTS public.v_company_summary;
    DROP VIEW IF EXISTS public.stock_prices_live;

    ALTER TABLE public.stock_prices
        ALTER COLUMN open  TYPE NUMERIC(24, 8),
        ALTER COLUMN high  TYPE NUMERIC(24, 8),
        ALTER COLUMN low   TYPE NUMERIC(24, 8),
        ALTER COLUMN close TYPE NUMERIC(24, 8);

    -- Recreate in the reverse of the drop order.
    FOR view_entry IN
        SELECT value
        FROM pg_catalog.jsonb_array_elements(saved_views)
        ORDER BY CASE value ->> 'name'
                     WHEN 'stock_prices_live' THEN 0
                     ELSE 1
                 END
    LOOP
        EXECUTE pg_catalog.format(
            'CREATE VIEW public.%I AS %s',
            view_entry ->> 'name',
            view_entry ->> 'definition'
        );
    END LOOP;

    IF saved_matview IS NOT NULL THEN
        EXECUTE pg_catalog.format(
            'CREATE MATERIALIZED VIEW public.mv_52week_extremes AS %s',
            saved_matview
        );
        FOREACH index_definition IN ARRAY saved_matview_indexes LOOP
            EXECUTE index_definition;
        END LOOP;
        REFRESH MATERIALIZED VIEW public.mv_52week_extremes;
    END IF;
END
$migration$;
