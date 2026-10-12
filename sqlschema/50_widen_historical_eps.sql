-- Full-history financials include EPS adjusted for compounded reverse splits.
-- TNXP's 2013-09-30 EPS (-2,720,000,000) overflowed NUMERIC(12,4), rolling
-- back the entire income-statement import. ADTX reaches -1,183,104,576,000.
-- Preserve all four decimal places and give EPS the same integer headroom as
-- our adjusted prices/indicators. No rows are removed or rounded differently.
-- Preserve the dependent view's installed definition, owner, and grants.

DO $migration$
DECLARE
    view_definition TEXT;
    view_owner TEXT;
    view_grants JSONB;
    grant_record JSONB;
    column_record RECORD;
BEGIN
    IF pg_catalog.to_regclass('public.income_statements') IS NULL THEN
        RAISE NOTICE 'income_statements absent; nothing to widen';
        RETURN;
    END IF;

    -- Skip when both columns already have at least the requested headroom.
    IF (SELECT count(*) = 2 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'income_statements'
          AND column_name IN ('basic_earnings_per_share', 'diluted_earnings_per_share')
          AND data_type = 'numeric'
          AND (numeric_precision IS NULL
               OR (numeric_scale = 4 AND numeric_precision >= 20))) THEN
        RETURN;
    END IF;

    IF pg_catalog.to_regclass('public.v_latest_fundamentals') IS NOT NULL THEN
        SELECT pg_catalog.pg_get_viewdef(c.oid, true),
               pg_catalog.pg_get_userbyid(c.relowner),
               (SELECT jsonb_agg(jsonb_build_object(
                    'grantee', CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                                    ELSE pg_catalog.pg_get_userbyid(a.grantee) END,
                    'privilege', a.privilege_type, 'grantable', a.is_grantable))
                FROM pg_catalog.aclexplode(c.relacl) a)
          INTO view_definition, view_owner, view_grants
          FROM pg_catalog.pg_class c
         WHERE c.oid = 'public.v_latest_fundamentals'::pg_catalog.regclass;
        DROP VIEW public.v_latest_fundamentals;
    END IF;

    FOR column_record IN
        SELECT column_name, data_type, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'income_statements'
          AND column_name IN ('basic_earnings_per_share', 'diluted_earnings_per_share')
    LOOP
        IF column_record.data_type <> 'numeric' OR
           (column_record.numeric_precision IS NOT NULL AND column_record.numeric_scale <> 4) THEN
            RAISE EXCEPTION 'Unexpected income_statements.% type or scale; refusing conversion',
                            column_record.column_name;
        END IF;
        IF column_record.numeric_precision < 20 THEN
            EXECUTE pg_catalog.format(
                'ALTER TABLE public.income_statements ALTER COLUMN %I TYPE NUMERIC(20,4)',
                column_record.column_name);
        END IF;
    END LOOP;

    IF view_definition IS NOT NULL THEN
        EXECUTE pg_catalog.format('CREATE VIEW public.v_latest_fundamentals AS %s',
                                  view_definition);
        EXECUTE pg_catalog.format('ALTER VIEW public.v_latest_fundamentals OWNER TO %I',
                                  view_owner);
        FOR grant_record IN SELECT value FROM jsonb_array_elements(view_grants) LOOP
            EXECUTE pg_catalog.format('GRANT %s ON public.v_latest_fundamentals TO %s%s',
                grant_record ->> 'privilege',
                CASE WHEN grant_record ->> 'grantee' = 'PUBLIC' THEN 'PUBLIC'
                     ELSE pg_catalog.quote_ident(grant_record ->> 'grantee') END,
                CASE WHEN (grant_record ->> 'grantable')::boolean THEN ' WITH GRANT OPTION'
                     ELSE '' END);
        END LOOP;
    END IF;
END
$migration$;
