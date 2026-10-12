-- REST aggregate v and WebSocket dv include fractional shares. Unconstrained
-- NUMERIC preserves their decimal precision and all existing BIGINT values.
-- Preserve the installed live view, owner, grants, options and comment.
-- Unexpected dependent views fail atomically; never cascade-drop them.
DO $migration$
DECLARE
    volume_type TEXT;
    volume_precision INTEGER;
    view_definition TEXT;
    view_owner TEXT;
    view_grants JSONB;
    view_options TEXT[];
    view_comment TEXT;
    grant_record JSONB;
BEGIN
    SELECT data_type, numeric_precision INTO volume_type, volume_precision
      FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'stock_prices_intraday'
       AND column_name = 'volume';
    IF volume_type IS NULL OR (volume_type = 'numeric' AND volume_precision IS NULL) THEN
        RETURN;
    END IF;
    IF volume_type NOT IN ('bigint', 'integer', 'numeric') THEN
        RAISE EXCEPTION 'Unexpected stock_prices_intraday.volume type: %', volume_type;
    END IF;
    IF pg_catalog.to_regclass('public.stock_prices_live') IS NOT NULL THEN
        SELECT pg_catalog.pg_get_viewdef(c.oid, true),
               pg_catalog.pg_get_userbyid(c.relowner), c.reloptions,
               pg_catalog.obj_description(c.oid, 'pg_class'),
               (SELECT jsonb_agg(jsonb_build_object(
                    'grantee', CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                                    ELSE pg_catalog.pg_get_userbyid(a.grantee) END,
                    'privilege', a.privilege_type, 'grantable', a.is_grantable))
                FROM pg_catalog.aclexplode(c.relacl) a)
          INTO view_definition, view_owner, view_options, view_comment, view_grants
          FROM pg_catalog.pg_class c
         WHERE c.oid = 'public.stock_prices_live'::pg_catalog.regclass;
        DROP VIEW public.stock_prices_live;
    END IF;
    ALTER TABLE public.stock_prices_intraday ALTER COLUMN volume TYPE NUMERIC;
    IF view_definition IS NOT NULL THEN
        EXECUTE pg_catalog.format('CREATE VIEW public.stock_prices_live %s AS %s',
            CASE WHEN view_options IS NULL THEN ''
                 ELSE 'WITH (' || array_to_string(view_options, ', ') || ')' END,
            view_definition);
        EXECUTE pg_catalog.format('ALTER VIEW public.stock_prices_live OWNER TO %I', view_owner);
        EXECUTE pg_catalog.format('COMMENT ON VIEW public.stock_prices_live IS %L', view_comment);
        FOR grant_record IN SELECT value FROM jsonb_array_elements(view_grants) LOOP
            EXECUTE pg_catalog.format('GRANT %s ON public.stock_prices_live TO %s%s',
                grant_record ->> 'privilege',
                CASE WHEN grant_record ->> 'grantee' = 'PUBLIC' THEN 'PUBLIC'
                     ELSE pg_catalog.quote_ident(grant_record ->> 'grantee') END,
                CASE WHEN (grant_record ->> 'grantable')::boolean THEN ' WITH GRANT OPTION'
                     ELSE '' END);
        END LOOP;
    END IF;
END
$migration$;
