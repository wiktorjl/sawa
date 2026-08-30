-- Ticker-level GICS overrides table.
--
-- Replaces the hard-coded CASE block previously in
-- get_gics_sector() (13_gics_sector_function.sql). The CASE block had
-- 6 entries — enough for a handful of well-known foreign ADRs but
-- not for the ~348 active ADRs Polygon doesn't carry a SIC for.
-- Moving overrides into a table lets a backfill script
-- (scripts/backfill_gics_overrides.py, yfinance-driven) populate the
-- long tail without anyone editing SQL.
--
-- get_gics_sector() now consults this table FIRST, then falls back to
-- the sic_gics_mapping table on sic_code, then to the provided SIC
-- description. The seed rows below match the previous CASE block
-- exactly so existing classifications don't change on this migration.
--
-- A fresh schema is installed before companies are loaded.  The idempotent
-- seed function below joins to companies rather than creating placeholders or
-- weakening the foreign key.  It runs once for no-drop upgrades and a company
-- trigger invokes it for matching companies loaded later.
--
-- Source column values:
--   manual   — curated by hand (the 6 legacy entries below)
--   yfinance — populated by scripts/backfill_gics_overrides.py
--
-- Confidence: same meaning as sic_gics_mapping (high/medium/low).

CREATE TABLE IF NOT EXISTS gics_overrides (
    ticker VARCHAR(10) PRIMARY KEY REFERENCES companies(ticker) ON DELETE CASCADE,
    gics_sector VARCHAR(50) NOT NULL,
    gics_industry VARCHAR(100),
    confidence VARCHAR(10) NOT NULL DEFAULT 'medium',
    source VARCHAR(20) NOT NULL,
    notes TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gics_overrides_sector ON gics_overrides (gics_sector);

CREATE OR REPLACE FUNCTION public.seed_legacy_gics_overrides(
    requested_ticker VARCHAR(10) DEFAULT NULL
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH legacy_overrides (
        ticker,
        gics_sector,
        gics_industry,
        confidence,
        source,
        notes
    ) AS (
        VALUES
            ('ASML', 'Information Technology', 'Semiconductor Equipment',    'high', 'manual', 'Dutch ADR - semiconductor lithography'),
            ('ARM',  'Information Technology', 'Semiconductors',             'high', 'manual', 'UK ADR - semiconductor IP licensing'),
            ('PDD',  'Consumer Discretionary', 'Internet Retail',            'high', 'manual', 'Chinese ADR - e-commerce platform'),
            ('TRI',  'Industrials',            'Professional Services',      'high', 'manual', 'Canadian - financial data & legal information services'),
            ('FER',  'Industrials',            'Construction & Engineering', 'high', 'manual', 'Spanish - infrastructure and construction'),
            ('CCEP', 'Consumer Staples',       'Soft Drinks',                'high', 'manual', 'European Coca-Cola bottler')
    )
    INSERT INTO public.gics_overrides (
        ticker,
        gics_sector,
        gics_industry,
        confidence,
        source,
        notes
    )
    SELECT
        seed.ticker,
        seed.gics_sector,
        seed.gics_industry,
        seed.confidence,
        seed.source,
        seed.notes
    FROM legacy_overrides AS seed
    INNER JOIN public.companies AS company ON company.ticker = seed.ticker
    WHERE requested_ticker IS NULL OR seed.ticker = requested_ticker
    ON CONFLICT (ticker) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$;

CREATE OR REPLACE FUNCTION public.seed_legacy_gics_override_after_company_change()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM public.seed_legacy_gics_overrides(NEW.ticker);
    RETURN NEW;
END;
$$;

DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger AS t
        WHERE t.tgname = 'seed_legacy_gics_override_after_company_change'
          AND t.tgrelid = 'public.companies'::pg_catalog.regclass
          AND NOT t.tgisinternal
    ) THEN
        EXECUTE $trigger$
            CREATE TRIGGER seed_legacy_gics_override_after_company_change
                AFTER INSERT OR UPDATE ON public.companies
                FOR EACH ROW
                WHEN (NEW.ticker IN ('ASML', 'ARM', 'PDD', 'TRI', 'FER', 'CCEP'))
                EXECUTE FUNCTION public.seed_legacy_gics_override_after_company_change()
        $trigger$;
    END IF;
END;
$do$;

-- Backfill matching companies on upgrade.  On a fresh or schema-only database
-- this inserts zero rows, so the foreign key remains satisfied.
SELECT public.seed_legacy_gics_overrides();
