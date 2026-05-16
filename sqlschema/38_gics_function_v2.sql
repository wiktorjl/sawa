-- Rewrite get_gics_sector() to consult the gics_overrides table.
--
-- Pre-migration: 13_gics_sector_function.sql defined a function with
-- a hard-coded CASE block of 6 ADR overrides (ASML, ARM, PDD, TRI,
-- FER, CCEP). With 348 active ADRs missing SIC, that list was wildly
-- under-sized.
--
-- This migration replaces the CASE block with a lookup against the
-- gics_overrides table (created by 37_gics_overrides.sql, seeded with
-- those same 6 ADRs so behavior is identical at landing time). After
-- this lands, scripts/backfill_gics_overrides.py extends coverage to
-- the long tail using yfinance — no further SQL edits required.
--
-- Lookup order:
--   1. gics_overrides(ticker)        — ticker-level overrides
--   2. sic_gics_mapping(sic_code)    — SIC → GICS seed data
--   3. p_sic_desc                    — fall back to SIC description
--                                       (or 'Unclassified' default)

CREATE OR REPLACE FUNCTION get_gics_sector(
    p_ticker TEXT,
    p_sic_code TEXT,
    p_sic_desc TEXT DEFAULT 'Unclassified'
) RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_sector TEXT;
BEGIN
    SELECT o.gics_sector INTO v_sector
    FROM gics_overrides o
    WHERE o.ticker = p_ticker;
    IF v_sector IS NOT NULL THEN
        RETURN v_sector;
    END IF;

    SELECT m.gics_sector INTO v_sector
    FROM sic_gics_mapping m
    WHERE m.sic_code = p_sic_code;

    RETURN COALESCE(v_sector, p_sic_desc);
END;
$$;

COMMENT ON FUNCTION get_gics_sector(TEXT, TEXT, TEXT) IS
    'Returns GICS sector for a given ticker. Consults the ticker-level '
    'gics_overrides table first, then sic_gics_mapping by SIC code, '
    'then falls back to the provided SIC description.';
