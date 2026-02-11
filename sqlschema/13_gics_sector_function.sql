-- ============================================
-- GICS SECTOR LOOKUP FUNCTION
-- ============================================
-- Centralizes GICS sector classification logic that was previously
-- duplicated as CASE statements across multiple queries.
-- Handles ticker-specific overrides for foreign ADRs without
-- proper SIC codes, then falls back to the sic_gics_mapping table.
--
-- Marked IMMUTABLE for query plan caching since the mapping data
-- and ticker overrides are effectively static.

CREATE OR REPLACE FUNCTION get_gics_sector(
    p_ticker TEXT,
    p_sic_code TEXT,
    p_sic_desc TEXT DEFAULT 'Unclassified'
) RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_sector TEXT;
BEGIN
    -- Ticker-specific overrides for foreign ADRs without proper SIC codes
    v_sector := CASE p_ticker
        WHEN 'ASML' THEN 'Information Technology'
        WHEN 'ARM'  THEN 'Information Technology'
        WHEN 'PDD'  THEN 'Consumer Discretionary'
        WHEN 'TRI'  THEN 'Industrials'
        WHEN 'FER'  THEN 'Industrials'
        WHEN 'CCEP' THEN 'Consumer Staples'
        ELSE NULL
    END;

    IF v_sector IS NOT NULL THEN
        RETURN v_sector;
    END IF;

    -- Look up GICS sector from SIC-to-GICS mapping table
    SELECT m.gics_sector INTO v_sector
    FROM sic_gics_mapping m
    WHERE m.sic_code = p_sic_code;

    RETURN COALESCE(v_sector, p_sic_desc);
END;
$$;

COMMENT ON FUNCTION get_gics_sector(TEXT, TEXT, TEXT) IS
    'Returns GICS sector for a given ticker, with overrides for foreign ADRs '
    'and fallback to sic_gics_mapping table, then to the provided SIC description.';
