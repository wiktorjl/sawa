-- ============================================
-- SIC TO GICS SECTOR MAPPING TABLE
-- ============================================
-- Maps SEC SIC codes to GICS classification
-- Based on Yahoo Finance sector classifications and SEC SIC code descriptions

CREATE TABLE IF NOT EXISTS sic_gics_mapping (
    sic_code VARCHAR(4) PRIMARY KEY,
    gics_sector VARCHAR(50) NOT NULL,
    gics_industry VARCHAR(100) NOT NULL,
    confidence VARCHAR(10) NOT NULL CHECK (confidence IN ('high', 'medium', 'low')),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trigger to auto-update updated_at timestamp
CREATE TRIGGER update_sic_gics_mapping_updated_at 
    BEFORE UPDATE ON sic_gics_mapping 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Index for fast sector lookups
CREATE INDEX IF NOT EXISTS idx_sic_gics_sector ON sic_gics_mapping(gics_sector);

-- Comment on table
COMMENT ON TABLE sic_gics_mapping IS 'Maps SEC SIC codes (4-digit) to GICS classification (11 sectors)';
COMMENT ON COLUMN sic_gics_mapping.sic_code IS 'SEC SIC code (4-digit)';
COMMENT ON COLUMN sic_gics_mapping.gics_sector IS 'GICS sector: Energy, Materials, Industrials, Consumer Discretionary, Consumer Staples, Health Care, Financials, Information Technology, Communication Services, Utilities, Real Estate';
COMMENT ON COLUMN sic_gics_mapping.gics_industry IS 'GICS industry sub-classification';
COMMENT ON COLUMN sic_gics_mapping.confidence IS 'Mapping confidence: high (validated), medium (inferred), low (ambiguous)';
COMMENT ON COLUMN sic_gics_mapping.notes IS 'Notes about the mapping, sample tickers, ambiguities';
