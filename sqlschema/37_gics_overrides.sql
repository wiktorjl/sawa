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

INSERT INTO gics_overrides (ticker, gics_sector, gics_industry, confidence, source, notes) VALUES
    ('ASML', 'Information Technology', 'Semiconductor Equipment',    'high', 'manual', 'Dutch ADR - semiconductor lithography'),
    ('ARM',  'Information Technology', 'Semiconductors',             'high', 'manual', 'UK ADR - semiconductor IP licensing'),
    ('PDD',  'Consumer Discretionary', 'Internet Retail',            'high', 'manual', 'Chinese ADR - e-commerce platform'),
    ('TRI',  'Industrials',            'Professional Services',      'high', 'manual', 'Canadian - financial data & legal information services'),
    ('FER',  'Industrials',            'Construction & Engineering', 'high', 'manual', 'Spanish - infrastructure and construction'),
    ('CCEP', 'Consumer Staples',       'Soft Drinks',                'high', 'manual', 'European Coca-Cola bottler')
ON CONFLICT (ticker) DO NOTHING;
