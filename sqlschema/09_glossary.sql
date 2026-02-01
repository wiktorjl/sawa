-- ============================================
-- GLOSSARY TABLES
-- ============================================

-- Cached AI-generated glossary definitions
CREATE TABLE IF NOT EXISTS glossary_terms (
    term VARCHAR(100) PRIMARY KEY,
    official_definition TEXT,
    plain_english TEXT,
    examples JSONB,              -- ["Example 1", "Example 2"]
    related_terms JSONB,         -- ["P/E Ratio", "EPS"]
    learn_more JSONB,            -- ["https://...", "https://..."]
    custom_prompt TEXT,          -- Regeneration instructions (if any)
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_used VARCHAR(50) DEFAULT 'glm-4.7'
);

-- Predefined terms list (definitions generated on demand)
CREATE TABLE IF NOT EXISTS glossary_term_list (
    term VARCHAR(100) PRIMARY KEY,
    category VARCHAR(50),        -- "Valuation", "Profitability", etc.
    source VARCHAR(20) DEFAULT 'curated'  -- 'curated', 'user', 'extracted'
);

-- Index for category filtering
CREATE INDEX IF NOT EXISTS idx_glossary_term_list_category 
    ON glossary_term_list(category);

-- Insert curated financial terms
INSERT INTO glossary_term_list (term, category, source) VALUES
    -- Valuation (10)
    ('P/E Ratio', 'Valuation', 'curated'),
    ('P/B Ratio', 'Valuation', 'curated'),
    ('P/S Ratio', 'Valuation', 'curated'),
    ('PEG Ratio', 'Valuation', 'curated'),
    ('EV/EBITDA', 'Valuation', 'curated'),
    ('EV/Sales', 'Valuation', 'curated'),
    ('Market Cap', 'Valuation', 'curated'),
    ('Enterprise Value', 'Valuation', 'curated'),
    ('Price Target', 'Valuation', 'curated'),
    ('Fair Value', 'Valuation', 'curated'),
    
    -- Profitability (8)
    ('ROE', 'Profitability', 'curated'),
    ('ROA', 'Profitability', 'curated'),
    ('ROIC', 'Profitability', 'curated'),
    ('Gross Margin', 'Profitability', 'curated'),
    ('Operating Margin', 'Profitability', 'curated'),
    ('Net Margin', 'Profitability', 'curated'),
    ('EPS', 'Profitability', 'curated'),
    ('EBITDA', 'Profitability', 'curated'),
    
    -- Liquidity (4)
    ('Current Ratio', 'Liquidity', 'curated'),
    ('Quick Ratio', 'Liquidity', 'curated'),
    ('Cash Ratio', 'Liquidity', 'curated'),
    ('Working Capital', 'Liquidity', 'curated'),
    
    -- Leverage (5)
    ('Debt/Equity', 'Leverage', 'curated'),
    ('Debt/Assets', 'Leverage', 'curated'),
    ('Interest Coverage', 'Leverage', 'curated'),
    ('Total Debt', 'Leverage', 'curated'),
    ('Leverage Ratio', 'Leverage', 'curated'),
    
    -- Cash Flow (5)
    ('Free Cash Flow', 'Cash Flow', 'curated'),
    ('Operating Cash Flow', 'Cash Flow', 'curated'),
    ('CapEx', 'Cash Flow', 'curated'),
    ('FCF Yield', 'Cash Flow', 'curated'),
    ('Cash Conversion', 'Cash Flow', 'curated'),
    
    -- Dividends (4)
    ('Dividend Yield', 'Dividends', 'curated'),
    ('Payout Ratio', 'Dividends', 'curated'),
    ('Dividend Growth', 'Dividends', 'curated'),
    ('Ex-Dividend Date', 'Dividends', 'curated'),
    
    -- Growth (6)
    ('Revenue Growth', 'Growth', 'curated'),
    ('Earnings Growth', 'Growth', 'curated'),
    ('YoY', 'Growth', 'curated'),
    ('QoQ', 'Growth', 'curated'),
    ('CAGR', 'Growth', 'curated'),
    ('Organic Growth', 'Growth', 'curated'),
    
    -- Trading (8)
    ('52-Week High', 'Trading', 'curated'),
    ('52-Week Low', 'Trading', 'curated'),
    ('Volume', 'Trading', 'curated'),
    ('Beta', 'Trading', 'curated'),
    ('Alpha', 'Trading', 'curated'),
    ('Volatility', 'Trading', 'curated'),
    ('Short Interest', 'Trading', 'curated'),
    ('Float', 'Trading', 'curated')
ON CONFLICT (term) DO NOTHING;
