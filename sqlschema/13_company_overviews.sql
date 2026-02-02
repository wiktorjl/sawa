-- AI-generated company overviews cache
-- Stores structured analysis from LLM for quick retrieval

CREATE TABLE IF NOT EXISTS company_overviews (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,

    -- Structured analysis sections
    main_product TEXT,
    revenue_model TEXT,
    headwinds JSONB,           -- ["headwind 1", "headwind 2", ...]
    tailwinds JSONB,           -- ["tailwind 1", "tailwind 2", ...]
    sector_outlook TEXT,
    competitive_position TEXT,

    -- Metadata
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_used VARCHAR(50) DEFAULT 'glm-4.7',
    custom_prompt TEXT,        -- If regenerated with custom instructions

    -- Constraints: unique per ticker+user, with NULL user_id for shared entries
    CONSTRAINT uq_company_overview_ticker_user
        UNIQUE NULLS NOT DISTINCT (ticker, user_id)
);

-- Index for fast ticker lookups
CREATE INDEX IF NOT EXISTS idx_company_overviews_ticker
    ON company_overviews(ticker);

-- Index for user-specific lookups
CREATE INDEX IF NOT EXISTS idx_company_overviews_user_id
    ON company_overviews(user_id) WHERE user_id IS NOT NULL;

COMMENT ON TABLE company_overviews IS 'AI-generated company analysis cached for quick retrieval';
COMMENT ON COLUMN company_overviews.user_id IS 'NULL for shared entries, set for user-specific overrides';
COMMENT ON COLUMN company_overviews.headwinds IS 'JSON array of risk/challenge strings';
COMMENT ON COLUMN company_overviews.tailwinds IS 'JSON array of growth driver strings';
