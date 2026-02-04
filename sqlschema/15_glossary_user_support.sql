-- ============================================
-- GLOSSARY USER-SPECIFIC DEFINITIONS SUPPORT
-- ============================================
-- Adds id and user_id columns to glossary_terms to support
-- per-user definition overrides and shared definitions

-- Step 1: Drop the old table and recreate with new schema
-- We need to change the primary key from (term) to (id)

-- First, backup any existing data
CREATE TABLE IF NOT EXISTS glossary_terms_backup AS SELECT * FROM glossary_terms;

-- Drop old table
DROP TABLE IF EXISTS glossary_terms CASCADE;

-- Create new table with id and user_id support
CREATE TABLE glossary_terms (
    id SERIAL PRIMARY KEY,
    term VARCHAR(100) NOT NULL,
    official_definition TEXT,
    plain_english TEXT,
    examples JSONB,              -- ["Example 1", "Example 2"]
    related_terms JSONB,         -- ["P/E Ratio", "EPS"]
    learn_more JSONB,            -- ["https://...", "https://..."]
    custom_prompt TEXT,          -- Regeneration instructions (if any)
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_used VARCHAR(50) DEFAULT 'glm-4.7',
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE
);

-- Unique constraint for user-specific definitions
CREATE UNIQUE INDEX idx_glossary_terms_term_user_unique
    ON glossary_terms(term, user_id) WHERE user_id IS NOT NULL;

-- Unique constraint for shared definitions (NULL user_id)
CREATE UNIQUE INDEX idx_glossary_terms_term_shared_unique
    ON glossary_terms(term) WHERE user_id IS NULL;

-- Restore backed up data (without user_id, they become shared definitions)
INSERT INTO glossary_terms (term, official_definition, plain_english, examples, related_terms, learn_more, custom_prompt, generated_at, model_used, user_id)
SELECT term, official_definition, plain_english, examples, related_terms, learn_more, custom_prompt, generated_at, model_used, NULL
FROM glossary_terms_backup
ON CONFLICT DO NOTHING;

-- Clean up backup
DROP TABLE IF EXISTS glossary_terms_backup;

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_glossary_terms_term ON glossary_terms(term);
CREATE INDEX IF NOT EXISTS idx_glossary_terms_user_id ON glossary_terms(user_id);
CREATE INDEX IF NOT EXISTS idx_glossary_terms_term_user ON glossary_terms(term, user_id);
