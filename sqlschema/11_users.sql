-- ============================================
-- USERS AND MULTI-USER SUPPORT
-- ============================================

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    is_admin BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Active user tracker (single row enforced)
CREATE TABLE IF NOT EXISTS active_user (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    user_id INTEGER REFERENCES users(id) ON DELETE RESTRICT
);

-- Default settings template (admin-managed, copied to new users)
CREATE TABLE IF NOT EXISTS default_settings (
    key VARCHAR(50) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default user (admin)
INSERT INTO users (name, is_admin) 
VALUES ('Default', TRUE) 
ON CONFLICT (name) DO NOTHING;

-- Set default user as active
INSERT INTO active_user (user_id) 
SELECT id FROM users WHERE name = 'Default'
ON CONFLICT (id) DO UPDATE SET user_id = (SELECT id FROM users WHERE name = 'Default');

-- Create default watchlist for default user (if doesn't exist)
INSERT INTO watchlists (user_id, name, is_default)
SELECT u.id, 'Default', TRUE
FROM users u
WHERE u.name = 'Default'
  AND NOT EXISTS (
      SELECT 1 FROM watchlists w WHERE w.user_id = u.id AND w.name = 'Default'
  );

-- Add sample stocks to default watchlist (if they exist in companies table)
INSERT INTO watchlist_symbols (watchlist_id, ticker, sort_order)
SELECT w.id, c.ticker, t.sort_order
FROM users u
JOIN watchlists w ON w.user_id = u.id AND w.name = 'Default'
CROSS JOIN (
    VALUES ('AAPL', 1), ('GOOGL', 2), ('AMZN', 3)
) AS t(ticker, sort_order)
JOIN companies c ON c.ticker = t.ticker
WHERE u.name = 'Default'
  AND NOT EXISTS (
      SELECT 1 FROM watchlist_symbols ws WHERE ws.watchlist_id = w.id
  )
ON CONFLICT (watchlist_id, ticker) DO NOTHING;

-- Copy default_settings to user_settings for default user
INSERT INTO user_settings (user_id, key, value)
SELECT u.id, ds.key, ds.value
FROM users u
CROSS JOIN default_settings ds
WHERE u.name = 'Default'
ON CONFLICT (user_id, key) DO NOTHING;

-- Insert default settings template
INSERT INTO default_settings (key, value) VALUES
    ('zai_api_key', ''),
    ('chart_period_days', '60'),
    ('number_format', 'compact'),
    ('fundamentals_timeframe', 'quarterly'),
    ('theme', 'osaka-jade'),
    ('chart_detail', 'normal')
ON CONFLICT (key) DO NOTHING;

-- ============================================
-- MIGRATIONS FOR EXISTING TABLES
-- ============================================

-- Add user_id to watchlists (with migration)
DO $$
BEGIN
    -- Add column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'watchlists' AND column_name = 'user_id') THEN
        -- Drop the old UNIQUE constraint on name
        ALTER TABLE watchlists DROP CONSTRAINT IF EXISTS watchlists_name_key;
        
        -- Add user_id column
        ALTER TABLE watchlists ADD COLUMN user_id INTEGER REFERENCES users(id) ON DELETE CASCADE;
        
        -- Assign existing watchlists to default user
        UPDATE watchlists SET user_id = (SELECT id FROM users WHERE name = 'Default')
        WHERE user_id IS NULL;
        
        -- Make user_id NOT NULL after migration
        ALTER TABLE watchlists ALTER COLUMN user_id SET NOT NULL;
        
        -- Create composite unique constraint (user_id, name)
        CREATE UNIQUE INDEX IF NOT EXISTS watchlists_user_id_name_unique 
            ON watchlists (user_id, name);
    END IF;
END $$;

-- Migrate user_settings table to have user_id
DO $$
BEGIN
    -- Check if we need to migrate
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'user_settings' AND column_name = 'user_id') THEN
        
        -- Create new table structure
        CREATE TABLE user_settings_new (
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            key VARCHAR(50) NOT NULL,
            value TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, key)
        );
        
        -- Migrate existing data to default user
        INSERT INTO user_settings_new (user_id, key, value, updated_at)
        SELECT (SELECT id FROM users WHERE name = 'Default'), key, value, updated_at
        FROM user_settings
        ON CONFLICT DO NOTHING;
        
        -- Drop old table and rename
        DROP TABLE user_settings;
        ALTER TABLE user_settings_new RENAME TO user_settings;
    END IF;
END $$;

-- Add user_id to glossary_terms (NULL = shared, NOT NULL = user override)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'glossary_terms' AND column_name = 'user_id') THEN
        -- Drop existing primary key constraint
        ALTER TABLE glossary_terms DROP CONSTRAINT IF EXISTS glossary_terms_pkey;
        
        -- Add user_id column
        ALTER TABLE glossary_terms ADD COLUMN user_id INTEGER REFERENCES users(id) ON DELETE CASCADE;
        
        -- Add ID column as new primary key
        ALTER TABLE glossary_terms ADD COLUMN id SERIAL PRIMARY KEY;
        
        -- Existing definitions become shared (user_id = NULL)
        -- No update needed as NULL is the default
        
        -- Create unique constraint for (term, user_id) with NULL handling
        -- Partial unique index for shared definitions (user_id IS NULL)
        CREATE UNIQUE INDEX IF NOT EXISTS glossary_terms_shared_unique 
            ON glossary_terms (term) WHERE user_id IS NULL;
        
        -- Unique index for user-specific overrides (user_id IS NOT NULL)
        CREATE UNIQUE INDEX IF NOT EXISTS glossary_terms_user_unique 
            ON glossary_terms (term, user_id) WHERE user_id IS NOT NULL;
    END IF;
END $$;

-- Create indexes for user_id columns
CREATE INDEX IF NOT EXISTS idx_watchlists_user_id ON watchlists(user_id);
CREATE INDEX IF NOT EXISTS idx_user_settings_user_id ON user_settings(user_id);
CREATE INDEX IF NOT EXISTS idx_glossary_terms_user_id ON glossary_terms(user_id);

-- ============================================
-- TRIGGERS
-- ============================================

-- Prevent deletion of active user
CREATE OR REPLACE FUNCTION prevent_active_user_deletion()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id = (SELECT user_id FROM active_user WHERE id = 1) THEN
        RAISE EXCEPTION 'Cannot delete the currently active user';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS prevent_active_user_deletion_trigger ON users;
CREATE TRIGGER prevent_active_user_deletion_trigger
    BEFORE DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION prevent_active_user_deletion();

-- Prevent demotion of last admin
CREATE OR REPLACE FUNCTION prevent_last_admin_demotion()
RETURNS TRIGGER AS $$
DECLARE
    admin_count INTEGER;
BEGIN
    -- Only check if changing from admin to non-admin
    IF OLD.is_admin = TRUE AND NEW.is_admin = FALSE THEN
        SELECT COUNT(*) INTO admin_count FROM users WHERE is_admin = TRUE;
        IF admin_count = 1 THEN
            RAISE EXCEPTION 'Cannot demote the last admin user';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS prevent_last_admin_demotion_trigger ON users;
CREATE TRIGGER prevent_last_admin_demotion_trigger
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION prevent_last_admin_demotion();
