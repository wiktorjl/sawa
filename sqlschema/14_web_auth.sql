-- ============================================
-- WEB AUTHENTICATION SUPPORT
-- ============================================
-- Adds password_hash column to users table for web login
-- TUI users without password can continue using TUI
-- Web requires password (set on first login)

-- Add password_hash column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'users' AND column_name = 'password_hash') THEN
        ALTER TABLE users ADD COLUMN password_hash VARCHAR(255);
        RAISE NOTICE 'Added password_hash column to users table';
    END IF;
END $$;

-- Create index for login queries (name + password_hash lookup)
CREATE INDEX IF NOT EXISTS idx_users_name_lower ON users(LOWER(name));

-- Note: password_hash is nullable
-- - NULL: TUI-only user, cannot log in to web
-- - Set: Can log in to both TUI and web
-- Web login sets password on first login attempt for existing users
