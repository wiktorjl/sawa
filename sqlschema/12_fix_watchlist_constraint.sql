-- Fix watchlist constraint: should be UNIQUE(user_id, name), not UNIQUE(name)
-- This allows different users to have watchlists with the same name

-- Drop the old constraint
ALTER TABLE watchlists DROP CONSTRAINT IF EXISTS watchlists_name_key;

-- Create composite unique index
CREATE UNIQUE INDEX IF NOT EXISTS watchlists_user_id_name_unique 
    ON watchlists (user_id, name);
