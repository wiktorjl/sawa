-- ============================================
-- CLEANUP: Remove TUI/Web specific tables
-- ============================================
-- Run this script to remove tables that were used by the TUI and Web interfaces
-- These tables are no longer needed after simplifying to CLI + MCP architecture

-- Drop TUI-specific tables (watchlists, users, settings)
DROP TABLE IF EXISTS watchlist_symbols CASCADE;
DROP TABLE IF EXISTS watchlists CASCADE;
DROP TABLE IF EXISTS user_settings CASCADE;
DROP TABLE IF EXISTS default_settings CASCADE;
DROP TABLE IF EXISTS active_user CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Drop glossary tables (AI-generated definitions for TUI)
DROP TABLE IF EXISTS glossary_terms CASCADE;
DROP TABLE IF EXISTS glossary_term_list CASCADE;

-- Drop company overviews table (AI-generated company analysis for TUI/Web)
DROP TABLE IF EXISTS company_overviews CASCADE;

-- Drop triggers and functions related to removed tables
DROP TRIGGER IF EXISTS watchlists_updated_at ON watchlists;
DROP TRIGGER IF EXISTS user_settings_updated_at ON user_settings;
DROP TRIGGER IF EXISTS prevent_active_user_deletion_trigger ON users;
DROP TRIGGER IF EXISTS prevent_last_admin_demotion_trigger ON users;

DROP FUNCTION IF EXISTS update_watchlist_timestamp();
DROP FUNCTION IF EXISTS prevent_active_user_deletion();
DROP FUNCTION IF EXISTS prevent_last_admin_demotion();
