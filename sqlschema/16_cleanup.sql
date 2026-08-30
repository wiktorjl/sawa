-- ============================================
-- HISTORICAL CLEANUP MARKER: retired TUI/Web tables
-- ============================================
-- This migration formerly dropped data-bearing legacy tables. Schema files are
-- replayed by ``sawa coldstart --no-drop``, so destructive cleanup does not
-- belong in this path. Existing installations retain any legacy data; an
-- operator who explicitly wants it removed can do so in a separately reviewed,
-- backed-up maintenance operation.
DO $$
BEGIN
    RAISE NOTICE 'Preserving any retired TUI/Web tables (migration 16 is non-destructive)';
END $$;
