-- Add the russell1000 index row.
--
-- Russell 1000 was originally planned for migration 35 alongside
-- nasdaq100/dow30/mag7 but had to be deferred because the originally
-- chosen source (iShares IWB holdings CSV) gates programmatic access
-- behind a JS consent page. A second source probe against Vanguard
-- VONE also failed (React SPA fetches behind a session).
--
-- This migration adds the row using Wikipedia as the source — the
-- Russell 1000 article maintains a full ~1,003-row constituents
-- table that the existing _fetch_wikipedia_constituents helper
-- handles. Russell only reconstitutes annually in late June, so the
-- Wikipedia list stays current with low maintenance.
--
-- Russell 2000 is still deferred: Wikipedia only has a 12-row
-- "notable constituents" snippet (vs. the full 1,003-row table for
-- Russell 1000), so a different source is needed (bundled snapshot
-- CSV, SEC EDGAR N-PORT, or a third-party feed).

INSERT INTO indices (code, name, description, source_url) VALUES
    ('russell1000',
     'Russell 1000',
     '~1000 largest US stocks by market cap',
     'https://en.wikipedia.org/wiki/Russell_1000_Index')
ON CONFLICT (code) DO NOTHING;
