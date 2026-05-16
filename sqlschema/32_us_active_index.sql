-- Add the us_active extended index covering all currently-tradeable US
-- common stocks, ETFs, and ADRs across all major exchanges.
--
-- Source: Polygon /v3/reference/tickers?market=stocks&active=true
-- with type IN (CS, ETF, ADRC) and no exchange filter (so XNYS, ARCX,
-- BATS, XNAS are all picked up — this is the key gap left by
-- nasdaq_listed which is XNAS-only).
--
-- Approximate size: ~10,400 tickers
--   ~5,277 CS (any exchange minus XASE)
--   ~4,986 ETFs (any exchange — ARCX dominates with 2,401)
--   ~  370 ADRCs (XNAS + XNYS)
--
-- Refreshed by `sawa index-update`.

INSERT INTO indices (code, name, description, source_url) VALUES
    ('us_active',
     'US Active (CS + ETF + ADRC)',
     'All currently-active US-tradeable common stocks, ETFs, and ADRs '
     'across XNAS, XNYS, ARCX, and BATS (excluding XASE microcap noise).',
     'https://api.polygon.io/v3/reference/tickers')
ON CONFLICT (code) DO NOTHING;
