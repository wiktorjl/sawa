-- Additional index definitions: nasdaq100, dow30, mag7.
--
-- All three reuse the existing indices / index_constituents schema
-- (12_indices.sql). Constituents are populated by `sawa index-update`
-- (which calls coldstart.populate_index_constituents → fetchers in
-- sawa/utils/symbols.py).
--
-- Sources by index:
--   nasdaq100    Wikipedia "Nasdaq-100" article, #constituents table
--   dow30        Wikipedia "Dow Jones Industrial Average", #constituents
--   mag7         Hard-coded constant: AAPL, MSFT, GOOGL, GOOG, AMZN,
--                NVDA, META, TSLA. The "Magnificent Seven" is not an
--                official index — the constituents are stable enough
--                that a literal list is correct.
--
-- Russell 1000 / 2000 were originally planned for this migration but
-- the iShares IWB/IWM holdings CSV endpoints are gated behind a JS
-- consent page that programmatic clients can't bypass. Coverage is
-- deferred until a workable source is chosen (Vanguard VONE/VTWO
-- holdings or a bundled snapshot).

INSERT INTO indices (code, name, description, source_url) VALUES
    ('nasdaq100',
     'NASDAQ-100',
     '100 largest non-financial companies on NASDAQ (= QQQ)',
     'https://en.wikipedia.org/wiki/Nasdaq-100'),
    ('dow30',
     'Dow Jones Industrial Average',
     '30 large-cap US stocks tracked by the Dow Jones Industrial Average',
     'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'),
    ('mag7',
     'Magnificent 7',
     'AAPL, MSFT, GOOGL, GOOG, AMZN, NVDA, META, TSLA (informal cohort)',
     NULL)
ON CONFLICT (code) DO NOTHING;
