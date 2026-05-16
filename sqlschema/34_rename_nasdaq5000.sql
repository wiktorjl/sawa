-- Rename the legacy 'nasdaq5000' index code/name to 'nasdaq_listed'.
--
-- The original code was a misnomer: the index is not "5000 of something"
-- and is not the NASDAQ Composite. It is "all currently-active
-- NASDAQ-listed tickers" — CS + ETF + ADRC on XNAS — and contains
-- ~4,677 rows. The new code matches what the data actually is.
--
-- The 22_views_advanced.sql view was also updated to reference the new
-- code and to rename the alias 'in_nasdaq5000' → 'in_nasdaq_listed'.
-- Any consumer code reading from that view must update accordingly.

UPDATE indices
   SET code = 'nasdaq_listed',
       name = 'NASDAQ Listed',
       description = 'All currently-active NASDAQ-listed tickers (CS + ETF + ADRC)'
 WHERE code = 'nasdaq5000';

-- Rebuild the view so the renamed column alias takes effect on
-- already-deployed databases. CREATE OR REPLACE rejects column renames,
-- so DROP first. CASCADE handles any downstream dependent objects (none
-- exist today but is safe if added later).
DROP VIEW IF EXISTS v_company_with_indices CASCADE;

CREATE VIEW v_company_with_indices AS
SELECT
    c.ticker,
    c.name,
    c.market_cap,
    c.sic_description as sector,
    c.primary_exchange as exchange,
    c.active,
    COALESCE(
        (SELECT array_agg(i.code ORDER BY i.name)
         FROM index_constituents ic
         JOIN indices i ON ic.index_id = i.id
         WHERE ic.ticker = c.ticker),
        ARRAY[]::varchar[]
    ) as indices,
    (EXISTS (
        SELECT 1 FROM index_constituents ic
        JOIN indices i ON ic.index_id = i.id
        WHERE ic.ticker = c.ticker AND i.code = 'sp500'
    )) as in_sp500,
    (EXISTS (
        SELECT 1 FROM index_constituents ic
        JOIN indices i ON ic.index_id = i.id
        WHERE ic.ticker = c.ticker AND i.code = 'nasdaq_listed'
    )) as in_nasdaq_listed
FROM companies c;
