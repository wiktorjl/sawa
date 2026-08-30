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

DO $migration$
DECLARE
    legacy_id INTEGER;
    canonical_id INTEGER;
BEGIN
    SELECT id INTO legacy_id
    FROM indices
    WHERE code = 'nasdaq5000';

    IF legacy_id IS NOT NULL THEN
        SELECT id INTO canonical_id
        FROM indices
        WHERE code = 'nasdaq_listed';

        IF canonical_id IS NULL THEN
            UPDATE indices
               SET code = 'nasdaq_listed',
                   name = 'NASDAQ Listed',
                   description =
                       'All currently-active NASDAQ-listed tickers (CS + ETF + ADRC)'
             WHERE id = legacy_id;
        ELSE
            -- Recover a partially applied/non-atomic legacy upgrade without
            -- deleting either row or any constituent. Copy memberships into
            -- the canonical index and retain the old row under an archival,
            -- non-routable code.
            INSERT INTO index_constituents (index_id, ticker, added_at)
            SELECT canonical_id, ticker, added_at
            FROM index_constituents
            WHERE index_id = legacy_id
            ON CONFLICT (index_id, ticker) DO NOTHING;

            UPDATE indices
               SET name = 'NASDAQ Listed',
                   description =
                       'All currently-active NASDAQ-listed tickers (CS + ETF + ADRC)'
             WHERE id = canonical_id;

            UPDATE indices
               SET code = ('nasdaq_legacy_' || id)::VARCHAR(20),
                   name = name || ' (legacy preserved)'
             WHERE id = legacy_id;
        END IF;
    END IF;
END
$migration$;

-- Rebuild the view so the renamed column alias takes effect on
-- already-deployed databases. CREATE OR REPLACE rejects column renames,
-- so DROP first. Deliberately omit CASCADE: if an operator has added a
-- dependent object, the atomic upgrade must stop and roll back instead of
-- silently deleting that extension.
DROP VIEW IF EXISTS v_company_with_indices;

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
