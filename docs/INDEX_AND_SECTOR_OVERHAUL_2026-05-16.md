# Index + sector classification overhaul — 2026-05-16

Note for anyone consuming this database (via SQL, MCP tools, or the
`sawa` Python API). Three things changed in ways you might care about:
**(1) one index code was renamed**, **(2) four new indices were added**,
and **(3) GICS sector classification now works for foreign ADRs** that
previously fell through to SIC descriptions.

If you only have time for one paragraph: the index code `nasdaq5000`
is gone — it is now `nasdaq_listed`. Replace it everywhere. The view
column `in_nasdaq5000` was renamed to `in_nasdaq_listed`. Everything
else is additive.

---

## Breaking changes (action required)

### 1. `nasdaq5000` → `nasdaq_listed`

```sql
-- Before
SELECT … FROM index_constituents ic JOIN indices i ON …
 WHERE i.code = 'nasdaq5000';

-- After
SELECT … FROM index_constituents ic JOIN indices i ON …
 WHERE i.code = 'nasdaq_listed';
```

The index used to be called `nasdaq5000`. It was a misnomer: the index
contains every active NASDAQ-listed ticker (CS + ETF + ADRC), not 5,000
of anything and not the NASDAQ Composite. Today it has 4,676
constituents. The new code matches what the data actually represents.

There is **no alias** — the old code returns "Unknown index" from
`fetch_index_symbols()` and zero rows from any join. MCP tools that
take an `index` parameter now reject the old value as well.

### 2. View column rename: `in_nasdaq5000` → `in_nasdaq_listed`

The `v_company_with_indices` view exposes per-index boolean flags. The
column previously named `in_nasdaq5000` is now `in_nasdaq_listed`. If
your queries select that column by name, update them.

```sql
-- Before
SELECT ticker, in_sp500, in_nasdaq5000 FROM v_company_with_indices;

-- After
SELECT ticker, in_sp500, in_nasdaq_listed FROM v_company_with_indices;
```

---

## Additive changes

### New indices in the `indices` table

| Code           | Name                          | Constituents | Source                |
|----------------|-------------------------------|-------------:|-----------------------|
| `nasdaq100`    | NASDAQ-100                    | 101          | Wikipedia             |
| `dow30`        | Dow Jones Industrial Average  | 30           | Wikipedia             |
| `russell1000`  | Russell 1000                  | 998          | Wikipedia             |
| `mag7`         | Magnificent 7                 | 8            | Hard-coded list       |

All five reuse the existing `(indices, index_constituents)` schema and
work with the same JOIN pattern as `sp500` / `nasdaq_listed` /
`us_active`. Multi-index membership is intentional — `AAPL` is now in
seven indices (sp500, nasdaq_listed, us_active, nasdaq100, dow30,
russell1000, mag7).

```sql
-- Membership for a single ticker
SELECT i.code FROM index_constituents ic
JOIN indices i ON i.id = ic.index_id
WHERE ic.ticker = 'AAPL'
ORDER BY i.code;
```

**Russell 2000 is intentionally NOT included.** iShares (IWM holdings
CSV), Vanguard (VTWO holdings), and Wikipedia all proved unworkable
for programmatic access. A future PR will likely ship a bundled
snapshot. Until then, querying `russell2000` returns the same
"Unknown index" error as any unknown code.

### `us_active` now includes ETV (commodity pools / grantor trusts)

The full set of types in `us_active` is now CS + ETF + ADRC + **ETV**.
ETV covers commodity pools and grantor trusts like USO, UNG, GBTC,
DBA, DBC, UCO, SCO, UGA, BOIL, KOLD, PALL, PPLT, UUP — the standard
proxies for dollar / oil / nat-gas / agriculture / broad-commodity /
crypto / precious-metals exposure. These were silently excluded before
this change.

The index `name` and `description` columns reflect this:

```
name:  US Active (CS + ETF + ADRC + ETV)
```

### CBOE is now in `us_active`

`CBOE` (Cboe Global Markets) is the lone NYSE/BATS-listed common stock
on the BATS exchange. The previous fetcher excluded all BATS-CS as
"singleton noise"; that was a misclassification (CBOE is an S&P 500
member). It is now in `us_active`. Membership check:

```sql
SELECT code FROM indices i
JOIN index_constituents ic ON ic.index_id = i.id
WHERE ic.ticker = 'CBOE';
-- → sp500, russell1000, us_active
```

### New companies in the `companies` table

Thirteen macro/commodity ETFs (technically ETVs) were added so they can
be screened, charted, and queried like any other ticker:

```
UUP   Invesco DB US Dollar Index Bullish Fund
USO   United States Oil Fund
UNG   United States Natural Gas Fund
UCO   ProShares 2x crude oil bull
SCO   ProShares 2x crude oil bear
UGA   United States Gasoline Fund
BOIL  ProShares 2x natural gas bull
KOLD  ProShares 2x natural gas bear
DBA   Invesco DB Agriculture Fund
DBC   Invesco DB Commodity Index
GBTC  Grayscale Bitcoin Trust ETF
PALL  abrdn Physical Palladium Shares
PPLT  abrdn Physical Platinum Shares
```

Each has 5 years of daily price history loaded.

### GICS sector classification now works for foreign ADRs

If you call `get_gics_sector(ticker, sic_code, sic_description)` — or
read `gics_sector` columns from MCP tools like `get_sector_performance`,
`screen_stocks`, `list_companies` — you will now get a real GICS sector
for approximately 350 foreign ADRs (BABA, TSM, SAP, ASML, ARM, NVO,
PDD, TM, HMC, SONY, BP, BBVA, SAN, BTI, UL, RELX, …) that previously
returned their SIC description (or a "(none)" placeholder) because
Polygon doesn't carry SIC for non-US issuers.

The function lookup order:

```
1. gics_overrides(ticker)  — ticker-level override (manual + yfinance)
2. sic_gics_mapping(sic_code) — SIC → GICS lookup
3. p_sic_desc — fall back to whatever was passed in
```

New table `gics_overrides(ticker PK, gics_sector, gics_industry,
confidence, source, notes, updated_at)`. Source values are `manual` (6
legacy entries from a hard-coded CASE block) and `yfinance` (~345
entries populated by `scripts/backfill_gics_overrides.py`).

### `sic_gics_mapping` extended

The seed mapping table grew from 210 rows to 380. The added 170 rows
cover every SIC code that appears in `companies` but was previously
unmapped (the biggest bucket: SIC `6770 BLANK CHECKS`, 224 SPACs, now
classified as `Financials / Capital Markets` with `confidence='low'`).

Net effect: there are now **zero** SICs in `companies` that don't have
a GICS mapping. The function's fallback to SIC description should
almost never fire.

### MCP tool enums expanded

Every tool that takes an `index` parameter (`list_companies`,
`search_companies`, `get_top_movers`, `screen_stocks`,
`detect_crossovers`, `get_dividend_yield_leaders`, etc.) now accepts:

```
sp500, nasdaq_listed, us_active, nasdaq100, dow30, russell1000, mag7
```

The previous accepted values were just `sp500` and `nasdaq5000`. Old
clients passing `nasdaq5000` will get an enum-validation error.

---

## Known gaps (not breaking anything, but worth knowing)

- **857 active common stocks still lack a `sic_code`.** Mostly foreign
  cross-listings on NYSE (AZN, RY, TTE, TD, UBS, SPOT, …) where Polygon
  doesn't classify non-US issuers. Their `get_gics_sector()` falls back
  to the SIC description, which is `NULL` for these — meaning the MCP
  sector tools return them under an "Unclassified" bucket. The same
  yfinance backfill that fixed the 345 ADRs can fix these too; it just
  hasn't been run yet (estimated ~3-4 hours due to throttling).

- **Russell 2000** is unavailable. See above.

- The `companies.sic_description` for ETVs (the 13 macro ETFs above) is
  `COMMODITY CONTRACTS BROKERS & DEALERS`, which maps to GICS
  `Financials / Capital Markets`. Technically correct per SIC, but if
  you're filtering for "Financials" you'll catch USO, UNG, GBTC, etc.
  alongside actual financial-services companies. Filtering by `c.type`
  in addition to sector is the workaround.

---

## Verification queries

If you want to sanity-check your local copy after pulling these
changes:

```sql
-- 1. Indices count + composition
SELECT code, name, constituent_count FROM (
  SELECT i.code, i.name, COUNT(ic.ticker) AS constituent_count
  FROM indices i LEFT JOIN index_constituents ic ON ic.index_id = i.id
  GROUP BY i.code, i.name
) t ORDER BY constituent_count DESC;
-- Expect 7 rows: us_active~10406, nasdaq_listed~4676, russell1000~998,
-- sp500~503, nasdaq100~101, dow30~30, mag7=8

-- 2. CBOE in us_active
SELECT i.code FROM indices i
JOIN index_constituents ic ON ic.index_id = i.id
WHERE ic.ticker = 'CBOE' ORDER BY i.code;
-- Expect: russell1000, sp500, us_active

-- 3. ADR GICS coverage
SELECT COUNT(*) FROM companies c
WHERE c.active=true AND c.type='ADRC'
  AND get_gics_sector(c.ticker, c.sic_code, c.sic_description) IN (
    'Energy','Materials','Industrials','Consumer Discretionary',
    'Consumer Staples','Health Care','Financials',
    'Information Technology','Communication Services',
    'Utilities','Real Estate');
-- Expect 375 (100% of active ADRs)

-- 4. gics_overrides table exists and has yfinance + manual rows
SELECT source, COUNT(*) FROM gics_overrides GROUP BY source;
-- Expect: manual=6, yfinance=~345
```

---

## File-level reference

| Change                              | File(s)                                                                                  |
|-------------------------------------|------------------------------------------------------------------------------------------|
| `nasdaq5000` rename                 | `sqlschema/34_rename_nasdaq5000.sql`, `sqlschema/12_indices.sql`, `sqlschema/22_views_advanced.sql` |
| Four new indices                    | `sqlschema/35_additional_indices.sql`, `sqlschema/39_russell1000_index.sql`             |
| ETV inclusion in `us_active`        | `sqlschema/32_us_active_index.sql` (description), `sawa/utils/symbols.py`                |
| `sic_gics_mapping` extension        | `sqlschema/36_sic_gics_data_extension.sql`                                               |
| `gics_overrides` table              | `sqlschema/37_gics_overrides.sql`                                                        |
| `get_gics_sector()` rewrite         | `sqlschema/38_gics_function_v2.sql`                                                      |
| BATS-CS fix (CBOE)                  | `sawa/utils/symbols.py`                                                                  |
| yfinance ADR backfill               | `scripts/backfill_gics_overrides.py`                                                     |
| Polygon dot → Yahoo dash mapping    | `scripts/backfill_gics_overrides.py`                                                     |
