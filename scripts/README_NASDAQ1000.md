# Adding NASDAQ-1000 Index Constituents

This guide explains how to add 1000 NASDAQ tickers to the database and label them as nasdaq1000 index constituents.

## Prerequisites

- List of 1000 ticker symbols in a text file (one per line)
- DATABASE_URL environment variable set
- Polygon API key (for downloading company data)

## Quick Start

```bash
# Assuming you have nasdaq1000_symbols.txt with your 1000 tickers

# Step 1: Create the nasdaq1000 index in the database
psql $DATABASE_URL -f scripts/add_nasdaq1000_index.sql

# Step 2: Add all symbols to the companies table (downloads company info & price history)
sawa add-symbol --file nasdaq1000_symbols.txt --years 5

# Step 3: Link symbols to the nasdaq1000 index
python scripts/populate_nasdaq1000.py nasdaq1000_symbols.txt
```

## Detailed Steps

### Step 1: Create the Index

This adds a row to the `indices` table:

```bash
psql $DATABASE_URL -f scripts/add_nasdaq1000_index.sql
```

Verify:
```sql
SELECT * FROM indices WHERE code = 'nasdaq1000';
```

### Step 2: Add Symbols to Database

This will:
- Download company information for each ticker
- Download 5 years of price history
- Download fundamentals and ratios
- Insert everything into the database

```bash
sawa add-symbol --file nasdaq1000_symbols.txt --years 5
```

**Note:** This will take some time (API rate limits). For 1000 symbols:
- ~1-2 hours for company info
- ~2-3 hours for price history
- Consider running with `--verbose` to monitor progress

### Step 3: Link Symbols to Index

This adds entries to the `index_constituents` table:

```bash
python scripts/populate_nasdaq1000.py nasdaq1000_symbols.txt
```

Output will show:
- How many symbols were added to the index
- Which symbols were skipped (if any aren't in the companies table yet)

## Verification

Check the results:

```sql
-- Count constituents
SELECT COUNT(*) FROM index_constituents
WHERE index_id = (SELECT id FROM indices WHERE code = 'nasdaq1000');

-- View constituents with company names
SELECT c.ticker, c.name, c.market_cap
FROM index_constituents ic
JOIN companies c ON ic.ticker = c.ticker
WHERE ic.index_id = (SELECT id FROM indices WHERE code = 'nasdaq1000')
ORDER BY c.market_cap DESC
LIMIT 20;

-- Check which indices a company belongs to
SELECT i.code, i.name
FROM index_constituents ic
JOIN indices i ON ic.index_id = i.id
WHERE ic.ticker = 'AAPL';
```

## File Format

Your `nasdaq1000_symbols.txt` should have one ticker per line:

```
AAPL
MSFT
GOOGL
AMZN
...
```

- Blank lines are ignored
- Whitespace is trimmed
- Case-insensitive (will be converted to uppercase)

## Troubleshooting

### Some symbols were skipped

If Step 3 reports skipped symbols, they're not in the `companies` table yet.
Re-run Step 2 for just those symbols:

```bash
# Create a file with only the missing symbols
python scripts/populate_nasdaq1000.py nasdaq1000_symbols.txt | grep "Skipped" | awk '{print $2}' > missing.txt

# Add them
sawa add-symbol --file missing.txt --years 5

# Re-link
python scripts/populate_nasdaq1000.py nasdaq1000_symbols.txt
```

### API rate limits

The `add-symbol` command respects Polygon API rate limits (5 calls/second).
If you hit limits:
- Add `--verbose` to see progress
- The command is idempotent - safe to re-run
- Consider breaking into smaller batches

### Memory issues

For 1000 symbols, this shouldn't be an issue, but if you encounter problems:
- Split the file into batches of 250 symbols
- Run each batch separately
- The final populate script will link all of them at once

## Alternative: Manual SQL Approach

If you prefer pure SQL:

```sql
-- 1. Create index
INSERT INTO indices (code, name, description) VALUES
('nasdaq1000', 'NASDAQ-1000', 'NASDAQ-1000 Index');

-- 2. Add constituents (assuming symbols already in companies table)
INSERT INTO index_constituents (index_id, ticker)
SELECT
    (SELECT id FROM indices WHERE code = 'nasdaq1000'),
    ticker
FROM companies
WHERE ticker IN ('AAPL', 'MSFT', 'GOOGL', ...) -- list all 1000
ON CONFLICT DO NOTHING;
```
