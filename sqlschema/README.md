# SQL Schema

PostgreSQL schema files for Sawa, executed in numeric prefix order. The
loader globs `NN_*.sql` and sorts; gaps in numbering are harmless.

## Files

| File | Purpose |
|------|---------|
| `00_setup.sql` | Documentation + verification queries (does not create anything) |
| `01_companies.sql` | `companies` (ticker is PK; central reference) |
| `02_market_data.sql` | `stock_prices`, `financial_ratios` |
| `03_fundamentals.sql` | `balance_sheets`, `income_statements`, `cash_flows` |
| `04_economy.sql` | `treasury_yields`, `inflation`, `inflation_expectations`, `labor_market` |
| `05_indexes.sql` | Performance indexes for the above tables |
| `06_views.sql` | Read-only views: `v_company_summary`, `v_economy_dashboard`, `v_latest_fundamentals`, `v_sector_summary`, dashboard market internals view |
| `07_procedures.sql` | PL/pgSQL helpers for loading |
| `08_sic_gics_mapping.sql` | `sic_gics_mapping` table (SIC → GICS) |
| `09_sic_gics_data.sql` | Seed data for `sic_gics_mapping` |
| `10_news.sql` | `news_articles`, `news_article_tickers`, `news_sentiment` |
| `11_technical_indicators.sql` | `technical_indicators`, `technical_indicator_metadata` |
| `12_indices.sql` | `indices`, `index_constituents`, seeds (`sp500`, `nasdaq5000`) |
| `13_gics_sector_function.sql` | `get_gics_sector(sic_code)` helper |
| `14_52week_extremes.sql` | `mv_52week_extremes` materialized view |
| `16_cleanup.sql` | Migration: drop old TUI/Web tables (no-op on fresh installs) |
| `17_extended_sma.sql` | Adds 150/200-day SMA columns |
| `18_corporate_actions.sql` | `stock_splits`, `dividends`, `earnings` |
| `19_earnings_yfinance.sql` | Earnings schema additions |
| `20_drop_revenue_estimate.sql` | Drop a deprecated column |
| `21_intraday_prices.sql` | `stock_prices_intraday` (5-min bars + view `stock_prices_live`) |
| `22_views_advanced.sql` | Views that depend on later tables |
| `23_add_cpi_yoy.sql` | Adds CPI year-over-year column |
| `24_widen_price_precision.sql` | Widens numeric precision on price columns |
| `25_trader_cards.sql` | `trader_cards` table |
| `26_market_internals.sql` | `market_internals` (VIX, VIX3M, HY spread, put/call) |
| `27_stock_character.sql` | Tables for stock character classification |
| `28_dashboard_market_internals.sql` | Dashboard view over market internals |

## Setup

### Recommended: via the pipeline

```bash
sawa coldstart --schema-only        # Drops tables, applies all SQL files
sawa coldstart --no-drop            # Re-applies SQL non-destructively (safe upgrade)
```

### Manual

```bash
# All at once
for f in sqlschema/*.sql; do psql "$DATABASE_URL" -f "$f"; done

# Or via the schema runner module
python -m sawa.database.schema --database-url "$DATABASE_URL" --drop --force
```

The runner uses `psycopg.sql.Identifier` for safe table-name handling and
verifies the expected tables exist after loading. See
`sawa/database/schema.py` for the list of `EXPECTED_TABLES`.

## Table Relationships

```
companies (ticker PK)
  ├─< stock_prices              (ticker, date)
  ├─< stock_prices_intraday     (ticker, ts)
  ├─< financial_ratios          (ticker, date)
  ├─< balance_sheets            (ticker, period_end, timeframe)
  ├─< income_statements         (ticker, period_end, timeframe)
  ├─< cash_flows                (ticker, period_end, timeframe)
  ├─< technical_indicators      (ticker, date)
  ├─< stock_splits              (ticker, execution_date)
  ├─< dividends                 (ticker, ex_date)
  ├─< earnings                  (ticker, period_end)
  ├─< index_constituents        (ticker, index_id) ──> indices
  └─< news_article_tickers      (article_id, ticker) ──> news_articles ──< news_sentiment

economy tables (independent)
  treasury_yields (date PK)
  inflation (date PK)
  inflation_expectations (date PK)
  labor_market (date PK)
  market_internals (date PK)
```

## Adding a Migration

1. Pick the next free `NN` (e.g. `29_*.sql`)
2. Make it idempotent: `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT
   EXISTS`, `DROP ... IF EXISTS`. The runner re-executes every file on
   `--no-drop` upgrades.
3. If the migration adds a new table, also add it to `EXPECTED_TABLES` in
   `sawa/database/schema.py`.
4. If it changes the contract of an existing load step, update the
   corresponding loader in `sawa/database/load.py`.

## Useful Queries

```sql
SELECT * FROM v_company_summary;
SELECT * FROM v_economy_dashboard LIMIT 10;
SELECT * FROM v_latest_fundamentals WHERE ticker = 'AAPL';
SELECT * FROM v_sector_summary;
SELECT * FROM stock_prices_live WHERE ticker = 'AAPL';
SELECT * FROM mv_52week_extremes WHERE ticker = 'AAPL';
```

Refresh materialized views after a price update:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_52week_extremes;
```
