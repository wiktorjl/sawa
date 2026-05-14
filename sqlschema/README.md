# SQL Schema

PostgreSQL schema files for Sawa, executed in numeric prefix order. The
loader globs `NN_*.sql` and sorts; gaps in numbering are harmless.

## Files

| File | Purpose | Data source for the tables it creates |
|------|---------|---------------------------------------|
| `00_setup.sql` | Documentation + verification queries (does not create anything) | — |
| `01_companies.sql` | `companies` (ticker is PK; central reference) | Polygon REST `/v3/reference/tickers/{t}`; ticker universe from Wikipedia (sp500) + bundled `data/nasdaq1000_symbols.txt` (nasdaq5000) |
| `02_market_data.sql` | `stock_prices`, `financial_ratios` | Polygon S3 (bulk) + Polygon REST `/v2/aggs/...` (incremental); `/stocks/financials/v1/ratios` |
| `03_fundamentals.sql` | `balance_sheets`, `income_statements`, `cash_flows` | Polygon REST `/stocks/financials/v1/{balance-sheets,income-statements,cash-flow-statements}` |
| `04_economy.sql` | `treasury_yields`, `inflation`, `inflation_expectations`, `labor_market` | Polygon REST `/fed/v1/{treasury-yields,inflation,inflation-expectations,labor-market}` |
| `05_indexes.sql` | Performance indexes for the above tables | — (DDL only) |
| `06_views.sql` | Read-only views: `v_company_summary`, `v_economy_dashboard`, `v_latest_fundamentals`, `v_sector_summary`, dashboard market internals view | computed (views) |
| `07_procedures.sql` | PL/pgSQL helpers for loading | — |
| `08_sic_gics_mapping.sql` | `sic_gics_mapping` table (SIC → GICS) | seed data (next file) |
| `09_sic_gics_data.sql` | Seed data for `sic_gics_mapping` | bundled SQL seed |
| `10_news.sql` | `news_articles`, `news_article_tickers`, `news_sentiment` | Polygon REST `/v2/reference/news` (sentiment is supplied by Polygon in the `insights` field, not computed locally) |
| `11_technical_indicators.sql` | `technical_indicators`, `technical_indicator_metadata` | **computed locally** from `stock_prices` via TA-Lib (`sawa/calculation/`, `sawa/ta_backfill.py`) |
| `12_indices.sql` | `indices`, `index_constituents`, seeds (`sp500`, `nasdaq5000`) | seed data + Wikipedia (sp500) + bundled file (nasdaq5000) |
| `13_gics_sector_function.sql` | `get_gics_sector(sic_code)` helper | — |
| `14_52week_extremes.sql` | `mv_52week_extremes` materialized view | computed from `stock_prices` |
| `16_cleanup.sql` | Migration: drop old TUI/Web tables (no-op on fresh installs) | — |
| `17_extended_sma.sql` | Adds 150/200-day SMA columns | computed (TA-Lib) |
| `18_corporate_actions.sql` | `stock_splits`, `dividends`, `earnings` | Polygon REST `/v3/reference/{splits,dividends}`; `earnings` populated manually via `scripts/populate_earnings.py` (yfinance) — see `docs/DATA_SOURCES.md` §2.7 |
| `19_earnings_yfinance.sql` | Earnings schema additions | yfinance (via populate script) |
| `20_drop_revenue_estimate.sql` | Drop a deprecated column | — |
| `21_intraday_prices.sql` | `stock_prices_intraday` (5-min bars + view `stock_prices_live`) | Polygon WebSocket (`wss://delayed.polygon.io/stocks`) |
| `22_views_advanced.sql` | Views that depend on later tables | computed (views) |
| `23_add_cpi_yoy.sql` | Adds CPI year-over-year column | computed from `inflation` |
| `24_widen_price_precision.sql` | Widens numeric precision on price columns | — |
| `25_trader_cards.sql` | `trader_cards` table | externally parsed from chart-analysis card.md files; no auto loader |
| `26_market_internals.sql` | `market_internals` (VIX, VIX3M, HY spread) | FRED (`VIXCLS`, `VXVCLS`, `BAMLH0A0HYM2`) — sole source since commit `2d4e350` |
| `27_stock_character.sql` | Tables for stock character classification (`stock_character_classification`, `stock_character_baseline`, `stock_character_flags`, `stock_character_scorecard`) | **computed locally** from `stock_prices` + `technical_indicators` + fundamentals (`sawa/calculation/stock_character*.py`, `sawa/stock_character_batch.py`) |
| `29_consolidate_vix.sql` | Migration: drop legacy VIX rows from `stock_prices` / `companies` (see [`docs/VIX_MIGRATION.md`](../docs/VIX_MIGRATION.md)) | — |

For the canonical mapping of external data source → table → loader →
pipeline command, see [`docs/DATA_SOURCES.md`](../docs/DATA_SOURCES.md).

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
