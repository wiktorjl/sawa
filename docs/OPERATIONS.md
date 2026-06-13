# Operations Guide

Operational reference for the Sawa data pipeline. For a higher-level overview
see [MAINTENANCE.md](MAINTENANCE.md); for project intro see the top-level
[`README.md`](../README.md). For the per-table mapping of external data
sources (which API populates which table) see
[DATA_SOURCES.md](DATA_SOURCES.md).

## Prerequisites

### Environment Variables

Set these in your shell or `.env` file (copy `.env.example`):

```bash
POLYGON_API_KEY=...              # Polygon REST
POLYGON_S3_ACCESS_KEY=...        # Polygon S3 (bulk history)
POLYGON_S3_SECRET_KEY=...
FRED_API_KEY=...                 # FRED — market internals (VIX, VIX3M, HY spread)
DATABASE_URL=postgresql://user:pass@host:5432/dbname
NTFY_TOPIC=https://ntfy.sh/...   # optional; pipeline + scheduler push notifications
```

Missing API keys behave differently by key:

- `POLYGON_API_KEY` (or `--api-key`) is required up front. If it is missing,
  `daily`/`weekly`/`quarterly`/`coldstart` log an error and exit non-zero
  before doing any work — Polygon underpins almost every step.
- `FRED_API_KEY` is optional. If it is missing, only the FRED market-internals
  step is skipped: the job logs an error, sends an ntfy alert (if `NTFY_TOPIC`
  is set) via `alert_missing_api_key`, and still exits 0.

### Database

PostgreSQL 12+ (14+ recommended). Create the database:

```bash
createdb sp500_data   # name is arbitrary; match DATABASE_URL
```

### Python Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cd mcp_server && pip install -e ".[dev]" && cd ..
```

TA-Lib needs the C library: `brew install ta-lib` (macOS) or
`apt install libta-lib-dev` (Ubuntu).

## Command Overview

| Command | Purpose | Frequency |
|---------|---------|-----------|
| `sawa coldstart` | Full database setup / rebuild | Once, then on schema rebuilds |
| `sawa daily` | Prices, news, TA, market internals | Daily after market close |
| `sawa weekly` | Economy, overviews, news, corporate actions, character | Weekly |
| `sawa quarterly` | Fundamentals + financial ratios | Quarterly |
| `sawa intraday` | WebSocket 5-min bars (15-min delayed) | During market hours |
| `sawa doctor` | Database sanity/completeness checks after jobs | After scheduled jobs |
| `sawa add-symbol` | Add new ticker(s) ad-hoc | As needed |
| `sawa adjust-splits` | Re-fetch adjusted prices after splits | After known split |
| `sawa ta-backfill` | Recompute technical indicators from history | After schema/code change |
| `sawa character` | Stock character classification (also runs in weekly) | As needed |
| `sawa data-status` | Show data freshness | Diagnostic |

All commands accept `--log-dir logs --verbose`.

## Coldstart Procedure

Use when:
- Setting up a new database
- Rebuilding after destructive schema changes
- Starting fresh after data corruption

```bash
sawa coldstart --years 5                   # Full bootstrap (5y of data)
sawa coldstart --years 5 --log-dir logs
sawa coldstart --schema-only               # DANGER: drops/recreates all tables; use throwaway DB
sawa coldstart --no-drop                   # Re-apply schema without dropping (safe upgrade)
sawa coldstart --load-only                 # Load already-downloaded CSVs only
sawa coldstart --skip-downloads            # Schema + load existing CSVs
sawa coldstart --drop-only --confirm-drop  # Destructive: drop everything
```

The universe is the union of S&P 500 (scraped from Wikipedia) and
NASDAQ-5000 (loaded from `data/nasdaq1000_symbols.txt`; despite the filename
the list contains ~5000 NASDAQ-listed tickers).

## Daily Update

Run after market close. Pulls prices, news, technical indicators, and market
internals (FRED). Safe to re-run — all upserts.

```bash
sawa daily
sawa daily --log-dir logs --verbose
sawa daily --dry-run                       # Preview only
sawa daily --from-date 2024-01-15          # Force replay from date
sawa daily --skip-news                     # Prices + TA only
sawa daily --skip-ta                       # Prices + news only
sawa daily --skip-market-internals         # Skip FRED step
sawa daily --news-only                     # Only update news
```

After a successful scheduled daily run, `sawa doctor --job daily` checks the
database before the scheduler marks the day complete. It verifies required
tables/views, active-company counts, latest `stock_prices` recency, latest-day
ticker coverage against recent populated price dates, OHLCV sanity, technical
indicator coverage, news/market internals freshness, and 52-week
materialized-view freshness.

## Weekly Update

```bash
sawa weekly
sawa weekly --skip-news --skip-overviews
sawa weekly --skip-corporate-actions
sawa weekly --skip-character               # Skip character classification batch
sawa weekly --character-workers 8
sawa weekly --dry-run
```

Updates:
- Economy: treasury yields, CPI/PCE, inflation expectations, labor market
- Company overviews
- News articles
- Corporate actions (splits, dividends)
- Stock character classification (Hurst-based regime classification)

After a successful scheduled weekly run, `sawa doctor --job weekly` checks the
database before the scheduler marks the ISO week complete. It validates core
price coverage plus economy table freshness, stock-character coverage, and
corporate-action table readability.

## Quarterly Update

```bash
sawa quarterly
sawa quarterly --skip-fundamentals
sawa quarterly --skip-ratios
```

Pulls balance sheets, income statements, cash flows, and financial ratios.

## Scheduling

### Recommended: `scripts/market_scheduler.sh`

A single cron entry handles intraday streaming during market hours, runs
`daily` ~1h after close, and `weekly` on Saturdays:

```cron
*/15 * * * 1-5 /path/to/sawa/scripts/market_scheduler.sh >> ~/.sawa/scheduler/cron.log 2>&1
```

State lives under `~/.sawa/scheduler/`. Sends ntfy notifications if
`NTFY_TOPIC` is set. The scheduler runs `sawa doctor --job daily` and
`sawa doctor --job weekly` after successful jobs; if doctor exits non-zero,
the job is not marked done and an error notification is sent.

### Alternative: discrete cron entries

```cron
0 18 * * 1-5 /path/to/sawa/scripts/daily.sh
0  2 * * 6   /path/to/sawa/scripts/weekly.sh   # Saturday, matching market_scheduler.sh
```

Quarterly is small — run by hand or once a quarter.

## Database Doctor

Use `doctor` when you want a database-only health check without contacting
external APIs:

```bash
sawa doctor                                # broad database check
sawa doctor --job daily                    # checks relevant after daily
sawa doctor --job weekly                   # checks relevant after weekly
sawa doctor --min-coverage 0.95            # stricter coverage vs recent baseline
sawa doctor --max-staleness-days 3         # stricter stock_prices recency
```

Exit code is `0` only when there are no failed checks. Warnings are printed and
included in notifications, but do not make the command fail.

## Re-entrancy

All operations are safe to re-run.

| Data | Key | Behavior |
|------|-----|----------|
| Stock prices | (ticker, date) | Upsert |
| Intraday prices | (ticker, timestamp) | Upsert |
| Fundamentals | (ticker, period_end, timeframe) | Upsert |
| Economy | (date) | Upsert |
| Market internals | (date) | Upsert |
| Companies | (ticker) | Upsert |
| Ratios | (ticker, date) | Upsert |
| News articles | (id) | Upsert |
| Technical indicators | (ticker, date) | Upsert |

If an update fails partway:
1. Check the log file for the underlying error
2. Fix it (network, API limits, schema mismatch)
3. Re-run the same command — the upsert keys mean partial progress is fine

## Log Files

```
logs/
  coldstart_YYYYMMDD_HHMMSS.log
  daily_YYYYMMDD_HHMMSS.log
  weekly_YYYYMMDD_HHMMSS.log
  quarterly_YYYYMMDD_HHMMSS.log
  ta_backfill_YYYYMMDD_HHMMSS.log
  character_YYYYMMDD_HHMMSS.log
```

Console output is INFO; the file gets DEBUG.

## Troubleshooting

### "No existing data found. Run coldstart first."
Database empty. Run `sawa coldstart`.

### "No symbols in database"
`companies` table is empty. Run `sawa coldstart`, or `sawa coldstart
--skip-downloads` if you already have CSVs in `data/`.

### "FRED_API_KEY not set" / market internals skipped
The step is skipped with an ntfy alert. Set `FRED_API_KEY` to fix.
Get a free key at <https://fred.stlouisfed.org/docs/api/api_key.html>.

### API rate limits
The pipeline uses `SyncRateLimiter` (default 5 req/s for Polygon — see
`sawa/utils/constants.py`). On 429s, wait and retry.

### Database connection
Confirm: `DATABASE_URL` set, PostgreSQL running, network reachable, user
has DDL + DML on the target database.

### S3 download failures
Confirm S3 credentials, network, and that the date range falls within
Polygon's available history (typically 5+ years back).

### Adjusting after a stock split
Polygon's daily aggregates are split-adjusted at fetch time, but historical
data already in the DB will not be retroactively adjusted. Run
`sawa adjust-splits --ticker XYZ` or let `sawa adjust-splits` auto-detect
recent splits.

## Data Directory Layout

```
data/
  nasdaq1000_symbols.txt        # NASDAQ universe list (~5000 tickers)
  data_mappings.json            # Optional CSV→table mapping for sawa.database.loader
  prices/AAPL.csv               # Per-symbol price files (coldstart output)
  fundamentals/
    balance_sheets.csv          income_statements.csv      cash_flow.csv
    *_update.csv                # Weekly delta files
  economy/
    treasury_yields.csv         inflation.csv
    inflation_expectations.csv  labor_market.csv
    market_internals.csv        # FRED-sourced VIX / VIX3M / HY spread
  overviews/overviews.csv
  ratios/ratios.csv
```

## Monitoring

Production deployments should consider:

1. **Job monitoring**: configure `NTFY_TOPIC`; `market_scheduler.sh` already
   sends start/stop/failure notifications.
2. **Healthchecks**: pair the cron call with a heartbeat ping:
   ```cron
   0 18 * * 1-5 /path/to/scripts/daily.sh && curl -fsS https://hc-ping.com/UUID
   ```
3. **Data freshness**: `sawa data-status` reports latest date per price table.
   Schedule as a sanity check.
4. **Database size**: views in `sqlschema/06_views.sql` and
   `22_views_advanced.sql` are good targets for slow-query monitoring.
