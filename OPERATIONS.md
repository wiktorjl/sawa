# Operations Guide

This document covers operational procedures for the S&P 500 data pipeline.

## Prerequisites

### Environment Variables

Set these in your shell or `.env` file:

```bash
POLYGON_API_KEY=your_polygon_api_key
POLYGON_S3_ACCESS_KEY=your_s3_access_key
POLYGON_S3_SECRET_KEY=your_s3_secret_key
DATABASE_URL=postgresql://user:pass@host:5432/dbname
```

### Database

PostgreSQL 14+ with a database created:

```bash
createdb sp500_data
```

### Python Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Commands Overview

| Command | Purpose | Frequency |
|---------|---------|-----------|
| `sawa coldstart` | Full database setup from scratch | Once, or to rebuild |
| `sawa daily` | Update stock prices | Daily (weekdays) |
| `sawa weekly` | Update fundamentals, economy, etc. | Weekly |
| `sawa update` | Legacy combined update | Deprecated |

## Coldstart Procedure

Use coldstart when:
- Setting up a new database
- Rebuilding after schema changes
- Starting fresh after data corruption

```bash
# Full bootstrap (5 years of data)
sawa coldstart --years 5

# With logging to file
sawa coldstart --years 5 --log-dir logs

# Schema only (no data download)
sawa coldstart --schema-only

# Load existing CSV data (no downloads)
sawa coldstart --load-only
```

## Daily Update Procedure

Updates stock prices only. Fast operation, safe to run multiple times.

```bash
# Standard daily update
sawa daily

# With logging
sawa daily --log-dir logs

# Preview what would be done
sawa daily --dry-run

# Force update from specific date
sawa daily --from-date 2024-01-15
```

### Cron Schedule (Daily)

Run at 6 AM on weekdays (after market data is available):

```cron
0 6 * * 1-5 /path/to/sawa_daily_pipeline/scripts/daily.sh
```

## Weekly Update Procedure

Updates slow-changing data:
- Company profiles/overviews
- Financial fundamentals (balance sheets, income, cash flow)
- Financial ratios
- Economy data (treasury yields, inflation, labor market)
- News articles

```bash
# Full weekly update
sawa weekly

# Skip specific data types
sawa weekly --skip-news
sawa weekly --skip-fundamentals --skip-ratios

# Preview what would be done
sawa weekly --dry-run
```

### Cron Schedule (Weekly)

Run Sunday at 2 AM:

```cron
0 2 * * 0 /path/to/sawa_daily_pipeline/scripts/weekly.sh
```

## Re-entrancy

All operations are safe to re-run:

| Data Type | Key | Behavior |
|-----------|-----|----------|
| Stock prices | (ticker, date) | Updates existing records |
| Fundamentals | (ticker, period_end, timeframe) | Updates existing records |
| Economy | (date) | Updates existing records |
| Companies | (ticker) | Updates existing records |
| Ratios | (ticker, date) | Updates existing records |

If an update fails partway through:
1. Check the log file for errors
2. Fix the underlying issue (network, API limits, etc.)
3. Re-run the same command - it will pick up where it left off

## Log Files

Logs are written to the `logs/` directory with timestamps:

```
logs/
  daily_20240115_060001.log
  weekly_20240114_020001.log
  coldstart_20240101_120000.log
```

**Console output**: INFO level and above
**File output**: DEBUG level (more detailed)

### Log Format

```
2024-01-15 06:00:01 [INFO] Starting daily update
2024-01-15 06:00:02 [INFO] Found 503 symbols in database
2024-01-15 06:00:03 [INFO] Downloading prices for 2024-01-15
2024-01-15 06:00:45 [INFO] Loaded 503 price records
2024-01-15 06:00:45 [INFO] Daily update complete
```

## Troubleshooting

### "No existing data found. Run coldstart first."

The database has no data. Run:
```bash
sawa coldstart
```

### "No symbols in database"

The companies table is empty. Either:
1. Run a full coldstart
2. Or run coldstart with `--skip-downloads` if you have CSV data

### API Rate Limits

The pipeline includes rate limiting. If you hit limits:
- Wait and retry
- Check your API plan limits
- Consider spreading updates over time

### Database Connection Issues

Check:
1. DATABASE_URL is set correctly
2. PostgreSQL is running
3. Network connectivity to database host
4. User has required permissions

### S3 Download Failures

Check:
1. S3 credentials are valid
2. Network connectivity
3. Date range is within available data

## Data Directory Structure

```
data/
  sp500_symbols.txt           # List of tracked symbols
  prices/
    AAPL.csv                  # Per-symbol price files
    MSFT.csv
    ...
  fundamentals/
    balance_sheets.csv
    cash_flow.csv
    income_statements.csv
  economy/
    treasury_yields.csv
    inflation.csv
    inflation_expectations.csv
    labor_market.csv
  overviews/
    overviews.csv
  ratios/
    ratios.csv
```

## Monitoring

For production deployments, consider:

1. **Cron job monitoring**: Use a service like Healthchecks.io
2. **Log aggregation**: Send logs to centralized logging
3. **Alerting**: Alert on failed runs or missing data
4. **Database monitoring**: Track table sizes and query performance

Example healthcheck integration in cron:

```cron
0 6 * * 1-5 /path/to/scripts/daily.sh && curl -s https://hc-ping.com/your-uuid
```
