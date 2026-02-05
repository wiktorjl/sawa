# Stock Price Validation Script - Implementation Plan

## Overview
Create a standalone Python script to perform out-of-band spot checking of stock prices from the database against Yahoo Finance data. The script will validate random samples of stocks and dates, identifying discrepancies beyond specified tolerances.

## Requirements

### Validation Parameters
- **Target table**: `stock_prices.close` (daily closing prices)
- **Discrepancy tolerance**: Flag if EITHER condition is met:
  - Relative difference > 0.5% (|yahoo - db| / yahoo > 0.005)
  - Absolute difference > $0.01 (|yahoo - db| > 0.01)
- **Default sample size**: N=50 stocks, M=10 days (500 comparisons)
- **Missing data handling**: Treat missing Yahoo Finance data as validation error

### Report Format
- Console output with color-coded summary
- Detailed markdown report file (`validation_report_YYYYMMDD_HHMMSS.md`)

## Architecture

### Script Location
`/home/user/code/sawa/scripts/validate_prices.py`

### Dependencies (not from sawa)
```
psycopg[binary] >= 3.1.0  # PostgreSQL adapter
yfinance >= 0.2.0         # Yahoo Finance data
python-dotenv >= 1.0.0    # Environment variable loading
```

### Components

#### 1. Configuration Module
- Load database credentials from `.env` file
- Parse command-line arguments (override defaults)
- Supported environment variables:
  - `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
- Command-line options:
  - `--stocks N`: Number of stocks to sample (default: 50)
  - `--days M`: Number of days to sample (default: 10)
  - `--output FILE`: Custom report filename
  - `--seed SEED`: Random seed for reproducibility

#### 2. Database Sampler
- Connect to PostgreSQL using psycopg
- Randomly select N tickers from `companies` table
  - Filter: `active = TRUE` (active stocks only)
- Randomly select M dates from `stock_prices` table
  - Strategy: Get distinct dates with data coverage across selected tickers
  - Ensure dates are recent (last 5 years) to avoid stale data
- Fetch database prices:
  ```sql
  SELECT ticker, date, close
  FROM stock_prices
  WHERE ticker = %s AND date = %s
  ```

#### 3. Yahoo Finance Fetcher
- Use `yfinance.download()` for batch efficiency
- Fetch historical data for all sampled stocks
- Batch dates to minimize API calls
- Handle errors:
  - Ticker not found → mark as validation error
  - No data for date → mark as validation error
  - Network errors → retry (3 attempts), then mark as error

#### 4. Price Comparator
- Compare database close price to Yahoo close price
- Tolerance calculation:
  ```python
  abs_diff = abs(yahoo_price - db_price)
  rel_diff = abs_diff / yahoo_price if yahoo_price != 0 else 1.0
  is_discrepancy = (abs_diff > 0.01) or (rel_diff > 0.005)
  ```
- Categorize issues:
  - **Critical**: Price mismatch beyond tolerance
  - **Warning**: Direction correct but magnitude off
  - **Error**: Missing data or fetch failure
  - **OK**: Within tolerance

#### 5. Report Generator
**Console Output:**
```
=== Stock Price Validation Report ===
Stocks sampled: 50
Dates sampled: 10
Total comparisons: 500
✓ Pass: 487
✗ Fail: 13
  - Critical mismatches: 8
  - Warnings: 3
  - Missing data errors: 2
```

**Markdown Report:**
- Summary section with counts and pass rate
- Detailed table of all discrepancies
  - Ticker, Date, DB Price, Yahoo Price, Abs Diff, Rel Diff, Status
- Statistics:
  - Distribution of discrepancies
  - Tickers with most issues
  - Dates with most issues
- Recommendations

### Execution Flow

```
1. Load configuration (.env + CLI args)
2. Connect to database
3. Sample N active tickers
4. Sample M dates from available price data
5. Fetch all DB prices for (ticker, date) pairs
6. Fetch Yahoo Finance data (batched by date ranges)
7. Compare prices with tolerance checks
8. Generate console report
9. Write detailed markdown report
10. Exit with status code:
    - 0: No issues found
    - 1: Issues detected
    - 2: Script error (connection, config, etc.)
```

## Data Structures

### Sampling Result
```python
class ValidationResult:
    ticker: str
    date: date
    db_price: float | None
    yahoo_price: float | None
    abs_diff: float
    rel_diff: float
    status: str  # "OK", "CRITICAL", "WARNING", "ERROR"
    message: str
```

### Report Summary
```python
class ReportSummary:
    total_comparisons: int
    passed: int
    failed: int
    critical: int
    warnings: int
    errors: int
    pass_rate: float
```

## Error Handling

### Database Errors
- Connection failure → Exit with status 2, log error
- Query errors → Log and continue with available data
- Null prices in DB → Mark as ERROR, report

### Yahoo Finance Errors
- Network timeout → Retry 3 times with 2s backoff
- Ticker not found → Mark as ERROR (ticker may have changed)
- No data for date → Mark as ERROR (missing data)
- Rate limit → Add delay between batch requests

## Implementation Considerations

### Performance
- Batch Yahoo Finance queries (download multiple tickers at once)
- Use database indexes on `(ticker, date)` - already exists per schema
- Parallel sampling queries (one for stocks, one for dates)
- Cache Yahoo Finance data per ticker to avoid redundant fetches

### Reproducibility
- Support `--seed` parameter for random sampling
- Log all sampled tickers and dates in report
- Include script version and timestamp

### Extensibility
- Easy to add new comparison metrics (volume, high/low)
- Pluggable tolerance strategies
- Configurable report formats

## Testing Strategy

### Manual Testing
1. Run with small sample: `--stocks 2 --days 2 --seed 42`
2. Verify markdown report is readable
3. Check console output colorization
4. Test with invalid database credentials

### Edge Cases to Test
- Stock with very low price (penny stocks)
- Stock with very high price (BRK.A, BRK.B)
- Dates with no data (weekends, holidays)
- Ticker delisted but not marked inactive
- Null values in database

## Command Examples

```bash
# Default run (50 stocks, 10 days)
python scripts/validate_prices.py

# Custom sample size
python scripts/validate_prices.py --stocks 20 --days 5

# Reproducible run
python scripts/validate_prices.py --seed 12345

# Custom output file
python scripts/validate_prices.py --output my_report.md
```

## Deliverables

1. **Primary script**: `scripts/validate_prices.py`
2. **Requirements file**: `scripts/validation_requirements.txt` (optional, or add to main requirements)
3. **Documentation**: Inline docstrings + this plan document
4. **Sample report**: `validation_report_example.md` (after first run)

## Success Criteria

- Script runs standalone without importing sawa modules
- Successfully connects to database using .env credentials
- Randomly samples specified number of stocks and dates
- Fetches Yahoo Finance data reliably
- Identifies price discrepancies with correct tolerance logic
- Generates readable console and markdown reports
- Handles errors gracefully with informative messages
- Exits with appropriate status codes
