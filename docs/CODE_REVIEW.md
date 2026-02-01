# Code Review Report

**Project**: S&P 500 Data Downloader  
**Review Date**: 2026-01-31  
**Reviewer**: Automated Code Review

## Executive Summary

This Python project for downloading S&P 500 market data and serving it via an MCP server is well-structured with good documentation. However, the review identified **29 issues** including critical security vulnerabilities, resource management bugs, and significant code duplication.

| Severity | Count |
|----------|-------|
| Critical | 3 |
| High | 5 |
| Medium | 12 |
| Low | 8 |
| Testing | 1 |

---

## Critical Issues

### 1. SQL Injection in rebuild_database.py
**File**: `rebuild_database.py:107, 124`  
**Category**: Security

Table and function names are directly interpolated into SQL:
```python
cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
cur.execute(f'DROP FUNCTION IF EXISTS "{func}" CASCADE')
```

**Fix**: Use `psycopg.sql` module for safe identifier handling:
```python
from psycopg import sql
cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))
```

### 2. SQL Injection in load_csv_to_postgres.py
**File**: `load_csv_to_postgres.py:322-339`  
**Category**: Security

Table and column names are interpolated directly:
```python
query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
```

**Fix**: Use `psycopg2.sql` module:
```python
from psycopg2 import sql
query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
    sql.Identifier(table_name),
    sql.SQL(', ').join(map(sql.Identifier, columns))
)
```

### 3. Weak SQL Validation in MCP Server
**File**: `mcp_server/database.py:42-86`  
**Category**: Security

The `validate_select_query` function has bypass vectors:
- Does not handle SQL comments: `SELECT * FROM t; -- INSERT INTO...`
- Does not handle CTEs: `WITH x AS (DELETE FROM t) SELECT * FROM x`
- Regex can be bypassed with different whitespace

**Fix**: Set read-only mode at connection level:
```python
def get_connection():
    conn = psycopg.connect(get_database_url(), row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
    return conn
```

---

## High Severity Issues

### 4. Resource Leak - Temp File Not Cleaned
**File**: `download_daily_prices.py:342-350`  
**Category**: Bug

Temp file is not cleaned up on exception during S3 download:
```python
with tempfile.NamedTemporaryFile(delete=False) as tmp:
    s3_client.download_fileobj(bucket, key, tmp)
    return tmp.name  # If exception here, file leaks
```

**Fix**:
```python
tmp = tempfile.NamedTemporaryFile(delete=False)
try:
    s3_client.download_fileobj(bucket, key, tmp)
    tmp.close()
    return tmp.name
except Exception:
    tmp.close()
    os.unlink(tmp.name)
    raise
```

### 5. Timezone-Naive Date Handling
**File**: `check_trading_days.py:156`  
**Category**: Bug

Using `datetime.fromtimestamp()` without timezone can cause date misalignment:
```python
timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
```

**Fix**:
```python
from datetime import timezone
timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
```

### 6. Incorrect Year Calculation
**Files**: `check_trading_days.py:88,91`, `download_fundamentals.py:170,172`, `download_economy_data.py:152,154`  
**Category**: Bug

Using `years * 365` doesn't account for leap years:
```python
calc_start = calc_end - timedelta(days=years * 365)
```

**Fix**:
```python
from dateutil.relativedelta import relativedelta
calc_start = calc_end - relativedelta(years=years)
```

### 7. Missing Ticker Input Validation
**Files**: `mcp_server/tools/companies.py:115`, `market_data.py:52`, etc.  
**Category**: Security

Ticker symbols are uppercased but not validated for format.

**Fix**:
```python
import re
def validate_ticker(ticker: str) -> str:
    if not re.match(r'^[A-Z]{1,5}(\.[A-Z])?$', ticker.upper()):
        raise ValueError(f"Invalid ticker format: {ticker}")
    return ticker.upper()
```

### 8. Database Connection Not Closed on Error
**File**: `load_csv_to_postgres.py:651`  
**Category**: Bug

Connection is only closed in the happy path.

**Fix**: Use try/finally:
```python
try:
    conn = connect_to_database(conn_params, logger)
    # ... operations ...
finally:
    if conn:
        conn.close()
```

---

## Medium Severity Issues

### 9. Type Hint Inconsistency - Old Syntax
**Files**: `download_sp500_symbols.py:27,73,111,150`, `check_trading_days.py`, `download_daily_prices.py`, `download_fundamentals.py`, `download_economy_data.py`, `ratio_downloader.py`, `load_csv_to_postgres.py`  
**Category**: Style

Using deprecated `typing.List[T]` instead of `list[T]` (Python 3.10+):
```python
from typing import List
def fetch_sp500_symbols(logger: logging.Logger) -> List[str]:
```

**Fix**: Use modern syntax per project guidelines:
```python
def fetch_sp500_symbols(logger: logging.Logger) -> list[str]:
```

### 10. Duplicate Code - Logger Setup
**Files**: All main download scripts (8+ files)  
**Category**: Design

`setup_logging()` is duplicated identically across files.

**Fix**: Create shared utility module `utils/logging.py`.

### 11. Duplicate Code - Date Range Calculation
**Files**: `check_trading_days.py:67-97`, `download_fundamentals.py:149-178`, `download_economy_data.py:131-160`  
**Category**: Design

`calculate_date_range()` is copied across files.

**Fix**: Create shared `utils/dates.py` module.

### 12. Duplicate Code - Date Parsing
**Files**: 5+ files with identical `parse_date()` function  
**Category**: Design

**Fix**: Consolidate into shared utility.

### 13. Missing Exception Chaining
**Files**: `download_fundamentals.py:116-118`, `download_economy_data.py:125-128`  
**Category**: Bug

```python
except ValueError as e:
    raise argparse.ArgumentTypeError(...)  # Missing 'from e'
```

**Fix**:
```python
except ValueError as e:
    raise argparse.ArgumentTypeError(...) from e
```

### 14. Broad Exception Catching
**Files**: `download_fundamentals.py:355`, `download_economy_data.py:266`, `combine_fundamentals.py:144`  
**Category**: Bug

Using bare `except Exception` masks specific errors.

**Fix**: Catch specific exceptions like `OSError`, `csv.Error`.

### 15. Import Inside Function
**File**: `mcp_server/server.py:311`  
**Category**: Style

`json` is imported inside the function.

**Fix**: Move to top of file with other imports.

### 16. Invalid SQL Identifier
**File**: `mcp_server/tools/fundamentals.py:200-201`  
**Category**: Bug

```sql
research_and_development as r&d,
selling_general_and_administrative as sg&a,
```

`&` is invalid in unquoted SQL identifiers.

**Fix**:
```sql
research_and_development as r_and_d,
selling_general_and_administrative as sga,
```

### 17. Opening Files Per Row
**File**: `download_daily_prices.py:426`  
**Category**: Performance

Opening file with `with open()` for each row instead of batching:
```python
with open(out_path, "a", newline="") as out_f:
    writer = csv.writer(out_f)
```

**Fix**: Batch writes by symbol or maintain dictionary of open file handles.

### 18. Inefficient File Tail Reading
**File**: `download_daily_prices.py:272-316`  
**Category**: Performance

Reading 4KB and parsing all lines to check for a date is inefficient for large files.

### 19. N+1 Query Pattern
**File**: `mcp_server/tools/companies.py:76-116`  
**Category**: Performance

LATERAL joins for every company detail query could be optimized with materialized views.

### 20. Incorrect Return Type
**File**: `download_economy_data.py:367`  
**Category**: Style

Function returns `None` but type hint says `Path`:
```python
def download_endpoint(...) -> Path:
    if not data:
        return None  # Type mismatch
```

**Fix**: `def download_endpoint(...) -> Path | None:`

---

## Low Severity Issues

### 21. Magic Numbers
**File**: `download_daily_prices.py:295`  
**Category**: Style

```python
offset = min(size, 4096)
```

**Fix**: Use named constant `READ_BUFFER_SIZE = 4096`.

### 22. F-string Without Interpolation
**File**: `download_sp500_symbols.py:90`  
**Category**: Style

```python
logger.info(f"Fetching S&P 500 constituents from Wikipedia...")
```

**Fix**: Remove `f` prefix for plain strings.

### 23. Hardcoded Batch Size
**File**: `download_overviews.py:354`  
**Category**: Design

```python
batch_size = 10  # Save every N tickers
```

**Fix**: Make configurable via constant or CLI argument.

### 24. Empty __init__.py
**File**: `mcp_server/tools/__init__.py`  
**Category**: Style

Should export public API or have module docstring.

### 25. Redundant Sort
**File**: `rebuild_database.py:23-31`  
**Category**: Bug

`get_sql_files` adds files in order then sorts again.

### 26. Missing Connection Type Hint
**File**: `rebuild_database.py:34`  
**Category**: Style

```python
def execute_sql_file(conn, file_path: Path, dry_run: bool = False) -> bool:
```

`conn` parameter missing type hint.

### 27. Potential Empty List Access
**File**: `check_trading_days.py:312-313`  
**Category**: Bug

```python
logger.info(f"First trading day: {trading_days[0]}")
logger.info(f"Last trading day:  {trading_days[-1]}")
```

Would crash if list empty (protected by earlier check, but fragile).

### 28. Inconsistent Error Handling Pattern
**Files**: Various  
**Category**: Style

Some files use `sys.exit(1)` immediately, others re-raise. Should standardize.

---

## Testing Gaps

### 29. No Test Files
**Severity**: High  
**Category**: Testing

No test files exist despite `pytest` in dev dependencies.

**Recommended Test Files**:
- `tests/test_database.py` - SQL validation, query execution
- `tests/test_companies.py` - MCP company tools
- `tests/test_market_data.py` - Price/ratio queries
- `tests/test_date_utils.py` - Date calculations
- `tests/test_csv_parsing.py` - Bulk file parsing
- `tests/test_symbol_fetching.py` - Wikipedia parsing (mocked)

---

## Priority Recommendations

### Immediate (Security)
1. Fix SQL injection in `rebuild_database.py` using `psycopg.sql`
2. Fix SQL injection in `load_csv_to_postgres.py` using `psycopg2.sql`
3. Add `SET default_transaction_read_only = on` in MCP server connections
4. Add ticker format validation

### High Priority
5. Fix temp file resource leak in `download_daily_prices.py`
6. Use timezone-aware datetime handling
7. Add comprehensive test suite

### Medium Priority
8. Create shared utility modules to eliminate duplication:
   - `utils/logging.py`
   - `utils/dates.py`
9. Update type hints to Python 3.10+ syntax
10. Fix invalid SQL column aliases in fundamentals.py

### Low Priority
11. Add named constants for magic numbers
12. Standardize error handling patterns
13. Add proper `__init__.py` exports

---

## Files Reviewed

| File | Lines | Issues |
|------|-------|--------|
| `download_sp500_symbols.py` | 253 | 2 |
| `check_trading_days.py` | 330 | 3 |
| `download_daily_prices.py` | 802 | 4 |
| `download_fundamentals.py` | 843 | 3 |
| `download_economy_data.py` | 470 | 3 |
| `download_overviews.py` | ~400 | 1 |
| `ratio_downloader.py` | ~350 | 1 |
| `load_csv_to_postgres.py` | ~700 | 2 |
| `rebuild_database.py` | ~200 | 2 |
| `combine_fundamentals.py` | ~200 | 1 |
| `mcp_server/server.py` | 347 | 1 |
| `mcp_server/database.py` | 129 | 1 |
| `mcp_server/tools/companies.py` | 167 | 2 |
| `mcp_server/tools/market_data.py` | ~150 | 1 |
| `mcp_server/tools/fundamentals.py` | ~250 | 1 |
| `mcp_server/tools/economy.py` | ~150 | 0 |
| `mcp_server/tools/__init__.py` | 1 | 1 |
