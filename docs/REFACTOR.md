# Refactoring Plan

**Project**: S&P 500 Data Downloader  
**Date**: 2026-01-31  
**Approach**: Monorepo with shared utilities (Option C)

## Executive Summary

Consolidate 10+ standalone scripts into a unified package architecture with shared utilities. The MCP server remains independently installable while sharing common code.

**Key Outcomes:**
- ~36% code reduction (5,500 → 3,500 lines)
- 0 duplicate functions (down from 6)
- Clean package structure with `pip install` support
- Security fixes integrated

---

## Target Structure

```
.
├── pyproject.toml            # Root package (sp500-tools)
├── sp500_tools/              # Main package
│   ├── __init__.py
│   ├── utils/                # Shared utilities
│   │   ├── __init__.py
│   │   ├── logging.py        # setup_logging()
│   │   ├── dates.py          # parse_date(), calculate_date_range()
│   │   ├── config.py         # Environment variable handling
│   │   ├── symbols.py        # Symbol file loading + validation
│   │   ├── csv_utils.py      # CSV read/write helpers
│   │   └── cli.py            # Common argparse patterns
│   ├── downloaders/          # Data download modules
│   │   ├── __init__.py
│   │   ├── massive.py        # Merged: fundamentals + economy + overviews
│   │   ├── polygon.py        # Merged: ratios + trading_days
│   │   ├── polygon_s3.py     # Renamed: download_daily_prices
│   │   └── symbols.py        # Renamed: download_sp500_symbols
│   ├── database/             # Database utilities
│   │   ├── __init__.py
│   │   ├── loader.py         # Renamed: load_csv_to_postgres
│   │   ├── schema.py         # Renamed: rebuild_database
│   │   └── connection.py     # Shared connection handling
│   └── processing/           # Data processing
│       ├── __init__.py
│       └── combine.py        # Renamed: combine_fundamentals
├── mcp_server/               # MCP server (independent package)
│   ├── pyproject.toml        # Depends on sp500-tools
│   ├── server.py
│   ├── database.py
│   └── tools/
├── data/                     # Output directory
└── tests/                    # Test suite (future)
```

---

## Phase 1: Create Shared Utilities

### 1.1 `sp500_tools/utils/logging.py`

Extract from 8 files:

```python
"""Unified logging configuration."""
import logging
import sys
from typing import TextIO

def setup_logging(
    verbose: bool = False,
    name: str | None = None,
    stream: TextIO = sys.stdout,
) -> logging.Logger:
    """Configure logging with timestamps and appropriate level."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(stream)],
    )
    return logging.getLogger(name or __name__)
```

### 1.2 `sp500_tools/utils/dates.py`

Extract from 5 files + fix leap year bug:

```python
"""Date parsing and range calculation utilities."""
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta
import argparse

DATE_FORMAT = "%Y-%m-%d"
DEFAULT_YEARS = 5

def parse_date(date_str: str) -> date:
    """Parse YYYY-MM-DD date string for argparse."""
    try:
        return datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD."
        ) from e  # Fixed: exception chaining

def calculate_date_range(
    start_date: date | None = None,
    end_date: date | None = None,
    years: int | None = None,
) -> tuple[date, date]:
    """Calculate start and end dates (uses relativedelta for leap years)."""
    calc_end = end_date or date.today()
    
    if start_date:
        calc_start = start_date
    elif years:
        calc_start = calc_end - relativedelta(years=years)
    else:
        calc_start = calc_end - relativedelta(years=DEFAULT_YEARS)
    
    if calc_start >= calc_end:
        raise ValueError(f"Start date {calc_start} must be before end date {calc_end}")
    
    return calc_start, calc_end

def timestamp_to_date(timestamp_ms: int) -> date:
    """Convert millisecond timestamp to date (timezone-aware)."""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).date()
```

### 1.3 `sp500_tools/utils/symbols.py`

Extract from 4 files + add validation:

```python
"""Symbol file loading and validation utilities."""
import re
from pathlib import Path
import logging

TICKER_PATTERN = re.compile(r'^[A-Z]{1,5}(\.[A-Z])?$')

def validate_ticker(ticker: str) -> str:
    """Validate and normalize ticker symbol."""
    ticker = ticker.upper().strip()
    if not TICKER_PATTERN.match(ticker):
        raise ValueError(f"Invalid ticker format: {ticker}")
    return ticker

def load_symbols(
    filepath: str | Path,
    logger: logging.Logger | None = None,
    validate: bool = True,
) -> list[str]:
    """Load stock symbols from a text file."""
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Symbols file not found: {filepath}")
    
    symbols = []
    with open(filepath, "r") as f:
        for line_num, line in enumerate(f, 1):
            symbol = line.strip()
            if not symbol or symbol.startswith("#"):
                continue
            symbol = symbol.upper()
            if validate:
                try:
                    symbol = validate_ticker(symbol)
                except ValueError as e:
                    if logger:
                        logger.warning(f"Line {line_num}: {e}")
                    continue
            symbols.append(symbol)
    
    if logger:
        logger.info(f"Loaded {len(symbols)} symbols from {filepath}")
    return symbols
```

### 1.4 `sp500_tools/utils/config.py`

```python
"""Configuration and environment variable utilities."""
import os

def get_env(key: str, default: str | None = None, required: bool = False) -> str | None:
    """Get environment variable with optional validation."""
    value = os.environ.get(key)
    if value is None:
        if required and default is None:
            raise ValueError(f"Required environment variable {key} is not set")
        return default
    return value

def get_polygon_api_key() -> str | None:
    return get_env("POLYGON_API_KEY")

def get_polygon_s3_credentials() -> tuple[str | None, str | None]:
    return get_env("POLYGON_S3_ACCESS_KEY"), get_env("POLYGON_S3_SECRET_KEY")

def get_massive_api_key() -> str | None:
    return get_env("MASSIVE_API_KEY")

def get_database_url() -> str | None:
    return get_env("DATABASE_URL")
```

### 1.5 `sp500_tools/utils/csv_utils.py`

```python
"""Standardized CSV handling utilities."""
import csv
from pathlib import Path
import logging
from typing import Any

def get_existing_keys(
    filepath: Path,
    key_field: str,
    logger: logging.Logger | None = None,
) -> set[str]:
    """Get set of existing record keys from CSV file."""
    if not filepath.exists():
        return set()
    
    existing = set()
    try:
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if key_field in row and row[key_field]:
                    existing.add(row[key_field])
    except (OSError, csv.Error) as e:
        if logger:
            logger.warning(f"Could not read {filepath}: {e}")
    return existing

def append_csv(
    filepath: Path,
    data: list[dict[str, Any]],
    fieldnames: list[str],
    logger: logging.Logger | None = None,
) -> int:
    """Append rows to CSV file, creating if needed."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    file_exists = filepath.exists()
    mode = "a" if file_exists else "w"
    
    with open(filepath, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)
    
    if logger:
        logger.debug(f"{'Appended to' if file_exists else 'Created'} {filepath}: {len(data)} rows")
    return len(data)
```

### 1.6 `sp500_tools/utils/cli.py`

```python
"""Standardized CLI patterns."""
import argparse
from .dates import parse_date

def create_parser(description: str, epilog: str = "") -> argparse.ArgumentParser:
    """Create parser with consistent formatting."""
    return argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )

def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common arguments (verbose, continue)."""
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--continue", dest="continue_mode", action="store_true",
                        help="Resume interrupted operation")

def add_date_args(parser: argparse.ArgumentParser, default_years: int = 5) -> None:
    """Add date range arguments."""
    parser.add_argument("--start-date", type=parse_date, metavar="YYYY-MM-DD")
    parser.add_argument("--end-date", type=parse_date, metavar="YYYY-MM-DD")
    parser.add_argument("--years", type=int, metavar="N", default=default_years)

def add_api_key_arg(parser: argparse.ArgumentParser, env_var: str) -> None:
    """Add API key argument."""
    parser.add_argument("--api-key", help=f"API key (overrides {env_var})")
```

---

## Phase 2: Merge Downloaders

### 2.1 Massive API Scripts → `sp500_tools/downloaders/massive.py`

**Merge:** `download_fundamentals.py` + `download_economy_data.py` + `download_overviews.py`

```python
"""
Unified Massive API downloader.

Usage:
    python -m sp500_tools.downloaders.massive fundamentals --endpoint balance-sheets
    python -m sp500_tools.downloaders.massive economy --endpoints treasury-yields
    python -m sp500_tools.downloaders.massive overviews --symbols-file sp500.txt
"""

class MassiveClient:
    """Unified client for Massive API."""
    BASE_URL = "https://api.massive.com"
    
    def __init__(self, api_key: str, logger: logging.Logger):
        self.api_key = api_key
        self.logger = logger
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {api_key}"
    
    def fetch_paginated(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch all pages from an endpoint."""
        all_results = []
        url = f"{self.BASE_URL}{path}"
        
        while url:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") != "OK":
                raise ValueError(f"API error: {data.get('error', 'Unknown')}")
            
            all_results.extend(data.get("results", []))
            url = data.get("next_url")
            params = {}
        
        return all_results

def cmd_fundamentals(args, client, logger): ...
def cmd_economy(args, client, logger): ...
def cmd_overviews(args, client, logger): ...
```

**Reduction:** 1,984 lines → ~600 lines (70%)

### 2.2 Polygon REST Scripts → `sp500_tools/downloaders/polygon.py`

**Merge:** `ratio_downloader.py` + `check_trading_days.py`

```python
"""
Unified Polygon.io REST API downloader.

Usage:
    python -m sp500_tools.downloaders.polygon ratios --symbols AAPL MSFT
    python -m sp500_tools.downloaders.polygon trading-days --years 5
"""

class PolygonClient:
    BASE_URL = "https://api.polygon.io"
    
    def __init__(self, api_key: str, logger: logging.Logger):
        self.api_key = api_key
        self.logger = logger
    
    def get(self, path: str, params: dict = None) -> dict:
        params = params or {}
        params["apiKey"] = self.api_key
        response = requests.get(f"{self.BASE_URL}{path}", params=params, timeout=30)
        response.raise_for_status()
        return response.json()
```

**Reduction:** 640 lines → ~400 lines (37%)

### 2.3 Script Mapping

| Original | New Location |
|----------|--------------|
| `download_daily_prices.py` | `sp500_tools/downloaders/polygon_s3.py` |
| `download_sp500_symbols.py` | `sp500_tools/downloaders/symbols.py` |
| `load_csv_to_postgres.py` | `sp500_tools/database/loader.py` |
| `rebuild_database.py` | `sp500_tools/database/schema.py` |
| `combine_fundamentals.py` | `sp500_tools/processing/combine.py` |

---

## Phase 3: Package Configuration

### 3.1 Root `pyproject.toml`

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "sp500-tools"
version = "0.2.0"
description = "S&P 500 data download and processing tools"
requires-python = ">=3.10"
dependencies = [
    "requests>=2.28.0",
    "beautifulsoup4>=4.11.0",
    "boto3>=1.28.0",
    "psycopg2-binary>=2.9.0",
    "python-dateutil>=2.8.0",
]

[project.optional-dependencies]
dev = ["pytest>=7.0", "pytest-cov", "mypy>=1.0", "ruff>=0.1.0"]

[project.scripts]
sp500-massive = "sp500_tools.downloaders.massive:main"
sp500-polygon = "sp500_tools.downloaders.polygon:main"
sp500-prices = "sp500_tools.downloaders.polygon_s3:main"
sp500-symbols = "sp500_tools.downloaders.symbols:main"
sp500-load = "sp500_tools.database.loader:main"
sp500-schema = "sp500_tools.database.schema:main"
sp500-combine = "sp500_tools.processing.combine:main"

[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP"]

[tool.mypy]
python_version = "3.10"
warn_return_any = true
```

### 3.2 MCP Server `mcp_server/pyproject.toml`

```toml
[project]
name = "stock-data-mcp-server"
version = "0.2.0"
requires-python = ">=3.10"
dependencies = [
    "sp500-tools",      # Shared utilities
    "mcp>=1.6.0",
    "psycopg[binary]>=3.0",
    "pydantic>=2.0",
]
```

---

## Phase 4: Security Fixes

### 4.1 SQL Injection in `database/loader.py`

```python
# Before (VULNERABLE)
query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"

# After (SAFE)
from psycopg2 import sql
query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
    sql.Identifier(table_name),
    sql.SQL(', ').join(map(sql.Identifier, columns))
)
```

### 4.2 SQL Injection in `database/schema.py`

```python
# Before (VULNERABLE)
cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')

# After (SAFE)
from psycopg import sql
cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))
```

### 4.3 Read-Only Mode in MCP Server

```python
# mcp_server/database.py
def get_connection():
    conn = psycopg.connect(get_database_url(), row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
    return conn
```

---

## Phase 5: Type Hints Modernization

Update all files to Python 3.10+ syntax:

| Old | New |
|-----|-----|
| `List[str]` | `list[str]` |
| `Dict[str, Any]` | `dict[str, Any]` |
| `Optional[str]` | `str \| None` |
| `Tuple[int, int]` | `tuple[int, int]` |

---

## Implementation Checklist

### Week 1: Foundation
- [ ] Create `sp500_tools/` package structure
- [ ] Implement `utils/` modules
- [ ] Verify imports work

### Week 2: Migration
- [ ] Merge Massive API scripts
- [ ] Merge Polygon REST scripts
- [ ] Move remaining scripts

### Week 3: Integration
- [ ] Create root `pyproject.toml`
- [ ] Update MCP server dependency
- [ ] Apply security fixes
- [ ] Modernize type hints

### Week 4: Cleanup
- [ ] Delete old script files
- [ ] Update AGENTS.md
- [ ] Test installation

---

## Metrics

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Python files | 17 | 12 | -29% |
| Lines of code | ~5,500 | ~3,500 | -36% |
| Duplicate functions | 6 | 0 | -100% |
| Entry points | 0 | 7 | CLI commands |

---

## Command Mapping

| Old | New |
|-----|-----|
| `python download_fundamentals.py --endpoint X` | `sp500-massive fundamentals --endpoint X` |
| `python download_economy_data.py --years 5` | `sp500-massive economy --years 5` |
| `python download_overviews.py --symbols-file X` | `sp500-massive overviews --symbols-file X` |
| `python ratio_downloader.py AAPL MSFT` | `sp500-polygon ratios --symbols AAPL MSFT` |
| `python check_trading_days.py --years 5` | `sp500-polygon trading-days --years 5` |
| `python download_daily_prices.py 2024-01-02` | `sp500-prices 2024-01-02` |
| `python download_sp500_symbols.py` | `sp500-symbols` |
| `python load_csv_to_postgres.py --csv X --table Y` | `sp500-load --csv X --table Y` |
| `python rebuild_database.py` | `sp500-schema` |
| `python combine_fundamentals.py` | `sp500-combine` |

---

## Files to Delete After Migration

```
download_sp500_symbols.py
check_trading_days.py
download_daily_prices.py
download_fundamentals.py
download_economy_data.py
download_overviews.py
ratio_downloader.py
load_csv_to_postgres.py
rebuild_database.py
combine_fundamentals.py
```
