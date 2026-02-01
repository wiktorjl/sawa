# Project: S&P 500 Data Downloader

## Overview
Python scripts for downloading S&P 500 market data from Polygon.io API and a Model Context Protocol (MCP) server for querying the data stored in PostgreSQL.

## Project Structure
```
.
├── data/                      # Output data directory
├── mcp_server/               # MCP server package
│   ├── server.py             # Main server entry point
│   ├── database.py           # Database connection utilities
│   ├── tools/                # MCP tool implementations
│   └── pyproject.toml        # Server dependencies
├── download_sp500_symbols.py # Fetch S&P 500 constituents
├── check_trading_days.py     # Find trading days in date range
├── download_daily_prices.py  # Download OHLC prices from S3
├── download_fundamentals.py  # Download financial statements
├── download_economy_data.py  # Download economic indicators
├── load_csv_to_postgres.py   # Load CSVs into PostgreSQL
└── requirements.txt          # Main script dependencies
```

## Build/Lint/Test Commands

### Environment Setup
```bash
# Use .venv virtual environment (project convention)
python -m venv .venv
source .venv/bin/activate

# Install main dependencies
pip install -r requirements.txt

# Install MCP server (editable mode)
cd mcp_server && pip install -e ".[dev]"
```

### Linting and Type Checking
```bash
# Run ruff linter (configured in mcp_server/pyproject.toml)
ruff check .
ruff check --fix .         # Auto-fix issues

# Type checking with mypy
mypy mcp_server/
```

### Testing
```bash
# Run all tests
pytest

# Run single test file
pytest tests/test_database.py

# Run single test function
pytest tests/test_database.py::test_validate_select_query

# Run with verbose output
pytest -v

# Run with coverage
pytest --cov=mcp_server
```

### Running Scripts
```bash
# Download S&P 500 symbols
python download_sp500_symbols.py -o data/sp500_symbols.txt

# Find trading days
POLYGON_API_KEY=xxx python check_trading_days.py --years 5

# Download daily prices
POLYGON_S3_ACCESS_KEY=xxx POLYGON_S3_SECRET_KEY=xxx \
  python download_daily_prices.py 2024-01-02 --end-date 2024-12-31

# Start MCP server
DATABASE_URL="postgresql://user:pass@host:5432/db" \
  python -m mcp_server.server
```

## Code Style Guidelines

### Python Version
- Minimum Python 3.10 (see pyproject.toml)
- Use modern Python syntax: `list[T]` instead of `List[T]`, `dict[K, V]` instead of `Dict[K, V]`

### Imports
- Standard library imports first, then third-party, then local (enforced by ruff I rule)
- No wildcard imports
- Group imports logically with blank lines between groups
```python
import argparse
import logging
import os
import sys
from datetime import date, datetime
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup

from database import execute_query
```

### Formatting
- Line length: 100 characters (configured in pyproject.toml)
- Use double quotes for strings
- 4-space indentation
- Trailing commas in multi-line structures

### Type Hints
- Use type hints for function signatures
- Use `Optional[T]` or `T | None` for nullable types
- Use `list[dict[str, Any]]` for result sets
```python
def fetch_data(
    ticker: str,
    start_date: date,
    limit: int = 100,
) -> list[dict[str, Any]]:
```

### Naming Conventions
- Functions/variables: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Private functions: `_leading_underscore`
- Module-level logger: `logger = logging.getLogger(__name__)`

### Docstrings
- Use Google-style docstrings with Args/Returns/Raises sections
- Module docstrings at top of file with usage examples
```python
def save_symbols(symbols: list[str], output_file: str, logger: logging.Logger) -> None:
    """
    Save symbols to a text file atomically, one per line.

    Args:
        symbols: List of ticker symbols
        output_file: Path to output file
        logger: Logger instance

    Raises:
        IOError: If file write fails
    """
```

### Error Handling
- Catch specific exceptions, not bare `except:`
- Use `raise ... from e` to chain exceptions
- Log errors with appropriate level before re-raising
- Exit with sys.exit(1) on fatal errors in CLI scripts
```python
try:
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    logger.error(f"Network error: {e}")
    sys.exit(1)
```

### Logging
- Create dedicated logger per module: `logger = logging.getLogger(__name__)`
- Use structured format: `%(asctime)s [%(levelname)s] %(message)s`
- Support `-v/--verbose` flag for DEBUG level
- Log to stdout (not stderr) for main scripts
```python
def setup_logging(verbose: bool = False) -> logging.Logger:
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)
```

### CLI Design (argparse)
- Use RawDescriptionHelpFormatter for formatted epilog
- Include usage examples in epilog
- Support environment variables for API keys with CLI override
- Common flags: `-o/--output`, `-v/--verbose`, `--continue`
```python
parser = argparse.ArgumentParser(
    description="Download data from API.",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  %(prog)s --years 5
  %(prog)s --start-date 2020-01-01
""",
)
```

### Database Queries
- Use parameterized queries with `%(param)s` syntax
- Validate SELECT-only for user-provided SQL
- Set query timeouts
- Return results as list of dicts
```python
sql = """
    SELECT ticker, name FROM companies
    WHERE ticker = %(ticker)s
    LIMIT %(limit)s
"""
results = execute_query(sql, {"ticker": "AAPL", "limit": 10})
```

### File I/O
- Use atomic writes with tempfile + os.replace for critical files
- Create parent directories with `os.makedirs(path, exist_ok=True)`
- Use Path objects from pathlib for path manipulation
- Use context managers (`with open(...) as f:`)

## MCP Server

### Tools Available
- `list_companies` - List S&P 500 companies with sector filter
- `get_company_details` - Company info + latest metrics
- `search_companies` - Search by name/ticker/sector
- `get_stock_prices` - Historical OHLCV data
- `get_financial_ratios` - P/E, ROE, debt/equity, etc.
- `get_fundamentals` - Balance sheet, cash flow, income statement
- `get_economy_data` - Treasury yields, inflation, labor market
- `get_economy_dashboard` - Economic summary
- `execute_query` - Custom SQL (SELECT only)

### Running the Server
```bash
cd mcp_server
pip install -e .
export DATABASE_URL="postgresql://user:pass@host:5432/stock_data"
python -m mcp_server.server
```

## Environment Variables
| Variable | Description | Used By |
|----------|-------------|---------|
| `POLYGON_API_KEY` | Polygon.io REST API key | check_trading_days.py |
| `POLYGON_S3_ACCESS_KEY` | Polygon S3 access key | download_daily_prices.py |
| `POLYGON_S3_SECRET_KEY` | Polygon S3 secret key | download_daily_prices.py |
| `MASSIVE_API_KEY` | Massive API key | download_fundamentals.py |
| `DATABASE_URL` | PostgreSQL connection URL | mcp_server |
| `MCP_LOG_LEVEL` | Server log level (default: info) | mcp_server |
| `MCP_MAX_ROWS` | Max query rows (default: 1000) | mcp_server |
| `MCP_QUERY_TIMEOUT` | Query timeout secs (default: 30) | mcp_server |

## Dependencies
- `requests>=2.28.0` - HTTP client
- `beautifulsoup4>=4.11.0` - HTML parsing
- `boto3>=1.28.0` - S3 client for Polygon bulk files
- `psycopg2-binary>=2.9.0` - PostgreSQL driver (main scripts)
- `mcp>=1.6.0` - MCP SDK (server)
- `psycopg[binary]>=3.0` - PostgreSQL driver (server, async)
- `pydantic>=2.0` - Data validation (server)

## Ruff Configuration
From `mcp_server/pyproject.toml`:
```toml
[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP"]
# E: pycodestyle errors
# F: pyflakes
# I: isort
# N: pep8-naming
# W: pycodestyle warnings
# UP: pyupgrade (modern Python syntax)
```
