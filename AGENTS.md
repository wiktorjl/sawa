# Project: Sawa - S&P 500 Data Downloader

## Overview
Python package for downloading S&P 500 market data from Polygon.io API, with a Terminal User Interface (TUI) for data exploration.

## Project Priorities

**Primary components (focus work here):**
1. **Backend data ingestion pipeline** (`sawa/`) - CLI tools for downloading and loading market data
2. **TUI application** (`tui/`) - Interactive terminal interface for exploring stock data

**Secondary/addon (only work on when explicitly requested):**
- **MCP server** (`mcp_server/`) - Model Context Protocol server for LLM integration

## Project Structure
```
.
├── pyproject.toml            # Root package (sawa)
├── sawa/                     # Main package
│   ├── __init__.py
│   ├── cli.py                # Main entry point (sawa command)
│   ├── coldstart.py          # Full database setup workflow
│   ├── update.py             # Incremental update workflow
│   ├── api/                  # API clients
│   │   ├── __init__.py
│   │   ├── client.py         # Polygon REST API client
│   │   └── s3.py             # Polygon S3 bulk data client
│   ├── utils/                # Shared utilities
│   │   ├── __init__.py
│   │   ├── logging.py        # setup_logging()
│   │   ├── dates.py          # parse_date(), calculate_date_range()
│   │   ├── config.py         # Environment variable handling
│   │   ├── symbols.py        # Symbol file loading + validation
│   │   ├── csv_utils.py      # CSV read/write helpers
│   │   └── cli.py            # Common argparse patterns
│   ├── database/             # Database utilities
│   │   ├── __init__.py
│   │   ├── loader.py         # CSV to PostgreSQL loader
│   │   ├── schema.py         # Database schema rebuild
│   │   └── connection.py     # Shared connection handling
│   └── processing/           # Data processing
│       ├── __init__.py
│       └── combine.py        # Combine fundamentals files
├── mcp_server/               # MCP server (independent package)
│   ├── pyproject.toml        # Depends on sawa
│   ├── server.py
│   ├── database.py
│   └── tools/
├── sqlschema/                # SQL schema files (01-07)
├── data/                     # Output directory
└── tests/                    # Test suite (future)
```

## Build/Lint/Test Commands

### Environment Setup
```bash
# Use .venv virtual environment (project convention)
python -m venv .venv
source .venv/bin/activate

# Install main package (editable mode)
pip install -e ".[dev]"

# Install MCP server
cd mcp_server && pip install -e ".[dev]"
```

### Linting and Type Checking
```bash
# Run ruff linter
ruff check .
ruff check --fix .         # Auto-fix issues

# Type checking with mypy
mypy sawa/
mypy mcp_server/
```

### Testing
```bash
# Run all tests
pytest

# Run single test file
pytest tests/test_database.py

# Run with verbose output
pytest -v

# Run with coverage
pytest --cov=sawa
```

### CLI Commands
After installation, the `sawa` command is available with two main workflows:

```bash
# Cold start: Full database setup from scratch
# - Drops existing tables
# - Creates schema from SQL files
# - Downloads all historical data (symbols, prices, fundamentals, overviews, economy, ratios)
# - Loads all data into PostgreSQL
sawa coldstart --years 5

# Mode options:
sawa coldstart --drop-only      # Drop tables only (keeps downloaded data)
sawa coldstart --schema-only    # Only create schema (no download/load)
sawa coldstart --load-only      # Only load existing CSV data (no schema)
sawa coldstart --skip-downloads # Create schema + load existing CSV data

# Skip specific downloads during cold start
sawa coldstart --years 5 --skip-prices
sawa coldstart --years 5 --skip-fundamentals --skip-economy
sawa coldstart --years 3 --skip-prices --skip-ratios --skip-overviews

# Use custom symbols file instead of fetching from Wikipedia
sawa coldstart --years 2 --symbols-file filter.txt

# Don't drop existing tables (useful for resuming)
sawa coldstart --years 5 --no-drop

# Incremental update: Pull new data since last update
sawa update

# Force update from specific date
sawa update --from-date 2024-01-01

# Common options for both commands
sawa coldstart --verbose               # Debug logging
sawa coldstart --output-dir ./mydata   # Custom output directory
sawa coldstart --schema-dir ./schema   # Custom schema directory

# Override environment variables via CLI
sawa coldstart --api-key YOUR_KEY --database-url postgresql://...

# Start MCP server
DATABASE_URL="postgresql://user:pass@host:5432/db" python -m mcp_server.server
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
from typing import Any

import requests
from bs4 import BeautifulSoup

from sawa.utils import setup_logging, load_symbols
```

### Formatting
- Line length: 100 characters (configured in pyproject.toml)
- Use double quotes for strings
- 4-space indentation
- Trailing commas in multi-line structures

### Type Hints
- Use type hints for function signatures
- Use `T | None` for nullable types (not `Optional[T]`)
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
- Use shared utility: `from sawa.utils import setup_logging`
- Log to stdout (not stderr) for main scripts
```python
logger = setup_logging(verbose=args.verbose)
```

### CLI Design
- Use shared utilities from `sawa.utils.cli`
- Support environment variables for API keys with CLI override
```python
from sawa.utils.cli import create_parser, add_common_args, add_date_args

parser = create_parser("Download data from API.", epilog="Examples...")
add_common_args(parser)
add_date_args(parser)
```

### Database Queries
- Use `psycopg2.sql` or `psycopg.sql` for safe identifier handling
- Never use f-strings for table/column names
- Set query timeouts
```python
from psycopg2 import sql

query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
    sql.Identifier(table_name),
    sql.SQL(', ').join(map(sql.Identifier, columns))
)
```

### File I/O
- Use atomic writes with tempfile + os.replace for critical files
- Create parent directories with `Path.mkdir(parents=True, exist_ok=True)`
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
| `POLYGON_API_KEY` | Polygon.io REST API key | sawa |
| `POLYGON_S3_ACCESS_KEY` | Polygon S3 access key | sawa |
| `POLYGON_S3_SECRET_KEY` | Polygon S3 secret key | sawa |
| `DATABASE_URL` | PostgreSQL connection URL | mcp_server |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | PostgreSQL config | sawa |
| `MCP_LOG_LEVEL` | Server log level (default: info) | mcp_server |
| `MCP_MAX_ROWS` | Max query rows (default: 1000) | mcp_server |
| `MCP_QUERY_TIMEOUT` | Query timeout secs (default: 30) | mcp_server |

## Dependencies

### Main Package (sawa)
- `requests>=2.28.0` - HTTP client
- `beautifulsoup4>=4.11.0` - HTML parsing
- `boto3>=1.28.0` - S3 client for Polygon bulk files
- `psycopg2-binary>=2.9.0` - PostgreSQL driver (sync)
- `psycopg[binary]>=3.0` - PostgreSQL driver (async)
- `python-dateutil>=2.8.0` - Date utilities

### MCP Server
- `sawa` - Shared utilities
- `mcp>=1.6.0` - MCP SDK
- `psycopg[binary]>=3.0` - PostgreSQL driver
- `pydantic>=2.0` - Data validation

## Ruff Configuration
From `pyproject.toml`:
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
