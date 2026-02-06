# Project: Sawa - S&P 500 Data Downloader

## Overview
Python package for downloading S&P 500 market data from Polygon.io API. Provides CLI tools for data ingestion and an MCP server for LLM integration.

## Project Structure
- `sawa/` - Main package (data acquisition, database updates, high-level functions)
- `mcp_server/` - MCP server (thin wrapper around sawa for LLM use)
- `tests/` - Test suite
- `sqlschema/` - SQL schema files

## Build/Lint/Test Commands

### Environment Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cd mcp_server && pip install -e ".[dev]"
```

### Linting and Type Checking
```bash
ruff check .              # Run linter
ruff check --fix .        # Auto-fix issues
mypy sawa/                # Type check main package
mypy mcp_server/          # Type check MCP server
```

### Testing
```bash
pytest                    # Run all tests
pytest tests/test_file.py # Run single test file
pytest -v                 # Verbose output
pytest -k test_name       # Run specific test by name
pytest --cov=sawa         # With coverage
```

### CLI Commands
```bash
sawa coldstart --years 5              # Full database setup
sawa coldstart --drop-only            # Drop tables only
sawa coldstart --schema-only          # Create schema only
sawa coldstart --skip-downloads       # Load existing CSV data
sawa daily                            # Daily price & news update (skips today before 5 PM ET)
sawa daily --from-date 2024-01-01     # Force update from date
sawa intraday                         # Stream live 5-min bars via WebSocket (15-min delayed)
sawa intraday --bar-size 15           # Stream 15-min bars
```

## Code Style Guidelines

### Python Version
- Minimum Python 3.10
- Use modern syntax: `list[T]` not `List[T]`, `T | None` not `Optional[T]`

### Imports
- Standard library first, then third-party, then local (enforced by ruff I rule)
- No wildcard imports
- Group with blank lines between groups

```python
import argparse
import logging
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup

from sawa.utils import setup_logging
```

### Formatting
- Line length: 100 characters
- Double quotes for strings
- 4-space indentation
- Trailing commas in multi-line structures

### Type Hints
- Use type hints for all function signatures
- Use `T | None` for nullable types
- Use `list[dict[str, Any]]` for result sets

```python
def fetch_data(ticker: str, start_date: date, limit: int = 100) -> list[dict[str, Any]]:
```

### Naming Conventions
- Functions/variables: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Private functions: `_leading_underscore`
- Module-level logger: `logger = logging.getLogger(__name__)`

### Docstrings
- Google-style with Args/Returns/Raises sections

```python
def save_symbols(symbols: list[str], output_file: str) -> None:
    """Save symbols to a text file atomically.

    Args:
        symbols: List of ticker symbols
        output_file: Path to output file

    Raises:
        IOError: If file write fails
    """
```

### Error Handling
- Catch specific exceptions, not bare `except:`
- Use `raise ... from e` to chain exceptions
- Log errors before re-raising
- Exit with `sys.exit(1)` on fatal CLI errors

```python
try:
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    logger.error(f"Network error: {e}")
    sys.exit(1)
```

### Database Queries
- Use `psycopg.sql` for safe identifier handling
- Never use f-strings for table/column names

```python
from psycopg import sql

query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
    sql.Identifier(table_name),
    sql.SQL(', ').join(map(sql.Identifier, columns))
)
```

### File I/O
- Use atomic writes with tempfile + os.replace
- Use `Path.mkdir(parents=True, exist_ok=True)`
- Use context managers (`with open(...) as f:`)

## Environment Variables
| Variable | Description |
|----------|-------------|
| `POLYGON_API_KEY` | Polygon.io REST API key |
| `POLYGON_S3_ACCESS_KEY` | Polygon S3 access key |
| `POLYGON_S3_SECRET_KEY` | Polygon S3 secret key |
| `DATABASE_URL` | PostgreSQL connection URL (MCP server) |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | PostgreSQL config (sawa) |

## Ruff Configuration
```toml
[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP"]
```
