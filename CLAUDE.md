# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

"Sawa" is a multi-component Python project for downloading, storing, and analyzing S&P 500 stock market data. It consists of three main packages:

1. **sawa** - Core CLI tool for data acquisition and processing
2. **sawa-tui** - Bloomberg-style terminal UI for browsing data
3. **stock-data-mcp-server** - MCP server for LLM integration

Data is sourced from Polygon.io and stored in PostgreSQL with normalized schema.

## Development Setup

```bash
# Create and activate virtual environment (always use .venv path)
python -m venv .venv
source .venv/bin/activate

# Install packages in development mode
pip install -e ".[dev]"
cd tui && pip install -e ".[dev]" && cd ..
cd mcp_server && pip install -e ".[dev]" && cd ..
```

## Build and Test Commands

```bash
# Linting
ruff check .
ruff check --fix .

# Type checking
mypy sawa/
mypy mcp_server/
mypy tui/sawa_tui/

# Run tests
pytest
pytest tests/domain/  # Test specific module
pytest --cov=sawa    # With coverage

# Before starting servers, check if ports are in use
lsof -i :5432  # PostgreSQL
lsof -i :8000  # If running any web server
```

## CLI Commands

The main CLI is `sawa` (entry point: sawa/cli.py):

```bash
# Full database setup from scratch (downloads all historical data)
sawa coldstart --years 5
sawa coldstart --years 3 --symbols-file custom.txt
sawa coldstart --schema-only           # Schema setup without data
sawa coldstart --skip-downloads        # Load existing CSV files
sawa coldstart --load-only             # Load data without schema changes
sawa coldstart --drop-only             # Clean database and exit

# Incremental updates
sawa daily                             # Daily prices, news, technical indicators
sawa daily --from-date 2024-01-01      # Force update from specific date
sawa weekly                            # Economy, overviews, news, corporate actions
sawa quarterly                         # Fundamentals (balance sheets, income, cash flow)

# Common options
sawa coldstart --verbose              # Debug logging
sawa coldstart --output-dir ./custom  # Custom data directory
```

## Running the TUI

```bash
cd tui
python -m sawa_tui.app
# Or if installed: sawa-tui
```

Entry point: tui/sawa_tui/app.py

## Running the MCP Server

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/db"
python -m mcp_server.server
```

Entry point: mcp_server/server.py

## Architecture

### Package: sawa (core)

Repository pattern with pluggable data providers:

- **domain/** - Immutable dataclasses (StockPrice, CompanyInfo, FinancialRatio, etc.)
- **repositories/** - Data access layer with provider abstraction
  - `factory.py` - RepositoryFactory for dependency injection
  - `base.py` - Abstract base classes for repositories
  - `database.py` - PostgreSQL implementations
  - `polygon_prices.py` - Polygon.io API client
  - `cache.py` - In-memory caching layer
- **api/** - External API clients (Polygon REST and S3)
- **database/** - PostgreSQL schema management and loaders
- **processing/** - Data transformation utilities
- **utils/** - Logging, config, date parsing, symbol validation

Key patterns:
- All domain models are frozen dataclasses with `__slots__`
- Repository pattern isolates data sources (database vs API)
- Factory pattern for creating repositories with caching
- Rate limiting for API calls

### Package: sawa-tui

**NOTE: The TUI package has been removed from the codebase.** It was deleted in commit 6e56114.
TUI-specific database tables (watchlists, glossary, users) were cleaned up via migration script 16_cleanup.sql.

### Package: stock-data-mcp-server

MCP server exposing PostgreSQL data via LLM tools:

- **server.py** - MCP server setup and tool registration
- **tools/** - Tool implementations (companies, market_data, fundamentals, economy)
- **services/** - Service layer with converters for domain models
- **charts/** - Unicode chart rendering with plotext
  - **renderers/** - Chart renderers for prices, economy, fundamentals
  - **themes/** - Color themes (osaka_jade, mono)
  - **widgets/** - Reusable chart components (gauge, trend, table)
- **database.py** - Connection pooling for PostgreSQL

The MCP server provides read-only access to data with visualization support.

## Database Schema

PostgreSQL schema in `sqlschema/` (applied in numeric order):

**Core Schema:**
- **00_setup.sql** - Documentation (expected tables list)
- **01_companies.sql** - Company metadata (primary reference table)
- **02_market_data.sql** - Daily OHLCV stock prices and financial ratios
- **03_fundamentals.sql** - Balance sheets, income statements, cash flows
- **04_economy.sql** - Treasury yields, inflation, labor market indicators
- **05_indexes.sql** - Performance indexes on all tables
- **06_views.sql** - Common views (company summary, economy dashboard, fundamentals, sectors)
- **07_procedures.sql** - CSV loading stored procedures

**Extended Schema:**
- **08_sic_gics_mapping.sql** - SIC to GICS sector classification table
- **09_sic_gics_data.sql** - Seed data for SIC/GICS mappings (~180 rows)
- **10_news.sql** - News articles, article-ticker associations, sentiment analysis
- **11_technical_indicators.sql** - Technical analysis indicators and metadata
- **12_indices.sql** - Market indices (S&P 500, NASDAQ 100) and constituents
- **13_gics_sector_function.sql** - get_gics_sector() lookup function
- **14_52week_extremes.sql** - Materialized view for 52-week highs/lows

**Migrations:**
- **16_cleanup.sql** - Drops old TUI tables (watchlists, users, glossary, company_overviews)
- **17_extended_sma.sql** - Adds sma_100/150/200, ema_100/200 to technical_indicators
- **18_corporate_actions.sql** - Stock splits, dividends, earnings tables
- **19_earnings_yfinance.sql** - Adds surprise_pct to earnings, changes constraints
- **20_drop_revenue_estimate.sql** - Drops revenue_estimate column from earnings
- **21_intraday_prices.sql** - Intraday price data (5-minute bars)
- **22_views_advanced.sql** - Advanced views (v_company_with_indices, stock_prices_live)

**Note:** Files are applied in numeric order. Migration files (16+) contain ALTER statements
and assume base schema (01-15) exists. File 15 does not exist (gap in numbering).

Data directory structure:
- `data/prices/` - OHLCV CSV files by ticker
- `data/fundamentals/` - Financial statement data
- `data/economy/` - Economic indicator data
- `data/overviews/` - Company overview data
- `data/ratios/` - Financial ratio data

## Configuration

Environment variables (use .env file):

```bash
# Required for data download
POLYGON_API_KEY=your_api_key
POLYGON_S3_ACCESS_KEY=your_s3_access
POLYGON_S3_SECRET_KEY=your_s3_secret

# Required for database operations
DATABASE_URL=postgresql://user:pass@localhost:5432/db

# Optional MCP server settings
MCP_LOG_LEVEL=info          # Log level (default: info)
MCP_MAX_ROWS=1000           # Max query results (default: 1000)
MCP_QUERY_TIMEOUT=30        # Query timeout seconds (default: 30)
```

## Key Implementation Patterns

### Repository Pattern Usage

```python
from sawa.repositories import get_factory

# Get global factory (configured from environment)
factory = get_factory()

# Get repositories
price_repo = factory.get_price_repository()
company_repo = factory.get_company_repository()

# Fetch data
prices = price_repo.get_prices("AAPL", start_date, end_date)
```

### Domain Models

All models in `sawa/domain/models.py` are immutable:
- Use `frozen=True` and `slots=True`
- Tickers are normalized to uppercase in `__post_init__`
- Use Decimal for financial values, not float

### Service Layer (MCP Server)

Service layer in mcp_server/services/ converts between domain models and API responses. Never return domain models directly from MCP tools - use converters.

### Testing

Tests use mock repositories (tests/repositories/mocks.py) to avoid database dependencies. When writing tests:
- Mock repositories via factory.set_factory()
- Use frozen dataclasses for test data
- Reset factory after tests with factory.reset_factory()

## Common Tasks

### Adding a New Data Type

1. Define domain model in `sawa/domain/models.py`
2. Create repository interface in `sawa/repositories/base.py`
3. Implement database repository in `sawa/repositories/database.py`
4. Add factory method in `sawa/repositories/factory.py`
5. Update schema in `sqlschema/`
6. Add loader in `sawa/database/`
7. Add download logic in `sawa/coldstart.py` or `sawa/update.py`

### Adding an MCP Tool

1. Implement tool function in `mcp_server/tools/`
2. Register in `mcp_server/server.py` @app.list_tools()
3. Add handler in @app.call_tool()
4. Create service layer function if needed in `mcp_server/services/`
5. Add chart renderer in `mcp_server/charts/renderers/` if visualization needed

### Adding a TUI View

1. Create view module in `tui/sawa_tui/views/`
2. Add view enum to `state.py` View class
3. Implement render function following pattern in `views/base.py`
4. Add key handler in `app.py` _handle_key()
5. Update status bar in `views/__init__.py` render_app()
