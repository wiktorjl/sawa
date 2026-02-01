# S&P 500 Data Downloader

Python package for downloading S&P 500 market data from Polygon.io API and storing it in PostgreSQL. Includes an MCP (Model Context Protocol) server for querying the data.

## Architecture

The project consists of two packages:

- **sp500-tools**: CLI tool for downloading and loading market data
- **mcp_server**: MCP server for querying data via LLM tools

```
.
├── sp500_tools/           # Main package
│   ├── cli.py             # CLI entry point (sp500 command)
│   ├── coldstart.py       # Full database setup workflow
│   ├── update.py          # Incremental update workflow
│   ├── api/               # Polygon REST and S3 clients
│   ├── database/          # PostgreSQL loader and schema management
│   ├── processing/        # Data processing utilities
│   └── utils/             # Shared utilities (logging, config, dates)
├── mcp_server/            # MCP server package
│   ├── server.py          # Server entry point
│   ├── database.py        # Database connection pool
│   └── tools/             # MCP tool implementations
└── sqlschema/             # PostgreSQL schema files (01-07)
```

## Prerequisites

- Python 3.10+
- PostgreSQL 12+
- Polygon.io API key (REST API access)
- Polygon.io S3 credentials (bulk data access)

## Installation

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install main package
pip install -e ".[dev]"

# Install MCP server
cd mcp_server && pip install -e ".[dev]" && cd ..
```

## Configuration

Set environment variables or use CLI flags:

| Variable | Description |
|----------|-------------|
| `POLYGON_API_KEY` | Polygon.io REST API key |
| `POLYGON_S3_ACCESS_KEY` | Polygon S3 access key |
| `POLYGON_S3_SECRET_KEY` | Polygon S3 secret key |
| `DATABASE_URL` | PostgreSQL connection URL (e.g., `postgresql://user:pass@host:5432/db`) |

A `.env` file in the project root is automatically loaded.

## Usage

### Cold Start (Full Setup)

Creates database schema, downloads all historical data, and loads into PostgreSQL:

```bash
# Download 5 years of data (default)
sp500 coldstart --years 5

# Use custom symbols file
sp500 coldstart --years 3 --symbols-file filter.txt

# Schema only (no data download)
sp500 coldstart --schema-only

# Load existing CSV files (skip download)
sp500 coldstart --skip-downloads

# Skip specific data types
sp500 coldstart --years 5 --skip-prices --skip-fundamentals
```

### Incremental Update

Pulls new data since the last update:

```bash
# Auto-detect last date from database
sp500 update

# Force update from specific date
sp500 update --from-date 2024-01-01
```

### Common Options

```bash
sp500 coldstart --verbose              # Debug logging
sp500 coldstart --output-dir ./mydata  # Custom output directory
sp500 coldstart --api-key YOUR_KEY     # Override env var
```

## MCP Server

The MCP server exposes PostgreSQL data through LLM-compatible tools.

### Starting the Server

```bash
export DATABASE_URL="postgresql://user:pass@host:5432/db"
python -m mcp_server.server
```

### Available Tools

| Tool | Description |
|------|-------------|
| `list_companies` | List S&P 500 companies with sector filter |
| `get_company_details` | Company info and latest metrics |
| `search_companies` | Search by name, ticker, or sector |
| `get_stock_prices` | Historical OHLCV data |
| `get_financial_ratios` | P/E, ROE, debt/equity, etc. |
| `get_fundamentals` | Balance sheet, cash flow, income statement |
| `get_economy_data` | Treasury yields, inflation, labor market |
| `get_economy_dashboard` | Economic summary |
| `execute_query` | Custom SQL (SELECT only) |

### Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_LOG_LEVEL` | info | Server log level |
| `MCP_MAX_ROWS` | 1000 | Maximum query rows |
| `MCP_QUERY_TIMEOUT` | 30 | Query timeout (seconds) |

## Database Schema

The schema consists of normalized tables:

- **companies**: S&P 500 company metadata (primary reference)
- **stock_prices**: Daily OHLCV prices
- **financial_ratios**: Time-series P/E, ROE, debt/equity, etc.
- **fundamentals**: Balance sheet, cash flow, income statement
- **economy tables**: Treasury yields, inflation, labor market data

Schema files in `sqlschema/` are applied in order (00-07).

## Development

```bash
# Linting
ruff check .
ruff check --fix .

# Type checking
mypy sp500_tools/
mypy mcp_server/

# Testing
pytest
pytest --cov=sp500_tools
```

## License

MIT
