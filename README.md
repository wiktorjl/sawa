# Sawa

Stock market data pipeline and analysis toolkit. Downloads, stores, and serves S&P 500 and NASDAQ data from [Polygon.io](https://polygon.io/) via PostgreSQL.

## Components

- **sawa** -- CLI tool for data acquisition, schema management, and incremental updates
- **stock-data-mcp-server** -- [MCP](https://modelcontextprotocol.io/) server exposing the database to LLMs with 60+ tools, charting, and technical analysis

```
sawa/                        # Core package
  api/                       # Polygon REST, S3, and WebSocket clients
  calculation/               # Technical analysis engine (TA-Lib)
  database/                  # PostgreSQL schema, loaders, connections
  domain/                    # Immutable dataclasses (StockPrice, CompanyInfo, etc.)
  processing/                # Data transformation utilities
  repositories/              # Repository pattern with caching layer
  utils/                     # Config, logging, date parsing, market hours
mcp_server/                  # MCP server package
  charts/                    # Unicode chart rendering (plotext)
  services/                  # Service layer and domain converters
  tools/                     # 60+ MCP tool implementations
sqlschema/                   # PostgreSQL schema files (applied in order)
scripts/                     # One-off data population scripts
tests/                       # Test suite
```

## Prerequisites

- Python 3.10+
- PostgreSQL 12+
- [TA-Lib C library](https://ta-lib.org/) (`brew install ta-lib` on macOS, `apt install libta-lib-dev` on Ubuntu)
- [Polygon.io](https://polygon.io/) API key and S3 credentials

## Installation

```bash
python -m venv .venv
source .venv/bin/activate

# Core package
pip install -e ".[dev]"

# MCP server
cd mcp_server && pip install -e ".[dev]" && cd ..
```

## Configuration

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `POLYGON_API_KEY` | Polygon.io REST API key |
| `POLYGON_S3_ACCESS_KEY` | Polygon S3 access key |
| `POLYGON_S3_SECRET_KEY` | Polygon S3 secret key |
| `DATABASE_URL` | PostgreSQL connection string |

## Usage

### Cold Start (Full Setup)

Downloads all historical data and loads into PostgreSQL:

```bash
sawa coldstart --years 5                   # Full setup with 5 years of data
sawa coldstart --years 3 --symbols-file custom.txt
sawa coldstart --schema-only               # Apply schema without downloading data
sawa coldstart --skip-downloads            # Load existing CSV files only
sawa coldstart --drop-only                 # Drop all tables (requires confirmation)
```

### Incremental Updates

```bash
sawa daily                                 # Prices, news, technical indicators
sawa daily --from-date 2024-01-01          # Force update from specific date
sawa weekly                                # Economy, overviews, corporate actions
sawa quarterly                             # Fundamentals (balance sheets, income, cash flow)
```

### Intraday Streaming

Stream 5-minute bars via Polygon WebSocket (15-minute delayed):

```bash
sawa intraday                              # Start streaming
sawa intraday --bar-size 15                # 15-minute bars
```

### Adding Symbols

```bash
sawa add-symbol TSLA                       # Add a single ticker
sawa add-symbol --file symbols.txt --years 5
```

## MCP Server

The MCP server provides read-only access to the database with visualization support.

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/db"
python -m mcp_server.server
```

### Tool Categories

| Category | Tools |
|----------|-------|
| Companies | list, search, details, index membership |
| Market Data | prices, intraday bars, live prices, YTD returns |
| Technical Analysis | indicators, crossovers, support/resistance, candlestick/chart patterns, squeeze, momentum |
| Fundamentals | balance sheets, income statements, cash flows, financial ratios |
| Screeners | multi-criteria stock screener, 52-week extremes, volume leaders, daily range |
| Market Overview | top movers, market breadth, sector performance |
| Economy | treasury yields, inflation, labor market, dashboard |
| Corporate Actions | dividends, stock splits, earnings calendar |
| News | recent articles with sentiment analysis |
| Database | custom SQL queries (SELECT only), schema introspection |

### Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_LOG_LEVEL` | `info` | Log level |
| `MCP_MAX_ROWS` | `1000` | Maximum query result rows |
| `MCP_QUERY_TIMEOUT` | `30` | Query timeout in seconds |

## Database Schema

Schema files in `sqlschema/` are applied in numeric order. Core tables:

- **companies** -- Company metadata (ticker, name, SIC code, market cap)
- **stock_prices** -- Daily OHLCV prices
- **stock_prices_intraday** -- 5-minute intraday bars
- **financial_ratios** -- P/E, ROE, debt/equity, dividend yield
- **balance_sheets, income_statements, cash_flows** -- Quarterly/annual fundamentals
- **technical_indicators** -- SMA, RSI, MACD, Bollinger Bands, ATR
- **treasury_yields, inflation, labor_market** -- Economic indicators
- **indices, index_constituents** -- S&P 500 and NASDAQ index membership
- **stock_splits, dividends, earnings** -- Corporate actions
- **news_articles, news_sentiment** -- News with per-ticker sentiment

## Development

```bash
ruff check .                 # Lint
ruff check --fix .           # Auto-fix
mypy sawa/                   # Type check core
mypy mcp_server/             # Type check MCP server
pytest                       # Run tests
pytest --cov=sawa            # With coverage
```

## License

MIT
