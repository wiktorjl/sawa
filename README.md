# Sawa

Ask questions about the stock market in plain English. Sawa is an [MCP server](https://modelcontextprotocol.io/) that gives AI assistants direct access to a PostgreSQL database of S&P 500 and NASDAQ market data -- prices, fundamentals, technicals, economic indicators, and more.

Connect it to Claude, ChatGPT, or any MCP-compatible client and just ask:

- *"Which S&P 500 stocks are trading below their 200-day moving average with RSI under 30?"*
- *"Compare Apple and Microsoft's revenue growth over the last 8 quarters"*
- *"Show me the top 10 dividend yield stocks in the healthcare sector"*
- *"What's the correlation between treasury yields and tech stock performance this year?"*
- *"Which stocks just crossed above their 150-day SMA on high volume?"*

The AI translates your questions into the right tool calls and SQL queries automatically. No query language to learn -- just ask what you want to know.

## How It Works

Sawa has two parts:

1. **Data pipeline** (`sawa` CLI) -- Downloads market data from [Polygon.io](https://polygon.io/) into PostgreSQL and keeps it current with daily/weekly/quarterly update jobs.

2. **MCP server** (`stock-data-mcp-server`) -- Exposes 60+ specialized tools that AI assistants call to answer your questions. The AI picks the right tool, passes the right parameters, and interprets the results for you.

The MCP server doesn't just run raw SQL. It provides structured tools for common analysis patterns -- screeners, technical indicators, chart pattern detection, support/resistance levels, earnings calendars, sector comparisons -- so the AI can answer complex questions in a single step instead of piecing together multiple queries.

## What You Can Ask About

| Domain | Examples |
|--------|----------|
| **Prices & Charts** | Daily/intraday OHLCV, live quotes, YTD returns, weekly/monthly candles |
| **Technical Analysis** | SMA/EMA crossovers, RSI, MACD, Bollinger Bands, squeeze indicators, momentum (ADX, Stochastic, Williams %R), support/resistance levels, candlestick and chart pattern detection |
| **Fundamentals** | Balance sheets, income statements, cash flows, financial ratios (P/E, ROE, debt/equity), multi-quarter trends |
| **Screening** | Multi-criteria stock screener, 52-week highs/lows, volume leaders, daily range leaders, top gainers/losers |
| **Market Overview** | Sector performance, market breadth (advance/decline), relative strength vs benchmark |
| **Economy** | Treasury yield curves, CPI/PCE inflation, labor market data, inflation expectations |
| **Corporate Actions** | Dividend history and calendar, stock splits, earnings calendar with surprise data |
| **News** | Recent articles with per-ticker sentiment analysis |
| **Custom Queries** | Direct read-only SQL against the full database when the built-in tools aren't enough |

## Quick Start

### 1. Set Up the Database

```bash
python -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"
cd mcp_server && pip install -e ".[dev]" && cd ..

cp .env.example .env   # Fill in Polygon.io and PostgreSQL credentials

sawa coldstart --years 5   # Download 5 years of historical data
```

### 2. Connect the MCP Server

Add to your AI client's MCP configuration (e.g. Claude Desktop, Claude Code):

```json
{
  "mcpServers": {
    "stock-data": {
      "type": "stdio",
      "command": "/path/to/sawa/.venv/bin/python",
      "args": ["-m", "mcp_server.server"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/db"
      }
    }
  }
}
```

Then just start asking questions.

### 3. Keep Data Fresh

```bash
sawa daily          # Run after market close -- prices, news, technical indicators
sawa weekly         # Economy data, corporate actions
sawa quarterly      # Fundamentals (balance sheets, income, cash flow)
```

## Prerequisites

- Python 3.10+
- PostgreSQL 12+
- [TA-Lib C library](https://ta-lib.org/) (`brew install ta-lib` on macOS, `apt install libta-lib-dev` on Ubuntu)
- [Polygon.io](https://polygon.io/) API key and S3 credentials

## Configuration

Copy `.env.example` to `.env` and fill in your credentials:

| Variable | Description |
|----------|-------------|
| `POLYGON_API_KEY` | Polygon.io REST API key |
| `POLYGON_S3_ACCESS_KEY` | Polygon S3 access key |
| `POLYGON_S3_SECRET_KEY` | Polygon S3 secret key |
| `DATABASE_URL` | PostgreSQL connection string |

MCP server options (optional):

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_LOG_LEVEL` | `info` | Log level |
| `MCP_MAX_ROWS` | `1000` | Maximum query result rows |
| `MCP_QUERY_TIMEOUT` | `30` | Query timeout in seconds |

## CLI Reference

```bash
# Initial setup
sawa coldstart --years 5                   # Full setup with 5 years of data
sawa coldstart --schema-only               # Apply schema without downloading data
sawa coldstart --skip-downloads            # Load existing CSV files only

# Incremental updates
sawa daily                                 # Prices, news, technical indicators
sawa daily --from-date 2024-01-01          # Force update from specific date
sawa weekly                                # Economy, corporate actions
sawa quarterly                             # Fundamentals

# Intraday streaming
sawa intraday                              # Stream 5-min bars (15-min delayed)

# Add symbols
sawa add-symbol TSLA                       # Add a single ticker
sawa add-symbol --file symbols.txt --years 5
```

## Database Schema

Schema files in `sqlschema/` are applied in numeric order. Core tables:

- **companies** -- Company metadata (ticker, name, SIC code, market cap)
- **stock_prices** / **stock_prices_intraday** -- Daily OHLCV and 5-minute bars
- **financial_ratios** -- P/E, ROE, debt/equity, dividend yield
- **balance_sheets, income_statements, cash_flows** -- Quarterly/annual fundamentals
- **technical_indicators** -- SMA, RSI, MACD, Bollinger Bands, ATR
- **treasury_yields, inflation, labor_market** -- Economic indicators
- **indices, index_constituents** -- S&P 500 and NASDAQ index membership
- **stock_splits, dividends, earnings** -- Corporate actions
- **news_articles, news_sentiment** -- News with per-ticker sentiment

## Project Structure

```
sawa/                        # Core data pipeline package
  api/                       # Polygon REST, S3, and WebSocket clients
  calculation/               # Technical analysis engine (TA-Lib)
  database/                  # PostgreSQL schema, loaders, connections
  domain/                    # Immutable dataclasses (StockPrice, CompanyInfo, etc.)
  repositories/              # Repository pattern with caching layer
mcp_server/                  # MCP server package
  charts/                    # Unicode chart rendering (plotext)
  tools/                     # 60+ MCP tool implementations
  services/                  # Service layer and domain converters
sqlschema/                   # PostgreSQL schema files (applied in order)
tests/                       # Test suite
```

## Development

```bash
ruff check .                 # Lint
mypy sawa/                   # Type check core
mypy mcp_server/             # Type check MCP server
pytest                       # Run tests
pytest --cov=sawa            # With coverage
```

## License

MIT
