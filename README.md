# Sawa -- AI-Powered Stock Market MCP Server

Ask questions about the stock market in plain English. Sawa is an [MCP server](https://modelcontextprotocol.io/) that gives AI assistants direct access to a PostgreSQL database of S&P 500 and NASDAQ market data -- prices, fundamentals, technicals, economic indicators, and more.

Connect it to Claude, ChatGPT, or any MCP-compatible client and just ask:

- *"Which S&P 500 stocks are trading below their 200-day moving average with RSI under 30?"*
- *"Compare Apple and Microsoft's revenue growth over the last 8 quarters"*
- *"Show me the top 10 dividend yield stocks in the healthcare sector"*
- *"What's the correlation between treasury yields and tech stock performance this year?"*
- *"Which stocks just crossed above their 150-day SMA on high volume?"*
- *"How's the market breadth looking today?"*

The AI translates your questions into the right tool calls and SQL queries automatically. No query language to learn -- just ask what you want to know.

### Example

> **You:** *"How's the market breadth looking today? What percentage of S&P 500 stocks are above their 50-day moving average?"*

The AI calls `get_market_breadth` and `get_sector_performance` behind the scenes, then responds:

```
Market breadth for the S&P 500 today (Feb 27, 2026):

  Advancers:  312  (62.4%)
  Decliners:  178  (35.6%)
  Unchanged:   10  ( 2.0%)
  A/D Ratio:  1.75

  Above 50-day SMA:   58.2%
  Above 200-day SMA:  64.8%

Breadth is moderately positive. The A/D ratio of 1.75 shows broad
participation in today's rally rather than a narrow move driven by
a few large caps. However, only 58% of stocks are above their 50-day
moving average, suggesting the short-term trend is mixed even as the
longer-term picture (65% above 200-day) remains constructive.

Sector leaders today:
  Communication Services  +1.84%
  Energy                  +1.22%
  Consumer Discretionary  +0.93%

Laggards:
  Financials              -0.67%
  Information Technology  -0.41%
```

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
sawa daily          # Run after market close -- prices, news, technical indicators, market internals
sawa weekly         # Economy, overviews, news, corporate actions, stock character classification
sawa quarterly      # Fundamentals (balance sheets, income, cash flow, ratios)
sawa doctor         # Check whether database contents look healthy
```

For unattended operation see `scripts/market_scheduler.sh` — a single
cron-driven script that manages intraday streaming during market hours and
runs `daily` / `weekly` after close, then runs scoped doctor checks before
marking those jobs complete. See [docs/MAINTENANCE.md](docs/MAINTENANCE.md)
for the full operational playbook.

## Prerequisites

- Python 3.10+
- PostgreSQL 12+
- [TA-Lib C library](https://ta-lib.org/) (`brew install ta-lib` on macOS, `apt install libta-lib-dev` on Ubuntu)
- [Polygon.io](https://polygon.io/) API key and S3 credentials

## Configuration

Copy `.env.example` to `.env` and fill in your credentials:

| Variable | Required | Description |
|----------|----------|-------------|
| `POLYGON_API_KEY` | yes | Polygon.io REST API key (prices, fundamentals, news, splits/dividends) |
| `POLYGON_S3_ACCESS_KEY` | yes | Polygon S3 access key (bulk historical price downloads) |
| `POLYGON_S3_SECRET_KEY` | yes | Polygon S3 secret key |
| `DATABASE_URL` | yes | PostgreSQL connection string |
| `FRED_API_KEY` | yes | FRED API key — required for market internals (VIX, VIX3M, HY spread). Pipeline alerts and skips this step if missing. |
| `NTFY_TOPIC` | no | ntfy.sh URL for pipeline push notifications |

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
sawa coldstart --schema-only               # DANGER: drops/recreates all tables; use throwaway DB
sawa coldstart --skip-downloads            # Load existing CSV files only
sawa coldstart --no-drop                   # Re-apply schema without destroying data (safe upgrade)

# Incremental updates
sawa daily                                 # Prices, news, technical indicators, market internals
sawa daily --from-date 2024-01-01          # Force update from specific date
sawa weekly                                # Economy, overviews, news, corporate actions, character
sawa weekly --skip-character               # Skip stock character classification
sawa quarterly                             # Fundamentals + financial ratios
sawa doctor --job daily                    # Database checks after daily/weekly jobs

# Intraday streaming (WebSocket, 15-min delayed)
sawa intraday                              # Stream 5-min bars

# Symbol management
sawa add-symbol TSLA COIN                  # Add tickers ad-hoc
sawa add-symbol --file data/nasdaq1000_symbols.txt --years 5

# Indices and screens
sawa index-list                            # List indices and constituent counts
sawa index-show sp500                      # Show index details
sawa index-update                          # Refresh constituents from Wikipedia
sawa ta-screen --rsi-max 30 --index sp500  # Run a TA screener

# Other
sawa character                             # Stock character classification (also runs in weekly)
sawa adjust-splits                         # Re-fetch adjusted prices after recent splits
sawa data-status                           # Show data freshness across price tables
sawa doctor                                # Validate database completeness/sanity
```

Full subcommand help: `sawa <command> --help`.

## Database Schema

Schema files in `sqlschema/` are applied in numeric order. Core tables:

- **companies** -- Company metadata (ticker, name, SIC code, market cap)
- **stock_prices** / **stock_prices_intraday** -- Daily OHLCV and 5-minute bars
- **financial_ratios** -- P/E, ROE, debt/equity, dividend yield
- **balance_sheets, income_statements, cash_flows** -- Quarterly/annual fundamentals
- **technical_indicators** -- SMA, RSI, MACD, Bollinger Bands, ATR
- **treasury_yields, inflation, labor_market** -- Economic indicators
- **market_internals** -- VIX, VIX3M, HY credit spread (FRED)
- **indices, index_constituents** -- S&P 500 and NASDAQ index membership
- **stock_splits, dividends, earnings** -- Corporate actions
- **news_articles, news_sentiment** -- News with per-ticker sentiment

## Data Sources

| Source | Auth | Provides |
|--------|------|----------|
| Polygon REST (`api.polygon.io`) | `POLYGON_API_KEY` | Daily prices, fundamentals, ratios, news + sentiment, splits, dividends, ticker details, treasury/inflation/labor (Polygon's `/fed/v1/*`) |
| Polygon S3 (`files.polygon.io`) | `POLYGON_S3_*` | Bulk historical daily OHLCV (used by `coldstart`; faster than REST for multi-year backfills) |
| Polygon WebSocket (`delayed.polygon.io`) | `POLYGON_API_KEY` | Live 5-min bars during market hours (15-min delayed on the basic tier) |
| FRED (`api.stlouisfed.org`) | `FRED_API_KEY` | Market internals: VIX (`VIXCLS`), VIX3M (`VXVCLS`), HY credit spread (`BAMLH0A0HYM2`) |
| Wikipedia | none | S&P 500 constituent list (HTML scrape) |
| `data/nasdaq1000_symbols.txt` | none | NASDAQ-5000 constituent list (bundled into the wheel) |
| `yfinance` (optional script) | none | Earnings dates via `scripts/populate_earnings.py` (Polygon's earnings endpoint currently returns no data) |

Technical indicators and the stock-character tables are **computed
locally** from `stock_prices` (TA-Lib) — they have no external source.

For the full mapping of every external endpoint, target table, and
loader module, see [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md).

## Project Structure

```
sawa/                        # Core data pipeline package
  api/                       # Polygon REST, S3, WebSocket clients + FRED
  calculation/               # Technical analysis engine (TA-Lib)
  database/                  # Schema, loaders (load.py / news / intraday / ta_load), connections
  domain/                    # Immutable dataclasses (StockPrice, CompanyInfo, etc.)
  repositories/              # Repository pattern with caching layer
  coldstart.py, daily.py,    # Pipeline entry points (invoked by `sawa <command>`)
  weekly.py, quarterly.py
mcp_server/                  # MCP server package
  charts/                    # Unicode chart rendering (plotext)
  tools/                     # 60+ MCP tool implementations
  services/                  # Service layer and domain converters
sqlschema/                   # PostgreSQL schema files (applied in numeric order)
scripts/                     # Shell wrappers, cron scheduler, ad-hoc backfills
data/                        # Local CSV cache + bundled symbol list
docs/                        # Operations and maintenance docs
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
