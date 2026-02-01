# Stock Data MCP Server

A Model Context Protocol (MCP) server providing read-only access to S&P 500 stock data stored in PostgreSQL.

## Overview

This MCP server exposes stock market data through standardized MCP tools, allowing AI assistants like Claude to query:
- Company information and fundamentals
- Historical stock prices and financial ratios
- Economic indicators (treasury yields, inflation, labor market)
- Financial statements (balance sheets, cash flows, income statements)

## Architecture

### Database Schema

```
companies (ticker PK)
    |
    |--< stock_prices (ticker FK, date)
    |--< financial_ratios (ticker FK, date)
    |--< balance_sheets (ticker FK, period_end, timeframe)
    |--< cash_flows (ticker FK, period_end, timeframe)
    |--< income_statements (ticker FK, period_end, timeframe)

economy tables (independent)
    |-- treasury_yields (date PK)
    |-- inflation (date PK)
    |-- inflation_expectations (date PK)
    |-- labor_market (date PK)

views
    |-- v_company_summary
    |-- v_economy_dashboard
    |-- v_latest_fundamentals
    |-- v_sector_summary
```

### Server Structure

```
mcp_server/
├── server.py          # FastMCP server with stdio transport
├── database.py        # PostgreSQL connection pool
├── pyproject.toml     # Dependencies
└── tools/
    ├── __init__.py
    ├── companies.py   # Company queries
    ├── market_data.py # Prices and ratios
    ├── fundamentals.py # Financial statements
    └── economy.py     # Economic indicators
```

## Tools

### 1. `list_companies`

Lists all active companies with optional filtering.

**Parameters:**
- `limit` (int, optional): Maximum number of results (default: 100, max: 1000)
- `offset` (int, optional): Number of results to skip (default: 0)
- `sector` (str, optional): Filter by SIC description (partial match)

**Returns:** Array of company objects with ticker, name, market_cap, sector

### 2. `get_company_details`

Get full company information including latest metrics.

**Parameters:**
- `ticker` (str): Stock ticker symbol (e.g., "AAPL")

**Returns:** Company profile with latest price, P/E ratio, and other metrics

### 3. `get_stock_prices`

Get daily OHLCV prices for a ticker.

**Parameters:**
- `ticker` (str): Stock ticker symbol
- `start_date` (str): Start date in YYYY-MM-DD format
- `end_date` (str, optional): End date in YYYY-MM-DD format (defaults to today)
- `limit` (int, optional): Maximum rows to return (default: 252, max: 1000)

**Returns:** Array of price records with date, open, high, low, close, volume

### 4. `get_financial_ratios`

Get time-series financial ratios.

**Parameters:**
- `ticker` (str): Stock ticker symbol
- `start_date` (str): Start date in YYYY-MM-DD format
- `end_date` (str, optional): End date (defaults to today)
- `limit` (int, optional): Maximum rows (default: 100, max: 1000)

**Returns:** Array of ratio records including P/E, ROE, debt/equity, etc.

### 5. `get_fundamentals`

Get latest balance sheet, cash flow, and income statement data.

**Parameters:**
- `ticker` (str): Stock ticker symbol
- `timeframe` (str, optional): "quarterly" or "annual" (default: "quarterly")
- `limit` (int, optional): Number of periods (default: 4, max: 20)

**Returns:** Consolidated fundamentals across all three statements

### 6. `search_companies`

Search companies by name or ticker.

**Parameters:**
- `query` (str): Search term
- `limit` (int, optional): Maximum results (default: 20, max: 100)

**Returns:** Matching companies sorted by relevance

### 7. `get_economy_data`

Get economic indicators for a date range.

**Parameters:**
- `indicator_type` (str): "treasury_yields", "inflation", "inflation_expectations", or "labor_market"
- `start_date` (str): Start date in YYYY-MM-DD format
- `end_date` (str, optional): End date (defaults to today)
- `limit` (int, optional): Maximum rows (default: 100, max: 1000)

**Returns:** Economic indicator records

### 8. `get_economy_dashboard`

Get a summary view of the latest economic indicators.

**Parameters:**
- `limit` (int, optional): Number of recent data points (default: 10, max: 100)

**Returns:** Combined view of treasury yields, inflation, and labor market data

### 9. `execute_query`

Execute a custom read-only SQL query.

**Parameters:**
- `sql` (str): SQL SELECT statement

**Returns:** Query results as array of objects

**Security:**
- Only SELECT statements allowed
- No DDL (CREATE, DROP, ALTER)
- No DML (INSERT, UPDATE, DELETE)
- Query timeout: 30 seconds
- Max rows: 1000

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABASE_URL` | PostgreSQL connection string | Yes |
| `MCP_LOG_LEVEL` | Logging level (debug/info/warn/error) | No (default: info) |
| `MCP_QUERY_TIMEOUT` | Query timeout in seconds | No (default: 30) |
| `MCP_MAX_ROWS` | Maximum rows per query | No (default: 1000) |

### Database Connection String

```
postgresql://user:password@host:port/database
```

Example:
```
postgresql://stockuser:secret@localhost:5432/stock_data
```

## Installation

### From Source

```bash
cd mcp_server
pip install -e .
```

### Dependencies

- Python 3.10+
- `mcp>=1.6.0` - MCP SDK
- `psycopg[binary]>=3.0` - PostgreSQL driver
- `pydantic>=2.0` - Data validation

## opencode Integration

opencode supports MCP servers through the `.opencode/mcp.json` configuration file. This server includes a pre-configured setup at `.opencode/mcp.json`.

### Quick Setup

1. Ensure the virtual environment is activated:
   ```bash
   source .venv/bin/activate  # or .venv\Scripts\activate on Windows
   ```

2. Install the MCP server:
   ```bash
   cd mcp_server
   pip install -e .
   ```

3. Set the database connection:
   ```bash
   export DATABASE_URL="postgresql://user:pass@host:5432/stock_data"
   ```

4. The server will automatically be available in opencode via the `tools/` commands.

### Configuration File

The configuration is located at `.opencode/mcp.json`:

```json
{
  "mcpServers": {
    "stock_data": {
      "name": "Stock Data Server",
      "description": "S&P 500 stock market data including prices, fundamentals, and economic indicators",
      "transport": "stdio",
      "command": "python",
      "args": ["-m", "mcp_server.server"],
      "cwd": "${workspaceFolder}",
      "env": {
        "DATABASE_URL": "${env:DATABASE_URL}",
        "MCP_LOG_LEVEL": "info"
      }
    }
  }
}
```

### Using with opencode

Once configured, you can use natural language to query the data:

- "Show me Apple's current stock price and P/E ratio"
- "List all technology companies in the S&P 500"
- "Get the last 30 days of price data for MSFT"
- "What are the latest treasury yields?"
- "Search for companies in the healthcare sector"

## Claude Desktop Integration

Add to your Claude Desktop configuration file:

**macOS:**
```bash
~/Library/Application Support/Claude/claude_desktop_config.json
```

**Windows:**
```
%APPDATA%/Claude/claude_desktop_config.json
```

**Configuration:**

```json
{
  "mcpServers": {
    "stock_data": {
      "command": "python",
      "args": ["-m", "mcp_server.server"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@host:5432/stock_data"
      }
    }
  }
}
```

With virtual environment:

```json
{
  "mcpServers": {
    "stock_data": {
      "command": "/path/to/venv/bin/python",
      "args": ["-m", "mcp_server.server"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@host:5432/stock_data"
      }
    }
  }
}
```

## Security

### Read-Only Access

The server is designed for read-only access:
- All SQL queries are validated to be SELECT statements only
- Database user should have SELECT privileges only
- No modifications to data are possible through MCP tools

### Query Safeguards

- **Timeout**: All queries have a 30-second timeout
- **Row limits**: Maximum 1000 rows per query
- **SQL injection protection**: Parameterized queries used throughout
- **Error sanitization**: Database errors are logged but not exposed to clients

## Usage Examples

### Get Company Information

```json
{
  "tool": "get_company_details",
  "params": {
    "ticker": "AAPL"
  }
}
```

### Get Stock Prices

```json
{
  "tool": "get_stock_prices",
  "params": {
    "ticker": "AAPL",
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  }
}
```

### Search Companies

```json
{
  "tool": "search_companies",
  "params": {
    "query": "technology",
    "limit": 10
  }
}
```

### Get Economy Data

```json
{
  "tool": "get_economy_data",
  "params": {
    "indicator_type": "treasury_yields",
    "start_date": "2024-01-01",
    "limit": 30
  }
}
```

### Custom Query

```json
{
  "tool": "execute_query",
  "params": {
    "sql": "SELECT ticker, name, market_cap FROM companies WHERE market_cap > 100000000000 ORDER BY market_cap DESC LIMIT 10"
  }
}
```

## Development

### Running Tests

```bash
cd mcp_server
pytest
```

### Type Checking

```bash
mypy mcp_server
```

### Linting

```bash
ruff check mcp_server
ruff format mcp_server
```

## License

MIT
