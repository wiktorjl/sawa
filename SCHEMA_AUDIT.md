# Schema Audit Report

## Actual Database Tables (from sqlschema/*.sql)

### Core Tables (CREATE TABLE)

| Table Name | Defined In | Primary Key | Description |
|---|---|---|---|
| `companies` | 01_companies.sql | `ticker` | Company metadata, central reference |
| `stock_prices` | 02_market_data.sql | `(ticker, date)` | Daily OHLCV prices |
| `financial_ratios` | 02_market_data.sql | `(ticker, date)` | Time-series financial ratios |
| `balance_sheets` | 03_fundamentals.sql | `(ticker, period_end, timeframe)` | Balance sheet data |
| `cash_flows` | 03_fundamentals.sql | `(ticker, period_end, timeframe)` | Cash flow statements |
| `income_statements` | 03_fundamentals.sql | `(ticker, period_end, timeframe)` | Income statements |
| `treasury_yields` | 04_economy.sql | `date` | Treasury yield curve |
| `inflation` | 04_economy.sql | `date` | CPI/PCE inflation metrics |
| `inflation_expectations` | 04_economy.sql | `date` | Market/model inflation expectations |
| `labor_market` | 04_economy.sql | `date` | Unemployment, job openings, etc. |
| `sic_gics_mapping` | 08_sic_gics_mapping.sql | `sic_code` | SIC to GICS sector mapping |
| `news_articles` | 10_news.sql | `id` | News articles |
| `news_article_tickers` | 10_news.sql | `(article_id, ticker)` | Article-ticker junction |
| `news_sentiment` | 10_news.sql | `(article_id, ticker)` | Per-article sentiment |
| `technical_indicators` | 11_technical_indicators.sql | `(ticker, date)` | Daily TA indicators (SMA, RSI, MACD, etc.) |
| `technical_indicator_metadata` | 11_technical_indicators.sql | `indicator_name` | TA indicator registry/metadata |
| `indices` | 12_indices.sql | `id` (serial) | Market index definitions (sp500, nasdaq100) |
| `index_constituents` | 12_indices.sql | `(index_id, ticker)` | Index membership junction table |
| `stock_splits` | 18_corporate_actions.sql | `id` (serial) | Stock split history |
| `dividends` | 18_corporate_actions.sql | `id` (serial) | Dividend history |
| `earnings` | 18_corporate_actions.sql | `id` (serial) | Earnings calendar and actuals |
| `stock_prices_intraday` | 21_intraday_prices.sql | `(ticker, timestamp)` | 5-min intraday bars |

### Views

| View Name | Defined In | Description |
|---|---|---|
| `v_company_summary` | 06_views.sql | Latest company overview with price/ratios |
| `v_economy_dashboard` | 06_views.sql | Combined economy indicators |
| `v_latest_fundamentals` | 06_views.sql | Latest quarterly fundamentals per company |
| `v_sector_summary` | 06_views.sql | Sector aggregates by SIC code |
| `v_company_with_indices` | 06_views.sql | Companies with index membership arrays |
| `stock_prices_live` | 06_views.sql | **VIEW** - union of historical EOD + today's intraday |

### Dropped Tables (16_cleanup.sql)

These tables no longer exist:
- `watchlist_symbols`, `watchlists`
- `user_settings`, `default_settings`, `active_user`, `users`
- `glossary_terms`, `glossary_term_list`
- `company_overviews`

---

## Incorrect/Non-Existent Table References in Query Log

The following table names appear in `logs/execute_query.log` but **do not exist** as actual tables:

| Ghost Table Name | Log Lines | What Should Be Used | Severity |
|---|---|---|---|
| `price_metrics` | Lines 9-13 | No such table. Must use `stock_prices` + computed columns | **ERROR** - query will fail |
| `daily_prices` | Lines 56, 64, 67, 89, 97, 122, 131, 134, 1282 | `stock_prices` (the actual table name) | **ERROR** - query will fail |
| `stock_prices_live` (used in log) | Lines 830-858 | This is a VIEW, not a table. Usage is valid but callers need to know it may be slow | WARNING |
| `intraday_bars` | Line 1186 | `stock_prices_intraday` (actual table name) | **ERROR** - query will fail |
| `index_members` | Line 53 | `index_constituents` (actual table) joined with `indices` | **ERROR** - query will fail |
| `market_indexes` | Line 1129 | `indices` (actual table name) | **ERROR** - query will fail |

### Column-Level Issues in Query Log

| Issue | Log Lines | Details |
|---|---|---|
| `stock_prices.date` vs `stock_prices.price_date` | Lines 224-304 use `date` column (correct), but lines 141-168 use `price_date` | `stock_prices` has `date` column. `price_date` does not exist. |
| `c.sp500` | Line 11 | `companies` table has no `sp500` column. Index membership is via `index_constituents` + `indices` |
| `c.gics_sector` | Lines 774, 1122 | `companies` table has no `gics_sector` column. Sector data is in `sic_gics_mapping` via JOIN on `sic_code` |

---

## Codebase Analysis: Table References in Python Code

### Correct References (no issues found)

The **Python codebase** (sawa/ and mcp_server/) is largely correct:

- `sawa/` core package consistently uses `stock_prices`, `index_constituents`, `indices`, `stock_prices_intraday`, `technical_indicators`
- `mcp_server/tools/` correctly uses `stock_prices_live` (the VIEW) for live price queries, and `stock_prices` for historical-only queries
- `mcp_server/tools/indices.py` correctly uses `index_constituents` + `indices`

### No Python code references these ghost tables:
- `daily_prices` - zero references in .py files
- `price_metrics` - zero references in .py files
- `index_members` - zero references in .py files
- `market_indexes` - zero references in .py files
- `intraday_bars` - used only as a Python function name (`load_intraday_bars`, `get_intraday_bars`), not as a SQL table name in queries

---

## Root Cause Analysis

The incorrect table names in the query log all come from the **MCP server's `execute_query` tool**, which allows LLMs to write free-form SQL. The LLM caller (not the codebase) is guessing table names incorrectly:

1. **`daily_prices`** - The LLM assumed a common naming pattern. Actual name is `stock_prices`.
2. **`price_metrics`** - Completely fabricated. No such table or view exists.
3. **`index_members`** - The LLM simplified the two-table pattern (`indices` + `index_constituents`) into a single imagined table.
4. **`market_indexes`** - Close guess but wrong. Actual name is `indices`.
5. **`intraday_bars`** - The LLM confused the Python function name with the table name. Actual table is `stock_prices_intraday`.
6. **`c.sp500`** / **`c.gics_sector`** - The LLM assumed these columns exist on `companies`, but they don't.

---

## Recommendations

### 1. Expose schema metadata to the LLM caller

The `execute_query` tool should provide table/column metadata so the LLM doesn't have to guess. Options:
- Add a `list_tables` tool that returns table names and column definitions
- Include schema summary in the `execute_query` tool description
- Auto-prepend available table names in the tool's system prompt

### 2. Provide a table name mapping in tool descriptions

At minimum, the execute_query tool description should list:
```
Available tables: companies, stock_prices, stock_prices_intraday, financial_ratios,
balance_sheets, cash_flows, income_statements, treasury_yields, inflation,
inflation_expectations, labor_market, technical_indicators, indices,
index_constituents, sic_gics_mapping, news_articles, news_article_tickers,
news_sentiment, stock_splits, dividends, earnings

Available views: stock_prices_live, v_company_summary, v_economy_dashboard,
v_latest_fundamentals, v_sector_summary, v_company_with_indices
```

### 3. Consider adding convenience views for common LLM mistakes

- `daily_prices` as an alias view for `stock_prices` (low cost, prevents errors)
- A denormalized `index_members` view joining `indices` + `index_constituents`

### 4. Validate queries before execution

The execute_query tool could parse table names from the SQL and warn/reject if referencing non-existent tables, before actually running the query against PostgreSQL.

---

## Summary

| Category | Count |
|---|---|
| Actual tables | 22 |
| Views | 6 |
| Ghost table references (log only) | 6 distinct names |
| Python code issues | 0 (codebase is clean) |
| All errors originate from | LLM free-form SQL via execute_query tool |
