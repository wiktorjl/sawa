# SQL Schema for Stock Data

This directory contains PostgreSQL schema definitions for the stock data project.

## Files

| File | Description |
|------|-------------|
| `00_setup.sql` | Complete setup script and verification |
| `01_companies.sql` | Companies/Overviews table (central reference) |
| `02_market_data.sql` | Stock prices and financial ratios tables |
| `03_fundamentals.sql` | Balance sheets, cash flows, income statements |
| `04_economy.sql` | Treasury yields, inflation, labor market tables |
| `05_indexes.sql` | Performance indexes for all tables |
| `06_views.sql` | Common query views |
| `07_procedures.sql` | Data loading procedures |

## Usage

### Option 1: Run all at once
```bash
psql -d your_database -f sqlschema/00_setup.sql
```

### Option 2: Run individually (recommended for first setup)
```bash
psql -d your_database -f sqlschema/01_companies.sql
psql -d your_database -f sqlschema/02_market_data.sql
psql -d your_database -f sqlschema/03_fundamentals.sql
psql -d your_database -f sqlschema/04_economy.sql
psql -d your_database -f sqlschema/05_indexes.sql
psql -d your_database -f sqlschema/06_views.sql
psql -d your_database -f sqlschema/07_procedures.sql
```

## Data Loading

### Load Companies
```sql
COPY companies (
    ticker, name, description, market, type, locale, currency_name, active,
    list_date, delisted_utc, primary_exchange, cik, composite_figi, share_class_figi,
    sic_code, sic_description, market_cap, weighted_shares_outstanding,
    share_class_shares_outstanding, total_employees, round_lot, ticker_root,
    ticker_suffix, homepage_url, phone_number, address_address1, address_city,
    address_state, address_postal_code, branding_logo_url, branding_icon_url
) FROM '/path/to/overviews/OVERVIEWS.csv' WITH (FORMAT csv, HEADER true);
```

### Load Stock Prices
```sql
-- For each ticker file
COPY stock_prices (ticker, date, open, high, low, close, volume)
FROM '/path/to/stocks/AAPL.csv' WITH (FORMAT csv, HEADER true);
```

### Load Financial Ratios
```sql
COPY financial_ratios (
    ticker, date, average_volume, cash, current, debt_to_equity,
    dividend_yield, earnings_per_share, enterprise_value, ev_to_ebitda,
    ev_to_sales, free_cash_flow, market_cap, price, price_to_book,
    price_to_cash_flow, price_to_earnings, price_to_free_cash_flow,
    price_to_sales, quick, return_on_assets, return_on_equity
) FROM '/path/to/ratios/RATIOS.csv' WITH (FORMAT csv, HEADER true);
```

### Load Economy Data
```sql
COPY treasury_yields FROM '/path/to/economy_data/treasury-yields.csv' WITH (FORMAT csv, HEADER true);
COPY inflation FROM '/path/to/economy_data/inflation.csv' WITH (FORMAT csv, HEADER true);
COPY inflation_expectations FROM '/path/to/economy_data/inflation-expectations.csv' WITH (FORMAT csv, HEADER true);
COPY labor_market FROM '/path/to/economy_data/labor-market.csv' WITH (FORMAT csv, HEADER true);
```

## Table Relationships

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
```

## Useful Queries

### Get latest stock price for all companies
```sql
SELECT * FROM v_company_summary;
```

### Get economy dashboard
```sql
SELECT * FROM v_economy_dashboard LIMIT 10;
```

### Get latest fundamentals
```sql
SELECT * FROM v_latest_fundamentals WHERE ticker = 'AAPL';
```

### Get sector summary
```sql
SELECT * FROM v_sector_summary;
```
