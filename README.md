# S&P 500 Data Downloader

A set of Python scripts to download S&P 500 constituent symbols, historical trading days, and daily OHLC prices using Polygon.io API.

## Features

- **download_sp500_symbols.py**: Download current S&P 500 constituents
- **check_trading_days.py**: Find all trading days in a date range
- **download_daily_prices.py**: Download OHLC daily prices for all symbols

## Prerequisites

- Python 3.8+
- Polygon.io API key (free tier available)

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set your Polygon.io API key
export POLYGON_API_KEY="your_api_key_here"
```

## Usage

### 1. Download S&P 500 Symbols

```bash
# Download symbols to default file (sp500_symbols.txt)
python download_sp500_symbols.py

# Custom output file
python download_sp500_symbols.py -o my_symbols.txt

# Verbose logging
python download_sp500_symbols.py -v
```

### 2. Check Trading Days

```bash
# Find trading days for the past 5 years (default)
python check_trading_days.py --years 5

# Custom start date
python check_trading_days.py --start-date 2020-01-01

# Custom date range
python check_trading_days.py --start-date 2020-01-01 --end-date 2023-12-31

# Custom output file
python check_trading_days.py --years 3 -o my_trading_days.txt
```

**Output:** `trading_days_YYYY-MM-DD.txt` with one date per line:
```
2024-01-02
2024-01-03
2024-01-04
...
```

### 3. Download Daily Prices

```bash
# Download prices for a specific date
python download_daily_prices.py --date 2024-01-02

# Continue from previous run
python download_daily_prices.py --date 2024-01-02 --continue

# Output directory
python download_daily_prices.py --date 2024-01-02 --output-dir ./prices
```

## Data Schema

OHLC CSV files have the following format:

```
date,symbol,open,close,high,low,volume
2024-01-02,AAPL,185.64,185.92,188.13,184.73,54678432
```

Prices are split-adjusted and dividend-unadjusted.

## API Reference

- [Polygon.io Docs](https://polygon.io/docs/)
- Uses REST API v3 and Bulk API for market data
