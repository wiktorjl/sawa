I want to build a set of python scripts to:

1. Figure out which days were trading days in the past N years
2. Download OHLC daily prices for all US equities for a given date
3. Downlad symbols that are components of SP500

N is 5 years.
I am using Polygin.IO (now Massive) and have an API key.

For checking if a day was a trading day if there is no explicit API for that, I am fine doing a hack, like checking if AAPL or similar stock traded on that day.

For downloading OHLC prices, I want them to be stored in a CSV file per symbol. I want the data to be ordered by date and have the following schema:

date, symbol, open, close, high, low, volume

Of course, I want split adjusted, dividend unadjusted prices.
I also want the solution to be written in python.
The script must provide clear and exhaustive help along with examples.
The script must neatly report its is progress in the console (piped to a log file as well).

I should be able to also rerun the script to download OHLC prices. For example, if i interrupt the script on symbol 111 out of 500, then i rerun the script (with flag --continue), the script should resume with 111 (lets assume it did not finish by default).

In summary, there will be three files:

1. download_sp500_symbols.py - stores sp symbols in sp500_symbols.txt
2. check_trading_days.py - takes a start date and stores all trading dates in file trading_days_START-DATE.txt where START_DATE was the date provided by the user
3. download_daily_prices.py - takes a date and stores OHLC data for all symbols in a file per symbol: i.e. AAPL.csv. I understand Polygon has a bulk api where we can download the whole market for a particular day as a single file from s3
