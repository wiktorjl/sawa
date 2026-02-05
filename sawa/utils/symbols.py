"""Symbol file loading and validation utilities."""

import logging
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup

TICKER_PATTERN = re.compile(r"^[A-Z]{1,5}(\.[A-Z])?$")


def validate_ticker(ticker: str) -> str:
    """
    Validate and normalize ticker symbol.

    Args:
        ticker: Raw ticker symbol

    Returns:
        Normalized uppercase ticker

    Raises:
        ValueError: If ticker format is invalid
    """
    ticker = ticker.upper().strip()
    if not TICKER_PATTERN.match(ticker):
        raise ValueError(f"Invalid ticker format: {ticker}")
    return ticker


def load_symbols(
    filepath: str | Path,
    logger: logging.Logger | None = None,
    validate: bool = True,
) -> list[str]:
    """
    Load stock symbols from a text file.

    Args:
        filepath: Path to file containing symbols (one per line)
        logger: Optional logger for progress reporting
        validate: Whether to validate ticker format

    Returns:
        List of ticker symbols

    Raises:
        FileNotFoundError: If file does not exist
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Symbols file not found: {filepath}")

    symbols = []
    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            symbol = line.strip()
            if not symbol or symbol.startswith("#"):
                continue
            symbol = symbol.upper()
            if validate:
                try:
                    symbol = validate_ticker(symbol)
                except ValueError as e:
                    if logger:
                        logger.warning(f"Line {line_num}: {e}")
                    continue
            symbols.append(symbol)

    if logger:
        logger.info(f"Loaded {len(symbols)} symbols from {filepath}")
    return symbols


def fetch_sp500_symbols(logger: logging.Logger) -> list[str]:
    """
    Fetch current S&P 500 symbols from Wikipedia.

    Args:
        logger: Logger instance

    Returns:
        List of ticker symbols

    Raises:
        requests.RequestException: If fetch fails
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; SawaDataBot/1.0)"}

    logger.info("Fetching S&P 500 symbols from Wikipedia...")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})

    if not table:
        raise ValueError("Could not find S&P 500 constituents table")

    symbols: list[str] = []
    for row in table.find_all("tr")[1:]:  # Skip header
        cells = row.find_all("td")
        if cells:
            ticker = cells[0].text.strip()
            symbols.append(ticker)

    logger.info(f"Found {len(symbols)} S&P 500 symbols")
    return symbols


def fetch_nasdaq100_symbols(logger: logging.Logger) -> list[str]:
    """
    Fetch current NASDAQ-100 symbols from Wikipedia.

    Args:
        logger: Logger instance

    Returns:
        List of ticker symbols

    Raises:
        requests.RequestException: If fetch fails
    """
    url = "https://en.wikipedia.org/wiki/NASDAQ-100"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; SawaDataBot/1.0)"}

    logger.info("Fetching NASDAQ-100 symbols from Wikipedia...")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})

    if not table:
        raise ValueError("Could not find NASDAQ-100 constituents table")

    symbols: list[str] = []
    for row in table.find_all("tr")[1:]:  # Skip header
        cells = row.find_all("td")
        if cells:
            ticker = cells[0].text.strip()  # First column is ticker
            symbols.append(ticker)

    logger.info(f"Found {len(symbols)} NASDAQ-100 symbols")
    return symbols


def fetch_index_symbols(index: str, logger: logging.Logger) -> list[str]:
    """
    Fetch symbols for a market index.

    Args:
        index: Index name ("sp500" or "nasdaq100")
        logger: Logger instance

    Returns:
        List of ticker symbols

    Raises:
        ValueError: If index not recognized
        requests.RequestException: If fetch fails
    """
    index_lower = index.lower()

    if index_lower in ("sp500", "s&p500", "s&p 500"):
        return fetch_sp500_symbols(logger)
    elif index_lower in ("nasdaq100", "nasdaq-100", "nasdaq 100"):
        return fetch_nasdaq100_symbols(logger)
    else:
        raise ValueError(f"Unknown index: {index}. Use 'sp500' or 'nasdaq100'")
