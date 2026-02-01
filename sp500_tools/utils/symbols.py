"""Symbol file loading and validation utilities."""

import logging
import re
from pathlib import Path

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
