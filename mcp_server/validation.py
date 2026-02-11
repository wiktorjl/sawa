"""Input validation for MCP server tool arguments.

Validates common argument types (tickers, dates, limits, numeric ranges)
and returns clear error messages. Raises ValueError on invalid input.
"""

import re
from datetime import date, datetime, timedelta
from typing import Any

# Ticker: 1-10 uppercase alphanumeric chars, dots, hyphens (e.g., BRK.B, BF-B)
_TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,9}$")

# Date: strict YYYY-MM-DD format
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Max date range to prevent runaway queries (10 years)
_MAX_DATE_RANGE_DAYS = 3650

# Max future date offset (1 year)
_MAX_FUTURE_DAYS = 365


def validate_ticker(ticker: str) -> str:
    """Validate and normalize a ticker symbol.

    Returns the uppercased ticker.
    Raises ValueError if invalid.
    """
    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("Ticker must be a non-empty string")

    ticker = ticker.strip().upper()

    if not _TICKER_RE.match(ticker):
        raise ValueError(
            f"Invalid ticker format: '{ticker}'. "
            "Must be 1-10 characters: letters, digits, dots, or hyphens"
        )

    return ticker


def validate_tickers(tickers: list[Any], max_count: int = 50) -> list[str]:
    """Validate a list of ticker symbols.

    Returns list of uppercased tickers.
    Raises ValueError if invalid.
    """
    if not isinstance(tickers, list) or len(tickers) == 0:
        raise ValueError("Tickers must be a non-empty list")

    if len(tickers) > max_count:
        raise ValueError(f"Too many tickers: {len(tickers)} (max {max_count})")

    return [validate_ticker(t) for t in tickers]


def validate_date(date_str: str, field_name: str = "date") -> str:
    """Validate a date string in YYYY-MM-DD format.

    Returns the validated date string.
    Raises ValueError if invalid.
    """
    if not isinstance(date_str, str) or not date_str.strip():
        raise ValueError(f"{field_name} must be a non-empty string")

    date_str = date_str.strip()

    if not _DATE_RE.match(date_str):
        raise ValueError(f"Invalid {field_name} format: '{date_str}'. Use YYYY-MM-DD")

    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid {field_name}: '{date_str}' is not a valid date")

    max_future = date.today() + timedelta(days=_MAX_FUTURE_DAYS)
    if parsed > max_future:
        raise ValueError(
            f"{field_name} '{date_str}' is too far in the future (max 1 year ahead)"
        )

    return date_str


def validate_date_range(start_date: str, end_date: str | None) -> None:
    """Validate that start_date <= end_date and range is reasonable.

    Raises ValueError if invalid.
    """
    validate_date(start_date, "start_date")

    if end_date is None:
        return

    validate_date(end_date, "end_date")

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start > end:
        raise ValueError(
            f"start_date ({start_date}) must be before end_date ({end_date})"
        )

    if (end - start).days > _MAX_DATE_RANGE_DAYS:
        raise ValueError(
            f"Date range too large: {(end - start).days} days (max {_MAX_DATE_RANGE_DAYS})"
        )


def validate_limit(limit: Any, max_limit: int = 1000) -> int:
    """Validate a limit parameter.

    Returns the validated limit as int.
    Raises ValueError if invalid.
    """
    if not isinstance(limit, (int, float)):
        raise ValueError(f"Limit must be a number, got {type(limit).__name__}")

    result = int(limit)

    if result < 1:
        raise ValueError(f"Limit must be at least 1, got {result}")

    if result > max_limit:
        raise ValueError(f"Limit too large: {result} (max {max_limit})")

    return result


def validate_positive_number(
    value: Any, field_name: str, allow_zero: bool = False
) -> float:
    """Validate that a value is a positive number.

    Returns the validated value as float.
    Raises ValueError if invalid.
    """
    if not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a number, got {type(value).__name__}")

    if allow_zero and value < 0:
        raise ValueError(f"{field_name} must be non-negative, got {value}")
    elif not allow_zero and value <= 0:
        raise ValueError(f"{field_name} must be positive, got {value}")

    return float(value)


def validate_tool_arguments(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Validate common arguments based on what's present in the dict.

    Validates and normalizes arguments in-place based on field names.
    Returns the arguments dict with normalized values.
    Raises ValueError with clear message on validation failure.
    """
    # Validate ticker (single)
    if "ticker" in arguments and arguments["ticker"] is not None:
        arguments["ticker"] = validate_ticker(arguments["ticker"])

    # Validate tickers (batch)
    if "tickers" in arguments and arguments["tickers"] is not None:
        arguments["tickers"] = validate_tickers(arguments["tickers"])

    # Validate dates
    start_date = arguments.get("start_date")
    end_date = arguments.get("end_date")

    if start_date is not None:
        arguments["start_date"] = validate_date(start_date, "start_date")

    if end_date is not None:
        arguments["end_date"] = validate_date(end_date, "end_date")

    if start_date is not None:
        validate_date_range(start_date, end_date)

    # Validate standalone date field (e.g., get_intraday_bars, get_market_breadth)
    if "date" in arguments and arguments["date"] is not None:
        arguments["date"] = validate_date(arguments["date"], "date")

    if "target_date" in arguments and arguments["target_date"] is not None:
        arguments["target_date"] = validate_date(arguments["target_date"], "target_date")

    # Validate limit
    if "limit" in arguments and arguments["limit"] is not None:
        arguments["limit"] = validate_limit(arguments["limit"])

    # Validate positive numeric fields
    if "min_price" in arguments and arguments["min_price"] is not None:
        arguments["min_price"] = validate_positive_number(
            arguments["min_price"], "min_price", allow_zero=True
        )

    if "min_volume" in arguments and arguments["min_volume"] is not None:
        arguments["min_volume"] = int(
            validate_positive_number(arguments["min_volume"], "min_volume")
        )

    if "min_yield" in arguments and arguments["min_yield"] is not None:
        arguments["min_yield"] = validate_positive_number(
            arguments["min_yield"], "min_yield", allow_zero=True
        )

    if "days" in arguments and arguments["days"] is not None:
        days = arguments["days"]
        if not isinstance(days, (int, float)) or int(days) < 1:
            raise ValueError(f"days must be a positive integer, got {days}")
        if int(days) > 30:
            raise ValueError(f"days too large: {int(days)} (max 30)")
        arguments["days"] = int(days)

    return arguments
