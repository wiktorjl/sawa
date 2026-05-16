"""Input validation for MCP server tool arguments.

Validates common argument types (tickers, dates, limits, numeric ranges)
and returns clear error messages. Raises ValueError on invalid input.
"""

import re
from datetime import date, datetime, timedelta
from typing import Any

# Ticker: 1-10 uppercase alphanumeric chars, dots, hyphens (e.g., BRK.B, BF-B)
_TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,9}$")

# Index codes are database-backed and intentionally not hard-coded here. Keep
# validation to a conservative identifier shape so future rows in `indices`
# can be used without an MCP server release.
_INDEX_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")

# Date: strict YYYY-MM-DD format
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Max date range to prevent runaway queries (10 years)
_MAX_DATE_RANGE_DAYS = 3650

# Max future date offset (1 year)
_MAX_FUTURE_DAYS = 365

_LEGACY_INDEX_CODES = {"nasdaq5000": "nasdaq_listed"}

_TOOLS_ALLOW_INDEX_ALL = {
    "get_market_breadth",
    "get_52week_extremes",
    "get_ex_dividend_calendar",
    "get_recent_splits",
    "get_dividend_yield_leaders",
    "get_earnings_calendar",
}

_TOOLS_ALLOW_INDEX_BOTH = {"scan_ytd_performance"}

_FIELD_ENUMS: dict[str, set[Any]] = {
    "chart_detail": {"compact", "normal", "detailed"},
    "taxonomy": {"sic", "gics"},
    "sort_order": {"asc", "desc"},
    "metric": {"volume", "dollar_volume", "volume_ratio"},
    "category": {"trend", "momentum", "volatility", "volume"},
    "extreme": {"highs", "lows", "both"},
    "timing": {"BMO", "AMC", "all"},
    "method": {"pivot", "cluster", "volume"},
    "indicator_type": {
        "treasury_yields",
        "inflation",
        "inflation_expectations",
        "labor_market",
        "market_internals",
    },
}

_TOOL_FIELD_ENUMS: dict[tuple[str, str], set[Any]] = {
    ("get_fundamentals", "timeframe"): {"quarterly", "annual"},
    ("get_weekly_monthly_candles", "timeframe"): {"weekly", "monthly"},
    ("get_top_movers", "direction"): {"gainers", "losers", "both"},
    ("detect_crossovers", "direction"): {"above", "below"},
    ("get_top_movers", "period"): {"1d", "1w", "1m", "ytd"},
    ("detect_crossovers", "sma_period"): {50, 100, 150, 200},
    ("screen_stocks", "sort_by"): {
        "market_cap",
        "price",
        "volume",
        "change_1d",
        "change_1w",
        "rsi_14",
        "pe_ratio",
        "dividend_yield",
    },
}

_VALID_TIMEFRAMES = {"daily", "weekly", "monthly"}
_VALID_ALIGNMENT_INDICATORS = {"sma", "sma_trend", "rsi", "macd"}


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
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a number, got {type(value).__name__}")

    if allow_zero and value < 0:
        raise ValueError(f"{field_name} must be non-negative, got {value}")
    elif not allow_zero and value <= 0:
        raise ValueError(f"{field_name} must be positive, got {value}")

    return float(value)


def validate_index_code(index: Any, tool_name: str) -> str:
    """Validate and normalize an index code or an allowed sentinel."""
    if not isinstance(index, str) or not index.strip():
        raise ValueError("index must be a non-empty string")

    normalized = index.strip().lower()
    if normalized in _LEGACY_INDEX_CODES:
        replacement = _LEGACY_INDEX_CODES[normalized]
        raise ValueError(f"index '{normalized}' was renamed; use '{replacement}'")

    if normalized == "all":
        if tool_name not in _TOOLS_ALLOW_INDEX_ALL:
            raise ValueError(f"index 'all' is not valid for {tool_name}")
        return normalized

    if normalized == "both":
        if tool_name not in _TOOLS_ALLOW_INDEX_BOTH:
            raise ValueError(f"index 'both' is not valid for {tool_name}")
        return normalized

    if not _INDEX_CODE_RE.match(normalized):
        raise ValueError(
            f"Invalid index code: '{index}'. Use lowercase letters, digits, or underscores"
        )

    return normalized


def validate_enum(value: Any, field_name: str, allowed: set[Any]) -> Any:
    """Validate a field against an explicit set of accepted values."""
    if value not in allowed:
        allowed_display = ", ".join(str(v) for v in sorted(allowed, key=str))
        raise ValueError(f"Invalid {field_name}: {value!r}. Must be one of: {allowed_display}")
    return value


def validate_string_list(
    values: Any,
    field_name: str,
    allowed: set[str] | None = None,
    max_count: int = 10,
) -> list[str]:
    """Validate a short list of strings, optionally constrained to known values."""
    if not isinstance(values, list) or not values:
        raise ValueError(f"{field_name} must be a non-empty list")
    if len(values) > max_count:
        raise ValueError(f"Too many {field_name}: {len(values)} (max {max_count})")

    normalized = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} values must be non-empty strings")
        item = value.strip().lower()
        if allowed is not None and item not in allowed:
            allowed_display = ", ".join(sorted(allowed))
            raise ValueError(
                f"Invalid {field_name} value: {value!r}. Must be one of: {allowed_display}"
            )
        normalized.append(item)

    return normalized


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

    if name == "get_intraday_bars":
        has_ticker = arguments.get("ticker") is not None
        has_tickers = arguments.get("tickers") is not None
        if has_ticker == has_tickers:
            raise ValueError("get_intraday_bars requires exactly one of ticker or tickers")

    if "index" in arguments and arguments["index"] is not None:
        arguments["index"] = validate_index_code(arguments["index"], name)

    if "code" in arguments and arguments["code"] is not None:
        arguments["code"] = validate_index_code(arguments["code"], name)

    for field_name, allowed in _FIELD_ENUMS.items():
        if field_name in arguments and arguments[field_name] is not None:
            arguments[field_name] = validate_enum(arguments[field_name], field_name, allowed)

    for (tool_name, field_name), allowed in _TOOL_FIELD_ENUMS.items():
        if name == tool_name and field_name in arguments and arguments[field_name] is not None:
            arguments[field_name] = validate_enum(arguments[field_name], field_name, allowed)
            if field_name == "sma_period":
                arguments[field_name] = int(arguments[field_name])

    if "timeframes" in arguments and arguments["timeframes"] is not None:
        arguments["timeframes"] = validate_string_list(
            arguments["timeframes"], "timeframes", _VALID_TIMEFRAMES
        )

    if "indicators" in arguments and arguments["indicators"] is not None:
        arguments["indicators"] = validate_string_list(
            arguments["indicators"], "indicators", _VALID_ALIGNMENT_INDICATORS
        )

    if name == "execute_query" and arguments.get("params") is not None:
        if not isinstance(arguments["params"], dict):
            raise ValueError("params must be an object mapping SQL parameter names to values")

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
        max_days = 252 if name == "detect_candlestick_patterns" else 30
        if int(days) > max_days:
            raise ValueError(f"days too large: {int(days)} (max {max_days})")
        arguments["days"] = int(days)

    return arguments
