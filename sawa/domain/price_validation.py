"""Shared validation for daily OHLCV rows at provider/storage boundaries."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

MAX_STORABLE_PRICE = Decimal("1000000000000")
PRICE_SCALE = Decimal("0.00000001")
MAX_BIGINT = 9_223_372_036_854_775_807
MAX_PROVIDER_FUTURE_DAYS = 1
_STRICT_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")


def is_plausible_daily_price_date(
    value: object,
    *,
    today: date | None = None,
    max_future_days: int = MAX_PROVIDER_FUTURE_DAYS,
) -> bool:
    """Validate a real strict ISO storage date within provider clock skew."""
    if type(value) is date:
        parsed = value
    elif isinstance(value, str) and _STRICT_ISO_DATE.fullmatch(value) is not None:
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            return False
    else:
        return False
    reference_date = today or date.today()
    return parsed <= reference_date + timedelta(days=max_future_days)


def is_valid_daily_ohlcv(
    row: Mapping[str, Any],
    *,
    allow_numeric_strings: bool = False,
) -> bool:
    """Return whether a daily bar fits the exact ``stock_prices`` envelope.

    REST responses must supply numeric JSON values. CSV artifacts necessarily
    represent those same values as strings, so their callers can opt into
    numeric-string parsing without weakening any of the finite, precision,
    range, integral-volume, or OHLC relationship checks.
    """

    values = tuple(row.get(field) for field in ("open", "high", "low", "close", "volume"))
    if any(value is None or isinstance(value, bool) for value in values):
        return False
    if any(
        not isinstance(value, (int, float, Decimal))
        and not (allow_numeric_strings and isinstance(value, str) and bool(value.strip()))
        for value in values
    ):
        return False

    try:
        decimals = tuple(Decimal(str(value)) for value in values)
        open_value, high_value, low_value, close_value, volume_value = decimals
        if not all(value.is_finite() for value in decimals):
            return False
        rounded_prices = tuple(
            value.quantize(PRICE_SCALE, rounding=ROUND_HALF_UP)
            for value in (open_value, high_value, low_value, close_value)
        )
    except (InvalidOperation, TypeError, ValueError):
        return False

    if any(
        value <= 0 or value >= MAX_STORABLE_PRICE
        for value in rounded_prices
    ):
        return False
    if (
        volume_value < 0
        or volume_value > MAX_BIGINT
        or volume_value != volume_value.to_integral_value()
    ):
        return False

    open_value, high_value, low_value, close_value = rounded_prices
    return high_value >= max(open_value, low_value, close_value) and low_value <= min(
        open_value,
        high_value,
        close_value,
    )
