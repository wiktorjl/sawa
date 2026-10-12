"""Exact fractional-share volume shared by REST recovery and streaming."""

from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

# Preserve the previous integer headroom while retaining fractional shares.
MAX_VOLUME = 9_223_372_036_854_775_807


def decimal_volume(value: Any, *, allow_string: bool = False) -> Decimal:
    """Accept finite nonnegative volume without truncating provider fractions."""
    types = (int, float, Decimal, str) if allow_string else (int, float, Decimal)
    if isinstance(value, bool) or not isinstance(value, types):
        raise ValueError("volume must be a finite non-negative number")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("volume must be a finite non-negative number") from exc
    if not result.is_finite() or not 0 <= result <= MAX_VOLUME:
        raise ValueError("volume is outside the supported non-negative range")
    # PostgreSQL unconstrained NUMERIC supports at most 16,383 fractional digits.
    if result and int(result.as_tuple().exponent) < -16383:
        raise ValueError("volume exceeds PostgreSQL NUMERIC fractional precision")
    return result if result else Decimal(0)


def sum_volumes(values: list[Decimal]) -> Decimal:
    """Keep fractions even when adding to a volume near the integer limit."""
    scale = max((max(0, -int(value.as_tuple().exponent)) for value in values), default=0)
    with localcontext() as context:
        context.prec = max(28, 20 + len(str(len(values))) + scale)
        return sum(values, Decimal(0))
