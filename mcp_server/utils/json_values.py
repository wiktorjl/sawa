"""Strict JSON normalization shared by database and protocol boundaries."""

import json
import math
from decimal import Decimal
from numbers import Integral, Real
from typing import Any


def normalize_json_value(value: Any) -> Any:
    """Return a deterministic, strict-JSON-safe representation of ``value``."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else None
    if isinstance(value, dict):
        return {str(key): normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_json_value(item) for item in value]
    return str(value)


def compact_json_size(value: Any) -> int:
    """Return UTF-8 bytes in the canonical compact strict-JSON representation."""
    rendered = json.dumps(
        normalize_json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    return len(rendered.encode("utf-8"))
