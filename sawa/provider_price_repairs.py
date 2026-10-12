"""Recover provider rounding loss using as-traded bars and verified split history."""

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sawa.domain.corporate_actions import SplitAdjuster, StockSplit
from sawa.domain.exceptions import ProviderError
from sawa.domain.price_validation import is_valid_daily_ohlcv, normalize_provider_volume
from sawa.utils.dates import timestamp_to_date

_FIELDS = {"o": "open", "h": "high", "l": "low", "c": "close"}
_ROUNDING_ERROR = Decimal("0.00005")


def _stored_shape(ticker: str, row: dict[str, Any]) -> dict[str, Any]:
    timestamp = row.get("t")
    if type(timestamp) is not int:
        raise ValueError("Invalid aggregate timestamp")
    return {
        "ticker": ticker, "date": timestamp_to_date(timestamp),
        **{field: row.get(key) for key, field in _FIELDS.items()},
        "volume": normalize_provider_volume(row.get("v")),
    }


def _matches_published(
    derived: dict[str, Any], published: dict[str, Any], *, probe: bool = False,
) -> bool:
    try:
        return (
            all(
                not isinstance(published.get(key), bool)
                and abs(Decimal(str(derived[field])) - Decimal(str(published[key])))
                <= (
                    max(_ROUNDING_ERROR, abs(Decimal(str(derived[field]))) * Decimal("0.0001"))
                    if probe else _ROUNDING_ERROR
                )
                for key, field in _FIELDS.items()
            )
            and derived["volume"] == normalize_provider_volume(published.get("v"))
        )
    except (KeyError, InvalidOperation, TypeError, ValueError):
        return False


def recover_rounded_adjusted_bars(
    client: Any, ticker: str, response: dict[str, Any], start: str, end: str,
    rate_limiter: Any = None,
) -> tuple[list[dict[str, Any]], int]:
    """Repair only zero-price rounding confirmed by raw bars and the exact ledger.

    Polygon sometimes rounds adjusted OTC OHLC to four decimal places, turning
    a valid positive price into zero (ELOX's 0.000001 * 11 = 0.000011). Require
    five nonzero adjusted bars to agree with an independently fetched as-traded
    response and its executed splits (0.01% probe tolerance for compounded
    historical rounding). Every repaired OHLC and volume must also
    agree with the published bar within its four-decimal rounding envelope.
    Unexplained or unrepresentable rows remain invalid; no prices are invented.
    """
    rows = response.get("results", [])
    if not isinstance(rows, list):
        raise ProviderError("Invalid aggregate results array", provider="polygon")
    candidates = [
        row for row in rows if isinstance(row, dict)
        and any(type(row.get(key)) in (int, float, Decimal) and row[key] == 0 for key in _FIELDS)
    ]
    if not candidates or not (
        response.get("ticker") == ticker and response.get("adjusted") is True
        and response.get("status") in {"OK", "DELAYED"}
    ):
        return rows, 0
    if rate_limiter:
        rate_limiter.acquire()
    raw = client.get(
        "aggregates", path_params={"ticker": ticker, "start": start, "end": end},
        params={"adjusted": "false", "limit": 50000},
    )
    if not (
        isinstance(raw, dict) and raw.get("ticker") == ticker
        and raw.get("adjusted") is False and raw.get("status") in {"OK", "DELAYED"}
        and isinstance(raw.get("results"), list)
    ):
        raise ProviderError("Invalid as-traded response for rounding recovery", provider="polygon")
    if rate_limiter:
        rate_limiter.acquire()
    as_of = date.today()
    ledger = client.get_splits(ticker=ticker, execution_date_lte=as_of.isoformat())
    if not isinstance(ledger, list):
        raise ProviderError("Invalid split ledger for rounding recovery", provider="polygon")
    splits = [StockSplit.from_polygon(row) for row in ledger]
    if any(s.ticker != ticker or s.execution_date > as_of for s in splits):
        raise ProviderError("Mismatched split ledger for rounding recovery", provider="polygon")
    if len({s.execution_date for s in splits}) != len(splits):
        raise ProviderError("Duplicate split dates for rounding recovery", provider="polygon")
    adjuster = SplitAdjuster(splits)
    derived: dict[int, dict[str, Any]] = {}
    seen: set[int] = set()
    for row in raw["results"]:
        if not isinstance(row, dict) or type(row.get("t")) is not int:
            continue
        timestamp = row["t"]
        if timestamp in seen:
            raise ProviderError("Duplicate raw bar for rounding recovery", provider="polygon")
        seen.add(timestamp)
        try:
            shaped = _stored_shape(ticker, row)
            if not is_valid_daily_ohlcv(shaped):
                continue
            adjusted = adjuster.adjust_row(shaped)
            if is_valid_daily_ohlcv(adjusted):
                derived[timestamp] = adjusted
        except (ValueError, OverflowError, OSError):
            continue
    probes: set[int] = set()
    for row in rows:
        if not isinstance(row, dict) or type(row.get("t")) is not int:
            continue
        try:
            if is_valid_daily_ohlcv(_stored_shape(ticker, row)) and row["t"] in derived:
                if not _matches_published(derived[row["t"]], row, probe=True):
                    return rows, 0
                probes.add(row["t"])
        except (ValueError, OverflowError, OSError):
            continue
    if len(probes) < 5:
        return rows, 0
    repaired = {}
    for row in candidates:
        timestamp = row.get("t")
        if type(timestamp) is not int:
            continue
        candidate = derived.get(timestamp)
        if candidate is not None and _matches_published(candidate, row):
            repaired[timestamp] = {
                **row, **{key: candidate[field] for key, field in _FIELDS.items()},
                "v": candidate["volume"],
            }
    return [
        repaired.get(row["t"], row)
        if isinstance(row, dict) and type(row.get("t")) is int else row
        for row in rows
    ], len(repaired)
