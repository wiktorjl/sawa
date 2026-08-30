"""
FRED (Federal Reserve Economic Data) API client.

Fetches market internals data:
- VIXCLS: CBOE Volatility Index (VIX) daily close
- VXVCLS: CBOE S&P 500 3-Month Volatility Index (VIX3M)
- BAMLH0A0HYM2: ICE BofA US High Yield Index OAS (credit spread)
"""

import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from sawa.domain.exceptions import ProviderError
from sawa.utils.security import redact_sensitive_text

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# FRED series IDs for market internals
SERIES = {
    "vix": "VIXCLS",
    "vix3m": "VXVCLS",
    "hy_spread": "BAMLH0A0HYM2",
}

_STRICT_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_MAX_PROVIDER_FUTURE_DAYS = 1
_MAX_MARKET_INTERNAL_VALUE = Decimal("9999.9999")


def _parse_iso_date(value: object, *, label: str) -> date:
    """Parse exactly YYYY-MM-DD rather than permissive ISO variants."""
    if not isinstance(value, str) or _STRICT_ISO_DATE.fullmatch(value) is None:
        raise ValueError(f"{label} must use YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} is not a valid calendar date") from exc


def _parse_requested_range(
    start_date: str | None,
    end_date: str | None,
) -> tuple[date | None, date | None]:
    """Validate caller-supplied bounds once, before per-series isolation."""
    start = (
        _parse_iso_date(start_date, label="start_date")
        if start_date is not None
        else None
    )
    end = (
        _parse_iso_date(end_date, label="end_date")
        if end_date is not None
        else None
    )
    if start is not None and end is not None and start > end:
        raise ValueError("start_date must be on or before end_date")
    return start, end


def _validate_observations(
    observations: object,
    *,
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    """Validate FRED observation dates before they can enter merged rows."""
    if not isinstance(observations, list):
        raise ProviderError("FRED observations payload is not an array", provider="fred")

    latest_plausible = date.today() + timedelta(days=_MAX_PROVIDER_FUTURE_DAYS)
    validated: list[dict[str, Any]] = []
    for index, observation in enumerate(observations):
        if not isinstance(observation, dict):
            raise ProviderError(
                f"FRED observation {index} is not an object",
                provider="fred",
            )
        try:
            observation_date = _parse_iso_date(
                observation.get("date"),
                label=f"observation {index} date",
            )
        except ValueError as exc:
            raise ProviderError(
                f"FRED observation {index} has an invalid date",
                provider="fred",
                original_error=exc,
            ) from exc

        if start is not None and observation_date < start:
            raise ProviderError(
                f"FRED observation {index} predates the requested window",
                provider="fred",
            )
        if end is not None and observation_date > end:
            raise ProviderError(
                f"FRED observation {index} exceeds the requested window",
                provider="fred",
            )
        if observation_date > latest_plausible:
            raise ProviderError(
                f"FRED observation {index} exceeds the plausible future bound",
                provider="fred",
            )
        value = observation.get("value")
        if value not in (".", "", None):
            if isinstance(value, bool):
                raise ProviderError(
                    f"FRED observation {index} has an invalid value",
                    provider="fred",
                )
            try:
                numeric_value = Decimal(str(value))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise ProviderError(
                    f"FRED observation {index} has an invalid value",
                    provider="fred",
                    original_error=exc,
                ) from exc
            if (
                not numeric_value.is_finite()
                or numeric_value < 0
                or numeric_value > _MAX_MARKET_INTERNAL_VALUE
            ):
                raise ProviderError(
                    f"FRED observation {index} has an invalid value",
                    provider="fred",
                )
        validated.append(observation)
    return validated


@dataclass(frozen=True, slots=True)
class FredSeriesFailure:
    """Safe, structured details for one failed FRED series request."""

    field: str
    series_id: str
    error_type: str
    message: str

    def to_dict(self) -> dict[str, str]:
        """Return JSON-friendly failure details for pipeline statistics."""
        return {
            "field": self.field,
            "series_id": self.series_id,
            "error_type": self.error_type,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class FredMarketInternalsResult:
    """Market-internals rows plus any independently failed source series."""

    rows: list[dict[str, Any]]
    failures: tuple[FredSeriesFailure, ...] = ()

    @property
    def all_series_failed(self) -> bool:
        """Whether every requested market-internals series failed."""
        return len(self.failures) == len(SERIES)

    @property
    def failure_details(self) -> list[dict[str, str]]:
        """Return JSON-friendly failures for daily/weekly/coldstart stats."""
        return [failure.to_dict() for failure in self.failures]


class FredClient:
    """Client for the FRED API."""

    def __init__(self, api_key: str, logger: logging.Logger | None = None):
        self.api_key = api_key
        self.logger = logger or logging.getLogger(__name__)
        self.client = httpx.Client(timeout=30.0)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "FredClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def get_series(
        self,
        series_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch observations for a FRED series.

        Args:
            series_id: FRED series ID (e.g., 'VIXCLS')
            start_date: Start date YYYY-MM-DD
            end_date: End date YYYY-MM-DD

        Returns:
            List of {"date": "YYYY-MM-DD", "value": "123.45"} dicts.
            Entries with value "." (missing) are filtered out.
        """
        requested_start, requested_end = _parse_requested_range(start_date, end_date)
        params: dict[str, str] = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }
        if start_date:
            params["observation_start"] = start_date
        if end_date:
            params["observation_end"] = end_date

        self.logger.debug(f"FRED: fetching {series_id} ({start_date} to {end_date})")

        response = self.client.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ProviderError("FRED response is not an object", provider="fred")

        observations = _validate_observations(
            data.get("observations", []),
            start=requested_start,
            end=requested_end,
        )
        # Filter out missing values (FRED uses "." for no data)
        return [
            obs for obs in observations
            if obs.get("value") not in (".", "", None)
        ]

    def get_market_internals(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> FredMarketInternalsResult:
        """
        Fetch all market internals series and merge by date.

        Returns:
            Rows with keys date, vix, vix3m, and hy_spread, plus a typed
            failure for each source series that could not be fetched. Missing
            values for a failed or gapped series are None.
        """
        requested_start, requested_end = _parse_requested_range(start_date, end_date)

        # Fetch all series
        series_data: dict[str, dict[str, str]] = {}
        failures: list[FredSeriesFailure] = []
        for field, series_id in SERIES.items():
            try:
                observations = self.get_series(series_id, start_date, end_date)
                # Defense against alternate/mocked get_series implementations:
                # no provider row may bypass the same date contract merely
                # because the transport method was substituted.
                observations = _validate_observations(
                    observations,
                    start=requested_start,
                    end=requested_end,
                )
                observations = [
                    observation
                    for observation in observations
                    if observation.get("value") not in (".", "", None)
                ]
                if not observations:
                    failure = FredSeriesFailure(
                        field=field,
                        series_id=series_id,
                        error_type="EmptyResult",
                        message="provider returned no usable observations",
                    )
                    failures.append(failure)
                    series_data[field] = {}
                    self.logger.warning(
                        f"  FRED {series_id} failed: "
                        f"{failure.error_type}: {failure.message}"
                    )
                    continue
                series_data[field] = {
                    obs["date"]: obs["value"]
                    for obs in observations
                }
                self.logger.info(
                    f"  FRED {series_id}: {len(observations)} observations"
                )
            except Exception as e:
                failure = FredSeriesFailure(
                    field=field,
                    series_id=series_id,
                    error_type=type(e).__name__,
                    message=redact_sensitive_text(e),
                )
                failures.append(failure)
                self.logger.warning(
                    f"  FRED {series_id} failed: "
                    f"{failure.error_type}: {failure.message}"
                )
                series_data[field] = {}

        # Preserve the date attached to every successful observation. FRED's
        # daily series normally publish business dates, but we deliberately do
        # not guess a different "next trading day" for an unusual calendar
        # date. Taking the union also means an HY-only success remains usable
        # when both volatility-series requests fail.
        observation_dates: set[str] = set()
        for field in SERIES:
            observation_dates.update(series_data[field])

        rows: list[dict[str, Any]] = []
        for dt in sorted(observation_dates):
            row: dict[str, Any] = {"date": dt}
            for field in SERIES:
                val = series_data[field].get(dt)
                row[field] = val  # str or None, DB will cast
            rows.append(row)

        self.logger.info(f"  Market internals: {len(rows)} dates total")
        return FredMarketInternalsResult(rows=rows, failures=tuple(failures))
