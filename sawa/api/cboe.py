"""
CBOE delayed-quotes API client.

Fetches same-day VIX/VIX3M settlement values. FRED (the primary
market-internals source) publishes VIXCLS/VXVCLS with a one-business-day
lag, so the evening daily run never sees today's row from FRED. CBOE's
delayed-quote feed carries the settled close (VIX settles 4:15 PM ET)
within minutes of settlement, letting the daily write today's VIX/VIX3M.

No API key required.
"""

import logging
import math
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import httpx

from sawa.domain.exceptions import ProviderError
from sawa.utils.security import redact_sensitive_text

BASE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/quotes"

# CBOE quote symbols -> market_internals columns
SYMBOLS = {
    "_VIX": "vix",
    "_VIX3M": "vix3m",
}

_STRICT_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_MAX_PROVIDER_FUTURE_DAYS = 1
_MAX_MARKET_INTERNAL_VALUE = 9_999.9999


@dataclass(frozen=True, slots=True)
class CboeQuoteFailure:
    """Safe, structured details for one failed CBOE quote request."""

    symbol: str
    field: str
    error_type: str
    message: str

    def to_dict(self) -> dict[str, str]:
        """Return JSON-friendly failure details for pipeline statistics."""
        return {
            "symbol": self.symbol,
            "field": self.field,
            "error_type": self.error_type,
            "message": self.message,
        }


class CboeMarketInternalsResult(list[dict[str, Any]]):
    """List-compatible CBOE rows with explicit per-quote failures.

    This remains a ``list`` subclass so callers written against the original
    return type keep working, while pipelines can inspect ``failures`` instead
    of treating a partial or total outage as a clean result.
    """

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        failures: tuple[CboeQuoteFailure, ...] = (),
    ) -> None:
        super().__init__(rows or [])
        self.failures = failures

    @property
    def rows(self) -> list[dict[str, Any]]:
        """Return the successful rows through the typed-result interface."""
        return self

    @property
    def all_quotes_failed(self) -> bool:
        """Whether every configured CBOE quote request failed."""
        return len(self.failures) == len(SYMBOLS)

    @property
    def failure_details(self) -> list[dict[str, str]]:
        """Return JSON-friendly failures for daily statistics."""
        return [failure.to_dict() for failure in self.failures]


def _parse_quote_date(last_trade: object) -> str:
    """Return a strict, plausible ISO date from a CBOE trade timestamp."""
    if not isinstance(last_trade, str) or len(last_trade) < 11 or last_trade[10] != "T":
        raise ProviderError("CBOE quote has invalid last_trade_time", provider="cboe")

    value = last_trade[:10]
    if _STRICT_ISO_DATE.fullmatch(value) is None:
        raise ProviderError("CBOE quote has invalid trade date", provider="cboe")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ProviderError(
            "CBOE quote has invalid trade date",
            provider="cboe",
            original_error=exc,
        ) from exc

    latest_plausible = date.today() + timedelta(days=_MAX_PROVIDER_FUTURE_DAYS)
    if parsed > latest_plausible:
        raise ProviderError(
            f"CBOE quote date {value} is beyond the plausible provider window",
            provider="cboe",
        )
    return value


def _validate_quote_identity(
    requested_symbol: str,
    payload: dict[str, Any],
    data: dict[str, Any],
) -> None:
    """Reject absent, mismatched, or mixed CBOE quote identities."""
    expected = requested_symbol.upper().lstrip("_^")
    identities: list[str] = []
    for raw_identity in (payload.get("symbol"), data.get("symbol")):
        if raw_identity is None:
            continue
        if not isinstance(raw_identity, str) or not raw_identity.strip():
            raise ProviderError("CBOE quote has invalid symbol identity", provider="cboe")
        identities.append(raw_identity.strip().upper().lstrip("_^"))
    if not identities or set(identities) != {expected}:
        raise ProviderError("CBOE quote symbol does not match request", provider="cboe")


class CboeClient:
    """Client for CBOE's delayed-quotes CDN endpoints."""

    def __init__(self, logger: logging.Logger | None = None):
        self.logger = logger or logging.getLogger(__name__)
        self.client = httpx.Client(timeout=30.0)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "CboeClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _fetch_quote(self, symbol: str) -> dict[str, Any]:
        """Fetch and validate one CBOE quote, raising on unusable data."""
        url = f"{BASE_URL}/{symbol}.json"
        self.logger.debug(f"CBOE: fetching {url}")

        response = self.client.get(url)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ProviderError("CBOE quote response is not an object", provider="cboe")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ProviderError("CBOE quote response has no data object", provider="cboe")
        _validate_quote_identity(symbol, payload, data)

        close = data.get("close")
        if close is None or isinstance(close, bool):
            raise ProviderError("CBOE quote has invalid close", provider="cboe")
        try:
            parsed_close = float(close)
        except (TypeError, ValueError) as exc:
            raise ProviderError(
                "CBOE quote has invalid close",
                provider="cboe",
                original_error=exc,
            ) from exc
        if (
            not math.isfinite(parsed_close)
            or parsed_close <= 0
            or parsed_close > _MAX_MARKET_INTERNAL_VALUE
        ):
            raise ProviderError("CBOE quote has invalid close", provider="cboe")

        trade_date = _parse_quote_date(data.get("last_trade_time"))
        return {"date": trade_date, "close": parsed_close}

    def get_quote(self, symbol: str) -> dict[str, Any] | None:
        """
        Fetch the delayed quote for an index symbol (e.g. '_VIX').

        Returns:
            {"date": "YYYY-MM-DD", "close": float} for the most recent
            session, or None if the quote is unavailable or unusable.
        """
        try:
            return self._fetch_quote(symbol)
        except ProviderError as exc:
            # Preserve the original one-quote API: unusable provider payloads
            # return None. The batch API calls _fetch_quote directly so the
            # same condition becomes an explicit typed per-symbol failure.
            self.logger.warning(f"  CBOE {symbol}: unusable quote ({exc})")
            return None

    def get_market_internals(self) -> CboeMarketInternalsResult:
        """
        Fetch latest VIX/VIX3M settlement values grouped by actual session.

        VIX and VIX3M normally settle together and therefore produce one row.
        If the feeds temporarily disagree, separate partial rows preserve each
        quote's reported session rather than fabricating either value's date.

        Returns:
            A list-compatible typed result with successful rows and one
            structured failure per unavailable/unusable quote. Rows use the
            quote's actual reported date; values are never shifted onto a
            different session when the two feeds temporarily disagree.
        """
        quotes: list[tuple[str, dict[str, Any]]] = []
        failures: list[CboeQuoteFailure] = []
        for symbol, field in SYMBOLS.items():
            try:
                quote = self._fetch_quote(symbol)
            except Exception as e:
                failure = CboeQuoteFailure(
                    symbol=symbol,
                    field=field,
                    error_type=type(e).__name__,
                    message=redact_sensitive_text(e),
                )
                failures.append(failure)
                self.logger.warning(
                    f"  CBOE {symbol} failed: "
                    f"{failure.error_type}: {failure.message}"
                )
                continue
            quotes.append((field, quote))
            self.logger.info(
                f"  CBOE {symbol}: {quote['close']} ({quote['date']})"
            )

        by_date: dict[str, dict[str, Any]] = {}
        for field, quote in quotes:
            row = by_date.setdefault(quote["date"], {"date": quote["date"]})
            row[field] = quote["close"]

        rows = [by_date[value] for value in sorted(by_date)]
        return CboeMarketInternalsResult(rows, failures=tuple(failures))
