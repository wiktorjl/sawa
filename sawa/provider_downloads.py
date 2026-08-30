"""Truthful result metadata for provider-to-artifact download steps."""

from __future__ import annotations

from typing import Any

from sawa.domain.exceptions import ProviderError
from sawa.utils.symbols import validate_ticker


class DownloadCount(int):
    """Downloaded row count with request outcomes and fresh-artifact state."""

    requested: int
    succeeded: int
    failed: int
    artifact_written: bool

    def __new__(
        cls,
        rows: int,
        *,
        requested: int,
        succeeded: int,
        failed: int,
        artifact_written: bool,
    ) -> DownloadCount:
        result = super().__new__(cls, rows)
        result.requested = requested
        result.succeeded = succeeded
        result.failed = failed
        result.artifact_written = artifact_written
        return result

    @property
    def all_failed(self) -> bool:
        return self.requested > 0 and self.succeeded == 0 and self.failed == self.requested

    @property
    def empty_successful(self) -> bool:
        """Whether requests answered successfully but yielded no rows."""
        return self.requested > 0 and self.succeeded > 0 and int(self) == 0

    def summary(self) -> dict[str, int | bool]:
        return {
            "requested": self.requested,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "rows": int(self),
            "artifact_written": self.artifact_written,
        }


class DownloadStats(dict[str, int]):
    """Per-feed row counts plus explicit request and artifact outcomes."""

    def __init__(self) -> None:
        super().__init__()
        self.requests: dict[str, dict[str, int | bool]] = {}
        self.artifacts: set[str] = set()

    def record(
        self,
        feed: str,
        rows: int,
        *,
        requested: int,
        succeeded: int,
        failed: int,
        artifact: str | None = None,
    ) -> None:
        self[feed] = rows
        artifact_written = artifact is not None
        self.requests[feed] = {
            "requested": requested,
            "succeeded": succeeded,
            "failed": failed,
            "rows": rows,
            "artifact_written": artifact_written,
        }
        if artifact is not None:
            self.artifacts.add(artifact)

    @property
    def total_requested(self) -> int:
        return sum(int(item["requested"]) for item in self.requests.values())

    @property
    def total_succeeded(self) -> int:
        return sum(int(item["succeeded"]) for item in self.requests.values())

    @property
    def total_failed(self) -> int:
        return sum(int(item["failed"]) for item in self.requests.values())

    @property
    def all_failed(self) -> bool:
        return (
            self.total_requested > 0
            and self.total_succeeded == 0
            and self.total_failed == self.total_requested
        )

    @property
    def has_failures(self) -> bool:
        return self.total_failed > 0

    @property
    def failed_feeds(self) -> set[str]:
        """Feeds for which every requested call failed."""
        return {
            feed
            for feed, item in self.requests.items()
            if int(item["requested"]) > 0
            and int(item["succeeded"]) == 0
            and int(item["failed"]) == int(item["requested"])
        }

    @property
    def empty_feeds(self) -> set[str]:
        """Feeds that answered but produced no fresh artifact rows."""
        return {
            feed
            for feed, item in self.requests.items()
            if int(item["requested"]) > 0
            and int(item["succeeded"]) > 0
            and int(item["rows"]) == 0
        }


def bind_provider_record(
    record: Any,
    requested_symbol: str,
    *,
    output_field: str,
    provider: str = "polygon",
) -> dict[str, Any]:
    """Validate provider identity and bind a copied row to the requested ticker."""
    if not isinstance(record, dict):
        raise ProviderError("Provider returned a non-object record", provider=provider)

    requested = validate_ticker(requested_symbol)
    identities: list[Any] = []
    identity_field_present = "ticker" in record or "tickers" in record
    if "ticker" in record:
        identities.append(record["ticker"])
    if "tickers" in record:
        raw_tickers = record["tickers"]
        identities.extend(raw_tickers if isinstance(raw_tickers, list) else [raw_tickers])

    if identity_field_present and not identities:
        raise ProviderError(
            "Provider returned an empty ticker identity", provider=provider
        )

    normalized: set[str] = set()
    try:
        for identity in identities:
            if not isinstance(identity, str):
                raise ValueError("provider ticker identity must be a string")
            normalized.add(validate_ticker(identity))
    except ValueError as exc:
        raise ProviderError(
            "Provider returned an invalid ticker identity", provider=provider
        ) from exc

    if normalized and normalized != {requested}:
        raise ProviderError("Provider returned a mismatched ticker identity", provider=provider)

    bound = dict(record)
    bound.pop("ticker", None)
    bound.pop("tickers", None)
    bound[output_field] = requested
    return bound
