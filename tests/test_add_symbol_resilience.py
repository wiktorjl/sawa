"""Transaction regressions for add-symbol ingestion (no real database)."""

import logging
from typing import Any
from unittest import mock

import psycopg
import pytest

from sawa import add_symbol
from sawa.add_symbol import (
    FundamentalLoadResult,
    RatioLoadResult,
    fetch_and_insert_fundamentals,
    fetch_and_insert_prices,
    fetch_and_insert_ratios,
    run_add_symbols,
)
from sawa.domain.exceptions import ProviderError


def _sql_text(statement: object) -> str:
    if isinstance(statement, str):
        return statement
    return statement.as_string()  # type: ignore[attr-defined,no-any-return]


class _RowFailureCursor:
    def __init__(
        self,
        events: list[str],
        outcomes: list[int | Exception] | None = None,
    ) -> None:
        self.events = events
        self.outcomes = outcomes or [1, psycopg.errors.CheckViolation("bad row"), 1]
        self.insert_attempts = 0
        self.insert_params: list[object] = []
        self.rowcount = -1

    def __enter__(self) -> "_RowFailureCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: object, params: object = None) -> None:
        text = _sql_text(statement).strip()
        if "information_schema.columns" in text:
            self.events.append("columns")
            return
        if text.startswith(("SAVEPOINT", "ROLLBACK TO SAVEPOINT", "RELEASE SAVEPOINT")):
            self.events.append(text)
            return

        self.insert_attempts += 1
        self.insert_params.append(params)
        self.events.append(f"insert-{self.insert_attempts}")
        outcome = self.outcomes[self.insert_attempts - 1]
        if isinstance(outcome, Exception):
            raise outcome
        self.rowcount = outcome

    def fetchall(self) -> list[tuple[str]]:
        return [
            ("ticker",),
            ("period_end",),
            ("timeframe",),
            ("total_assets",),
        ]


class _TransactionConnection:
    def __init__(
        self,
        *,
        fail_commit: bool = False,
        outcomes: list[int | Exception] | None = None,
    ) -> None:
        self.events: list[str] = []
        self.cursor_instance = _RowFailureCursor(self.events, outcomes)
        self.fail_commit = fail_commit

    def cursor(self) -> _RowFailureCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.events.append("commit")
        if self.fail_commit:
            raise RuntimeError("commit failed")

    def rollback(self) -> None:
        self.events.append("whole-transaction-rollback")


class _RatiosClient:
    def get_ratios(self, symbol: str) -> list[dict[str, object]]:
        return [
            {"date": "2026-01-01", "price": 10},
            {"date": "2026-01-02", "price": 11},
            {"date": "2026-01-03", "price": 12},
        ]


class _FundamentalsClient:
    def get_fundamentals(self, endpoint: str, **kwargs: object) -> list[dict[str, Any]]:
        if endpoint != "balance-sheets":
            return []
        return [
            {"tickers": ["TEST"], "period_end": "2026-03-31", "total_assets": 10},
            {"tickers": ["TEST"], "period_end": "2026-06-30", "total_assets": 11},
            {"tickers": ["TEST"], "period_end": "2026-09-30", "total_assets": 12},
        ]


def _assert_savepoint_recovery(events: list[str], name: str) -> None:
    assert events == [
        f"SAVEPOINT {name}",
        "insert-1",
        f"RELEASE SAVEPOINT {name}",
        f"SAVEPOINT {name}",
        "insert-2",
        f"ROLLBACK TO SAVEPOINT {name}",
        f"RELEASE SAVEPOINT {name}",
        f"SAVEPOINT {name}",
        "insert-3",
        f"RELEASE SAVEPOINT {name}",
        "commit",
    ]


def test_ratios_row_failure_preserves_successes_and_returns_committed_count() -> None:
    conn = _TransactionConnection()

    inserted = fetch_and_insert_ratios(
        conn,
        _RatiosClient(),  # type: ignore[arg-type]
        "TEST",
        logging.getLogger(__name__),
    )

    assert inserted == 2
    assert inserted.failed_rows == 1
    assert "whole-transaction-rollback" not in conn.events
    _assert_savepoint_recovery(conn.events, "ratio_row")


def test_fundamental_row_failure_preserves_successes_and_truthful_stats() -> None:
    conn = _TransactionConnection()

    stats = fetch_and_insert_fundamentals(
        conn,
        _FundamentalsClient(),  # type: ignore[arg-type]
        "TEST",
        "2026-01-01",
        "2026-12-31",
        logging.getLogger(__name__),
    )

    assert stats == {"balance_sheets": 2, "income_statements": 0, "cash_flows": 0}
    assert stats.failures == {
        "balance_sheets": "1 of 3 provider rows were rejected or failed to persist"
    }
    assert conn.events[0] == "columns"
    assert "whole-transaction-rollback" not in conn.events
    _assert_savepoint_recovery(conn.events[1:], "fundamental_row")


def test_fundamental_do_nothing_rowcount_is_excluded_from_committed_stats() -> None:
    conn = _TransactionConnection(outcomes=[1, 0, 1])

    stats = fetch_and_insert_fundamentals(
        conn,
        _FundamentalsClient(),  # type: ignore[arg-type]
        "TEST",
        "2026-01-01",
        "2026-12-31",
        logging.getLogger(__name__),
    )

    assert stats["balance_sheets"] == 2
    assert "ROLLBACK TO SAVEPOINT fundamental_row" not in conn.events
    assert conn.events[-1] == "commit"
    assert stats.failures == {}


def test_all_ratio_row_failures_are_exposed_after_transaction_recovery() -> None:
    failures = [psycopg.errors.CheckViolation("bad row") for _ in range(3)]
    conn = _TransactionConnection(outcomes=failures)

    inserted = fetch_and_insert_ratios(
        conn,
        _RatiosClient(),  # type: ignore[arg-type]
        "TEST",
        logging.getLogger(__name__),
    )

    assert inserted == 0
    assert inserted.failed_rows == 3
    assert conn.events[-1] == "commit"


def test_all_fundamental_row_failures_are_exposed() -> None:
    failures = [psycopg.errors.CheckViolation("bad row") for _ in range(3)]
    conn = _TransactionConnection(outcomes=failures)

    stats = fetch_and_insert_fundamentals(
        conn,
        _FundamentalsClient(),  # type: ignore[arg-type]
        "TEST",
        "2026-01-01",
        "2026-12-31",
        logging.getLogger(__name__),
    )

    assert stats["balance_sheets"] == 0
    assert stats.failures == {
        "balance_sheets": "3 of 3 provider rows were rejected or failed to persist"
    }


def test_fundamentals_reject_mismatched_provider_ticker_without_mutation() -> None:
    record: dict[str, Any] = {
        "tickers": ["MSFT"],
        "period_end": "2026-03-31",
        "total_assets": 10,
    }

    class MismatchedClient:
        def get_fundamentals(
            self, endpoint: str, **kwargs: object
        ) -> list[dict[str, Any]]:
            return [record] if endpoint == "balance-sheets" else []

    conn = _TransactionConnection()
    stats = fetch_and_insert_fundamentals(
        conn,
        MismatchedClient(),  # type: ignore[arg-type]
        "AAPL",
        "2026-01-01",
        "2026-12-31",
        logging.getLogger(__name__),
    )

    assert stats["balance_sheets"] == 0
    assert stats.failures == {
        "balance_sheets": "1 of 1 provider rows were rejected or failed to persist"
    }
    assert record == {
        "tickers": ["MSFT"],
        "period_end": "2026-03-31",
        "total_assets": 10,
    }
    assert conn.cursor_instance.insert_attempts == 0


def test_fundamentals_reject_mixed_provider_tickers_without_writing() -> None:
    record: dict[str, Any] = {
        "tickers": ["AAPL", "MSFT"],
        "period_end": "2026-03-31",
        "total_assets": 10,
    }

    class MixedClient:
        def get_fundamentals(
            self, endpoint: str, **kwargs: object
        ) -> list[dict[str, Any]]:
            return [record] if endpoint == "balance-sheets" else []

    conn = _TransactionConnection()
    stats = fetch_and_insert_fundamentals(
        conn,
        MixedClient(),  # type: ignore[arg-type]
        "AAPL",
        "2026-01-01",
        "2026-12-31",
        logging.getLogger(__name__),
    )

    assert stats["balance_sheets"] == 0
    assert conn.cursor_instance.insert_attempts == 0
    assert record["tickers"] == ["AAPL", "MSFT"]


@pytest.mark.parametrize(
    "identity_fields",
    [
        {"ticker": "MSFT"},
        {"ticker": "AAPL", "tickers": ["MSFT"]},
    ],
)
def test_ratios_reject_mismatched_or_mixed_identity_before_database_access(
    identity_fields: dict[str, object],
) -> None:
    record: dict[str, object] = {
        **identity_fields,
        "date": "2026-01-01",
        "price": 10,
    }

    class IdentityClient:
        def get_ratios(self, symbol: str) -> list[dict[str, object]]:
            return [record]

    class NoDatabaseAccess:
        def cursor(self) -> object:
            raise AssertionError("mismatched provider rows must not reach the database")

    with pytest.raises(ProviderError):
        fetch_and_insert_ratios(
            NoDatabaseAccess(),
            IdentityClient(),  # type: ignore[arg-type]
            "AAPL",
            logging.getLogger(__name__),
        )

    assert all(record[key] == value for key, value in identity_fields.items())


def test_commit_failure_propagates_instead_of_returning_uncommitted_count() -> None:
    conn = _TransactionConnection(fail_commit=True)

    with pytest.raises(RuntimeError, match="commit failed"):
        fetch_and_insert_ratios(
            conn,
            _RatiosClient(),  # type: ignore[arg-type]
            "TEST",
            logging.getLogger(__name__),
        )

    assert conn.events[-1] == "commit"
    assert "whole-transaction-rollback" not in conn.events


def test_fundamental_endpoint_failure_is_exposed_while_peers_continue() -> None:
    class PartiallyFailingClient:
        def get_fundamentals(
            self, endpoint: str, **kwargs: object
        ) -> list[dict[str, Any]]:
            if endpoint == "income-statements":
                raise RuntimeError("token=super-secret upstream unavailable")
            return []

    conn = _TransactionConnection()
    stats = fetch_and_insert_fundamentals(
        conn,
        PartiallyFailingClient(),  # type: ignore[arg-type]
        "TEST",
        "2026-01-01",
        "2026-12-31",
        logging.getLogger(__name__),
    )

    assert stats == {"balance_sheets": 0, "income_statements": 0, "cash_flows": 0}
    assert stats.failures == {
        "income_statements": "token=<redacted> upstream unavailable"
    }


def test_add_symbol_price_loader_rejects_all_malformed_rows_before_db() -> None:
    class MalformedPriceClient:
        def get(self, *args: object, **kwargs: object) -> dict[str, Any]:
            return {
                "results": [
                    {
                        "t": 1781265600000,
                        "o": True,
                        "h": 2,
                        "l": 1,
                        "c": 2,
                        "v": 100,
                    }
                ]
            }

    class NoDatabaseAccess:
        def cursor(self) -> object:
            raise AssertionError("invalid provider rows must not reach the database")

    with pytest.raises(ProviderError, match="no valid rows"):
        fetch_and_insert_prices(
            NoDatabaseAccess(),
            MalformedPriceClient(),  # type: ignore[arg-type]
            "TEST",
            "2026-06-12",
            "2026-06-12",
            logging.getLogger(__name__),
        )


def test_add_symbol_price_loader_treats_empty_history_as_primary_failure() -> None:
    class EmptyPriceClient:
        def get(self, *args: object, **kwargs: object) -> dict[str, Any]:
            return {"results": []}

    class NoDatabaseAccess:
        def cursor(self) -> object:
            raise AssertionError("empty provider results must not reach the database")

    with pytest.raises(ProviderError, match="returned no rows"):
        fetch_and_insert_prices(
            NoDatabaseAccess(),
            EmptyPriceClient(),  # type: ignore[arg-type]
            "TEST",
            "2026-06-12",
            "2026-06-12",
            logging.getLogger(__name__),
        )


class _ContextConnection:
    def __enter__(self) -> "_ContextConnection":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _AddSymbolClient:
    def __init__(self, *args: object, **kwargs: object) -> None:
        pass

    def get_ticker_details(self, symbol: str) -> dict[str, Any]:
        return {"ticker": symbol, "name": symbol}


def _empty_fundamentals() -> FundamentalLoadResult:
    result = FundamentalLoadResult()
    result.update(balance_sheets=0, income_statements=0, cash_flows=0)
    return result


def test_total_primary_price_failure_returns_unsuccessful_status() -> None:
    with mock.patch.object(add_symbol, "PolygonClient", _AddSymbolClient), mock.patch.object(
        add_symbol, "SyncRateLimiter"
    ), mock.patch.object(
        add_symbol.psycopg, "connect", return_value=_ContextConnection()
    ), mock.patch.object(
        add_symbol, "get_existing_symbols", return_value=set()
    ), mock.patch.object(
        add_symbol, "insert_company", return_value=True
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_prices", side_effect=RuntimeError("outage")
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_ratios", return_value=0
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_fundamentals", return_value=_empty_fundamentals()
    ):
        stats = run_add_symbols(
            "key", "test-only", ["AAPL"], logger=logging.getLogger(__name__)
        )

    assert stats["success"] is False
    assert stats["degraded"] is True
    assert stats["failed"] == ["AAPL"]
    assert stats["feed_failures"]["AAPL"] == {"prices": "outage"}


def test_auxiliary_feed_failure_is_degraded_but_symbol_succeeds() -> None:
    def ratios(conn: object, client: object, symbol: str, logger: object) -> int:
        if symbol == "AAPL":
            raise RuntimeError("temporary ratio outage")
        return 0

    with mock.patch.object(add_symbol, "PolygonClient", _AddSymbolClient), mock.patch.object(
        add_symbol, "SyncRateLimiter"
    ), mock.patch.object(
        add_symbol.psycopg, "connect", return_value=_ContextConnection()
    ), mock.patch.object(
        add_symbol, "get_existing_symbols", return_value=set()
    ), mock.patch.object(
        add_symbol, "insert_company", return_value=True
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_prices", return_value=1
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_ratios", side_effect=ratios
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_fundamentals", return_value=_empty_fundamentals()
    ):
        stats = run_add_symbols(
            "key",
            "test-only",
            ["aapl", "MSFT"],
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is True
    assert stats["degraded"] is True
    assert stats["failed"] == []
    assert stats["added"] == ["AAPL", "MSFT"]
    assert stats["degraded_symbols"] == ["AAPL"]


def test_ratio_row_failures_degrade_an_otherwise_successful_symbol() -> None:
    with mock.patch.object(add_symbol, "PolygonClient", _AddSymbolClient), mock.patch.object(
        add_symbol, "SyncRateLimiter"
    ), mock.patch.object(
        add_symbol.psycopg, "connect", return_value=_ContextConnection()
    ), mock.patch.object(
        add_symbol, "get_existing_symbols", return_value=set()
    ), mock.patch.object(
        add_symbol, "insert_company", return_value=True
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_prices", return_value=1
    ), mock.patch.object(
        add_symbol,
        "fetch_and_insert_ratios",
        return_value=RatioLoadResult(0, failed_rows=3),
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_fundamentals", return_value=_empty_fundamentals()
    ):
        stats = run_add_symbols(
            "key", "test-only", ["AAPL"], logger=logging.getLogger(__name__)
        )

    assert stats["success"] is True
    assert stats["degraded"] is True
    assert stats["feed_failures"]["AAPL"] == {
        "ratios": "3 of 3 ratio rows failed to persist"
    }


def test_existing_symbol_continues_when_company_refresh_fails() -> None:
    class CompanyOutageClient(_AddSymbolClient):
        def get_ticker_details(self, symbol: str) -> dict[str, Any]:
            raise RuntimeError("company provider unavailable")

    with mock.patch.object(
        add_symbol, "PolygonClient", CompanyOutageClient
    ), mock.patch.object(add_symbol, "SyncRateLimiter"), mock.patch.object(
        add_symbol.psycopg, "connect", return_value=_ContextConnection()
    ), mock.patch.object(
        add_symbol, "get_existing_symbols", return_value={"AAPL"}
    ), mock.patch.object(
        add_symbol, "insert_company"
    ) as insert_company, mock.patch.object(
        add_symbol, "fetch_and_insert_prices", return_value=1
    ) as prices, mock.patch.object(
        add_symbol, "fetch_and_insert_ratios", return_value=RatioLoadResult(0)
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_fundamentals", return_value=_empty_fundamentals()
    ):
        stats = run_add_symbols(
            "key", "test-only", ["AAPL"], logger=logging.getLogger(__name__)
        )

    insert_company.assert_not_called()
    prices.assert_called_once()
    assert stats["success"] is True
    assert stats["skipped"] == ["AAPL"]
    assert stats["feed_failures"]["AAPL"] == {
        "company": "company provider unavailable"
    }


def test_new_symbol_company_write_failure_blocks_downstream_feeds() -> None:
    with mock.patch.object(add_symbol, "PolygonClient", _AddSymbolClient), mock.patch.object(
        add_symbol, "SyncRateLimiter"
    ), mock.patch.object(
        add_symbol.psycopg, "connect", return_value=_ContextConnection()
    ), mock.patch.object(
        add_symbol, "get_existing_symbols", return_value=set()
    ), mock.patch.object(
        add_symbol, "insert_company", return_value=False
    ), mock.patch.object(
        add_symbol, "fetch_and_insert_prices"
    ) as prices:
        stats = run_add_symbols(
            "key", "test-only", ["AAPL"], logger=logging.getLogger(__name__)
        )

    prices.assert_not_called()
    assert stats["success"] is False
    assert stats["failed"] == ["AAPL"]
    assert stats["feed_failures"]["AAPL"] == {
        "company": "Company record was not persisted"
    }


def test_new_symbol_rejects_mismatched_company_identity_before_write() -> None:
    source = {"ticker": "MSFT", "name": "Wrong company"}

    class MismatchedCompanyClient(_AddSymbolClient):
        def get_ticker_details(self, symbol: str) -> dict[str, Any]:
            return source

    with mock.patch.object(
        add_symbol, "PolygonClient", MismatchedCompanyClient
    ), mock.patch.object(add_symbol, "SyncRateLimiter"), mock.patch.object(
        add_symbol.psycopg, "connect", return_value=_ContextConnection()
    ), mock.patch.object(
        add_symbol, "get_existing_symbols", return_value=set()
    ), mock.patch.object(
        add_symbol, "insert_company"
    ) as insert_company, mock.patch.object(
        add_symbol, "fetch_and_insert_prices"
    ) as prices:
        stats = run_add_symbols(
            "key", "test-only", ["AAPL"], logger=logging.getLogger(__name__)
        )

    insert_company.assert_not_called()
    prices.assert_not_called()
    assert stats["success"] is False
    assert stats["failed"] == ["AAPL"]
    assert "mismatched ticker identity" in stats["feed_failures"]["AAPL"]["company"]
    assert source == {"ticker": "MSFT", "name": "Wrong company"}
