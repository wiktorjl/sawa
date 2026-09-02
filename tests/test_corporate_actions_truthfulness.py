"""Offline truthfulness and transaction tests for corporate-action updates."""

from __future__ import annotations

import logging
from contextlib import ExitStack
from datetime import date, timedelta
from types import SimpleNamespace
from unittest import mock

import pytest

from sawa import cli
from sawa import corporate_actions as actions
from sawa.domain.corporate_actions import (
    Dividend,
    Earnings,
    StockSplit,
    is_unrepresentable_split_ratio,
)


def _connection(*, rollback_on_error_exit: bool = False) -> mock.MagicMock:
    """Return a database-shaped mock without opening a database."""
    conn = mock.MagicMock(name="offline_corporate_actions_connection")
    conn.__enter__.return_value = conn

    if rollback_on_error_exit:

        def exit_transaction(exc_type, _exc, _traceback) -> bool:
            if exc_type is not None:
                conn.rollback()
            return False

        conn.__exit__.side_effect = exit_transaction
    else:
        conn.__exit__.return_value = False
    return conn


def _client() -> mock.MagicMock:
    return mock.MagicMock(name="offline_polygon_client")


def _dependency_patches(
    conn: mock.MagicMock,
    client: mock.MagicMock,
) -> ExitStack:
    stack = ExitStack()
    stack.enter_context(mock.patch.object(actions.psycopg, "connect", return_value=conn))
    stack.enter_context(mock.patch.object(actions, "PolygonClient", return_value=client))
    stack.enter_context(
        mock.patch.object(actions, "SyncRateLimiter", return_value=mock.MagicMock())
    )
    return stack


def _split(ticker: str = "AAPL") -> dict[str, object]:
    return {
        "ticker": ticker,
        "execution_date": "2026-08-01",
        "split_from": 1,
        "split_to": 4,
    }


def _dividend(ticker: str = "AAPL") -> dict[str, object]:
    return {
        "ticker": ticker,
        "ex_dividend_date": "2026-08-15",
        "cash_amount": "0.25",
        "dividend_type": "CD",
        "frequency": 4,
    }


@pytest.mark.parametrize(
    ("tickers", "feeds", "message"),
    [
        (
            ["AAPL"],
            {
                "include_splits": False,
                "include_dividends": False,
                "include_earnings": False,
            },
            "at least one corporate-action feed",
        ),
        (
            [],
            {
                "include_splits": True,
                "include_dividends": False,
                "include_earnings": False,
            },
            "ticker list is empty",
        ),
    ],
)
def test_invalid_request_is_rejected_before_client_or_database(
    tickers: list[str],
    feeds: dict[str, bool],
    message: str,
) -> None:
    with (
        mock.patch.object(actions, "PolygonClient") as client_factory,
        mock.patch.object(actions.psycopg, "connect") as connect,
        mock.patch.object(actions, "SyncRateLimiter") as limiter_factory,
        pytest.raises(ValueError, match=message),
    ):
        actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
            tickers=tickers,
            **feeds,
        )

    client_factory.assert_not_called()
    limiter_factory.assert_not_called()
    connect.assert_not_called()


def test_all_earnings_requests_fail_truthfully_and_redact_secrets(caplog) -> None:
    conn = _connection()
    client = _client()
    first_secret = "earnings-bearer-secret"
    second_secret = "earnings-query-secret"
    client.get_ticker_events.side_effect = [
        RuntimeError(f"Authorization: Bearer {first_secret}"),
        ValueError(f"https://provider.invalid/events?apiKey={second_secret}"),
    ]
    logger = logging.getLogger("test-corporate-earnings-failure")

    with _dependency_patches(conn, client), caplog.at_level(
        logging.DEBUG, logger=logger.name
    ):
        stats = actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
            tickers=["MSFT", "AAPL"],
            include_splits=False,
            include_dividends=False,
            include_earnings=True,
            logger=logger,
        )

    requests = stats["earnings_requests"]
    assert requests["requested"] == 2
    assert requests["succeeded"] == 0
    assert requests["failed"] == 2
    assert requests["requested"] == requests["succeeded"] + requests["failed"]
    assert requests["empty"] == 0
    assert stats["earnings_loaded"] == 0
    assert stats["split_tickers"] == []
    assert stats["success"] is False
    assert stats["degraded"] is True
    assert stats["rolled_back"] is True
    conn.rollback.assert_called_once_with()
    conn.commit.assert_not_called()

    rendered = f"{stats!r}\n{caplog.text}"
    assert first_secret not in rendered
    assert second_secret not in rendered
    assert "<redacted>" in rendered


def test_incomplete_split_persistence_rolls_back_but_retains_attempt_counts() -> None:
    conn = _connection()
    client = _client()
    client.get_splits.return_value = [_split(), _split() | {"execution_date": "2026-08-02"}]
    persistence = actions.ActionPersistenceResult(
        1,
        source_rows=2,
        rejected_rows=1,
        persisted_tickers=["AAPL"],
    )

    with (
        _dependency_patches(conn, client),
        mock.patch.object(actions, "load_splits", return_value=persistence) as load_splits,
    ):
        stats = actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
            tickers=["AAPL"],
            include_splits=True,
            include_dividends=False,
        )

    load_splits.assert_called_once()
    assert load_splits.call_args.kwargs["commit"] is False
    assert stats["splits_fetched"] == 2
    assert stats["splits_loaded"] == 0
    assert stats["split_tickers"] == []
    assert stats["splits_persistence"] == {
        "source_rows": 2,
        "attempted_rows": 1,
        "committed_rows": 0,
        "rejected_rows": 1,
    }
    assert stats["success"] is False
    assert stats["degraded"] is True
    conn.rollback.assert_called_once_with()
    conn.commit.assert_not_called()


def test_successful_split_and_dividend_transaction_commits_once() -> None:
    conn = _connection()
    client = _client()
    client.get_splits.return_value = [_split("AAPL")]
    client.get_dividends.return_value = [_dividend("MSFT")]
    split_result = actions.ActionPersistenceResult(
        1,
        source_rows=1,
        persisted_tickers=["AAPL"],
    )
    dividend_result = actions.ActionPersistenceResult(1, source_rows=1)

    with (
        _dependency_patches(conn, client),
        mock.patch.object(actions, "load_splits", return_value=split_result) as load_splits,
        mock.patch.object(
            actions, "load_dividends", return_value=dividend_result
        ) as load_dividends,
    ):
        stats = actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
            tickers=["MSFT", "AAPL"],
            include_splits=True,
            include_dividends=True,
        )

    assert load_splits.call_args.kwargs["commit"] is False
    assert load_dividends.call_args.kwargs["commit"] is False
    assert stats["success"] is True
    assert stats["degraded"] is False
    assert stats["splits_loaded"] == 1
    assert stats["dividends_loaded"] == 1
    assert stats["split_tickers"] == ["AAPL"]
    assert stats["splits_persistence"]["committed_rows"] == 1
    assert stats["dividends_persistence"]["committed_rows"] == 1
    conn.commit.assert_called_once_with()
    conn.rollback.assert_not_called()


def test_untracked_instruments_cannot_abort_the_batch() -> None:
    """Out-of-universe provider records must never fail a tracked ticker's load.

    Polygon's feeds carry structured-product identifiers and money-market funds
    whose values the schema cannot represent. They are dropped by the ticker
    filter anyway, but parsing them first let one of them abort everything, so
    no splits or dividends loaded at all.
    """
    conn = _connection()
    client = _client()
    client.get_splits.return_value = [
        _split("AAPL"),
        _split("NIPMY") | {"split_to": 1.5},          # fund reorg, fractional
    ]
    client.get_dividends.return_value = [
        _dividend("MSFT"),
        _dividend("VIIT0142"),                        # not a valid ticker
        _dividend("PRTPX") | {"cash_amount": 4.76e-07},  # unrepresentable amount
        _dividend("YMAX") | {"frequency": 52},        # weekly income ETF
    ]
    split_result = actions.ActionPersistenceResult(
        1, source_rows=1, persisted_tickers=["AAPL"]
    )
    dividend_result = actions.ActionPersistenceResult(1, source_rows=1)

    with (
        _dependency_patches(conn, client),
        mock.patch.object(actions, "load_splits", return_value=split_result),
        mock.patch.object(
            actions, "load_dividends", return_value=dividend_result
        ) as load_dividends,
    ):
        stats = actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
            tickers=["MSFT", "AAPL", "YMAX"],
            include_splits=True,
            include_dividends=True,
        )

    assert stats["success"] is True
    # The tracked weekly-paying ETF survives; the untracked instruments do not.
    loaded = load_dividends.call_args.args[1]
    assert sorted(d.ticker for d in loaded) == ["MSFT", "YMAX"]
    assert stats["splits_eligible"] == 1


def test_default_annual_all_empty_feeds_fail_and_roll_back() -> None:
    conn = _connection()
    cursor = mock.MagicMock(name="active_ticker_cursor")
    cursor.fetchall.return_value = [("AAPL",)]
    conn.cursor.return_value.__enter__.return_value = cursor
    client = _client()
    client.get_splits.return_value = []
    client.get_dividends.return_value = []

    with _dependency_patches(conn, client):
        stats = actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
        )

    assert stats["splits_fetched"] == 0
    assert stats["dividends_fetched"] == 0
    assert stats["feed_expectations"] == {
        "splits_nonempty_required": True,
        "dividends_nonempty_required": True,
        "history_days": 365,
    }
    assert stats["success"] is False
    assert stats["degraded"] is True
    assert len(stats["errors"]) == 2
    assert stats["rolled_back"] is True
    conn.rollback.assert_called_once_with()
    conn.commit.assert_not_called()


def test_short_daily_split_only_empty_window_is_successful() -> None:
    conn = _connection()
    client = _client()
    client.get_splits.return_value = []

    with _dependency_patches(conn, client):
        stats = actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
            start_date=date.today() - timedelta(days=14),
            tickers=["AAPL"],
            include_splits=True,
            include_dividends=False,
        )

    assert stats["feed_expectations"]["splits_nonempty_required"] is False
    assert stats["splits_fetched"] == 0
    assert stats["success"] is True
    assert stats["degraded"] is False
    assert stats["errors"] == []
    conn.commit.assert_called_once_with()
    conn.rollback.assert_not_called()


@pytest.mark.parametrize(
    ("splits", "dividends", "missing_feed"),
    [
        ([_split()], [], "dividend provider returned no rows"),
        ([], [_dividend()], "split provider returned no rows"),
    ],
)
def test_default_annual_one_sided_empty_feed_fails_independently(
    splits: list[dict[str, object]],
    dividends: list[dict[str, object]],
    missing_feed: str,
) -> None:
    conn = _connection()
    cursor = mock.MagicMock(name="active_ticker_cursor")
    cursor.fetchall.return_value = [("AAPL",)]
    conn.cursor.return_value.__enter__.return_value = cursor
    client = _client()
    client.get_splits.return_value = splits
    client.get_dividends.return_value = dividends
    split_result = actions.ActionPersistenceResult(
        len(splits),
        source_rows=len(splits),
        persisted_tickers=["AAPL"] if splits else [],
    )
    dividend_result = actions.ActionPersistenceResult(
        len(dividends),
        source_rows=len(dividends),
    )

    with (
        _dependency_patches(conn, client),
        mock.patch.object(actions, "load_splits", return_value=split_result),
        mock.patch.object(actions, "load_dividends", return_value=dividend_result),
    ):
        stats = actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
        )

    assert stats["success"] is False
    assert any(missing_feed in error for error in stats["errors"])
    assert stats["splits_loaded"] == 0
    assert stats["dividends_loaded"] == 0
    assert stats["split_tickers"] == []
    conn.rollback.assert_called_once_with()
    conn.commit.assert_not_called()


def test_later_dividend_error_rolls_back_staged_split_transaction() -> None:
    conn = _connection(rollback_on_error_exit=True)
    client = _client()
    client.get_splits.return_value = [_split()]
    client.get_dividends.return_value = [_dividend()]
    split_result = actions.ActionPersistenceResult(
        1,
        source_rows=1,
        persisted_tickers=["AAPL"],
    )

    with (
        _dependency_patches(conn, client),
        mock.patch.object(actions, "load_splits", return_value=split_result) as load_splits,
        mock.patch.object(
            actions,
            "load_dividends",
            side_effect=RuntimeError("later dividend write failed"),
        ),
        pytest.raises(RuntimeError, match="later dividend write failed"),
    ):
        actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
            tickers=["AAPL"],
            include_splits=True,
            include_dividends=True,
        )

    assert load_splits.call_args.kwargs["commit"] is False
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once_with()


def test_dividend_upsert_uses_nullable_identity_conflict_expression() -> None:
    conn = _connection()
    cursor = mock.MagicMock(name="offline_dividend_cursor")
    conn.cursor.return_value.__enter__.return_value = cursor
    dividend = Dividend.from_polygon(_dividend())

    result = actions.load_dividends(
        conn,
        [dividend],
        logging.getLogger("test-dividend-conflict-target"),
        commit=False,
    )

    insert_sql = next(
        call.args[0]
        for call in cursor.execute.call_args_list
        if "INSERT INTO dividends" in call.args[0]
    )
    normalized_sql = " ".join(insert_sql.split()).replace("( ", "(").replace(" )", ")")
    assert (
        "ON CONFLICT (ticker, ex_dividend_date, "
        "(COALESCE(dividend_type, ''::character varying))) DO UPDATE"
    ) in normalized_sql
    assert result == 1
    conn.commit.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        _split() | {"split_from": 0},
        _split() | {"split_to": float("inf")},
    ],
)
def test_split_parser_rejects_nonpositive_or_nonfinite_ratios(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        StockSplit.from_polygon(payload)


@pytest.mark.parametrize(
    "payload",
    [
        _split() | {"split_to": 1.5},
        _split() | {"split_to": 0.9668},
        _split() | {"split_from": 2.5, "split_to": 3},
    ],
)
def test_fund_reorganization_ratios_are_skippable_not_malformed(
    payload: dict[str, object],
) -> None:
    """Fractional ratios are unrepresentable in the integer schema, not corrupt.

    One of these aborted the whole corporate-actions batch, including the real
    equity splits that drive price/TA repair.
    """
    assert is_unrepresentable_split_ratio(payload) is True


@pytest.mark.parametrize(
    "payload",
    [
        _split() | {"split_from": 0},
        _split() | {"split_to": -2},
        _split() | {"split_to": float("inf")},
        _split() | {"split_to": "3"},
        _split() | {"split_to": None},
        42,
    ],
)
def test_malformed_split_ratios_stay_fatal(payload: object) -> None:
    """Only fractional ratios are skipped; anything else must still raise."""
    assert is_unrepresentable_split_ratio(payload) is False


def test_weekly_and_semimonthly_dividend_frequencies_are_accepted() -> None:
    """52 (weekly) and 24 (semi-monthly) are ordinary income-ETF schedules.

    Rejecting them aborted the whole dividend batch, so nothing loaded at all.
    """
    for frequency in (0, 1, 2, 4, 12, 24, 52):
        parsed = Dividend.from_polygon(_dividend() | {"frequency": frequency})
        assert parsed.frequency == frequency


@pytest.mark.parametrize("frequency", [3, 7, 53, -1, 365])
def test_bogus_dividend_frequency_is_still_rejected(frequency: int) -> None:
    with pytest.raises(ValueError, match="frequency must be one of"):
        Dividend.from_polygon(_dividend() | {"frequency": frequency})


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"cash_amount": "NaN"}, "finite NUMERIC"),
        ({"cash_amount": "-0.01"}, "positive finite"),
        ({"frequency": 3}, "frequency must be one of"),
    ],
)
def test_dividend_parser_rejects_invalid_value_domains(
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        Dividend.from_polygon(_dividend() | overrides)


@pytest.mark.parametrize(
    "attributes",
    [
        {"eps_actual": "Infinity"},
        {"revenue_actual": "1.5"},
    ],
)
def test_earnings_parser_rejects_nonfinite_or_nonintegral_values(
    attributes: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        Earnings.from_polygon_event(
            "AAPL",
            {
                "type": "earnings",
                "date": "2026-08-15",
                "attributes": attributes,
            },
        )


def test_provider_action_outside_requested_window_rolls_back_before_load() -> None:
    conn = _connection(rollback_on_error_exit=True)
    client = _client()
    client.get_splits.return_value = [
        _split()
        | {"execution_date": (date.today() - timedelta(days=30)).isoformat()}
    ]

    with (
        _dependency_patches(conn, client),
        mock.patch.object(actions, "load_splits") as load_splits,
        pytest.raises(ValueError, match="outside the requested window"),
    ):
        actions.run_corporate_actions_update(
            api_key="offline-key",
            database_url="postgresql://unused.invalid/offline",
            start_date=date.today() - timedelta(days=7),
            tickers=["AAPL"],
            include_splits=True,
            include_dividends=False,
        )

    load_splits.assert_not_called()
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once_with()


def test_invalid_cli_ticker_does_not_dispatch_runner() -> None:
    args = SimpleNamespace(
        verbose=False,
        log_dir=None,
        api_key="offline-key",
        database_url="postgresql://unused.invalid/offline",
        start_date=None,
        ticker="AAPL; DROP TABLE companies",
        dividends_only=False,
        splits_only=False,
        include_earnings=False,
        dry_run=False,
    )

    with (
        mock.patch.object(cli, "setup_logging", return_value=mock.MagicMock()),
        mock.patch.object(actions, "run_corporate_actions_update") as run_update,
    ):
        return_code = cli.cmd_corporate_actions(args)

    assert return_code == 1
    run_update.assert_not_called()
