"""Intraday runner status must truthfully summarize stream telemetry."""

import logging
from typing import Any
from unittest import mock

from sawa import intraday


def _connection() -> mock.MagicMock:
    conn = mock.MagicMock(name="offline_connection")
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    return conn


def _stream_client(
    *,
    dropped_buffered_bars: int = 0,
    minute_events_received: int = 0,
    minute_events_accepted: int = 0,
    invalid_minute_events: int = 0,
    provider_status_errors: int = 0,
    malformed_messages: int = 0,
    reconnectable_failures: int = 0,
    stream_recovery_pending: bool = False,
) -> mock.MagicMock:
    client = mock.MagicMock()
    client.dropped_buffered_bars = dropped_buffered_bars
    client.minute_events_received = minute_events_received
    client.minute_events_accepted = minute_events_accepted
    client.invalid_minute_events = invalid_minute_events
    client.provider_status_errors = provider_status_errors
    client.malformed_messages = malformed_messages
    client.reconnectable_failures = reconnectable_failures
    client.stream_recovery_pending = stream_recovery_pending

    async def run() -> None:
        return None

    client.run = run
    return client


def _run_with_client(client: mock.MagicMock) -> dict[str, Any]:
    with mock.patch.object(
        intraday.psycopg, "connect", return_value=_connection()
    ), mock.patch.object(
        intraday, "get_symbols_from_db", return_value=["AAPL"]
    ), mock.patch.object(
        intraday, "PolygonWebSocketClient", return_value=client
    ):
        return intraday.run_intraday(
            api_key="offline-key",
            database_url="offline-db",
            logger=logging.getLogger(__name__),
        )


def test_intraday_runner_is_unsuccessful_after_buffered_bar_loss() -> None:
    stats = _run_with_client(_stream_client(dropped_buffered_bars=7))

    assert stats["success"] is False
    assert stats["degraded"] is True
    assert stats["dropped_buffered_bars"] == 7
    assert "historical recovery is required" in stats["error"]


def test_intraday_runner_rejects_stop_without_any_accepted_bar() -> None:
    stats = _run_with_client(_stream_client())

    assert stats["success"] is False
    assert stats["dropped_buffered_bars"] == 0
    assert stats["degraded"] is True
    assert "without accepting" in stats["error"]


def test_intraday_runner_rejects_stream_with_only_invalid_minute_events() -> None:
    stats = _run_with_client(
        _stream_client(
            minute_events_received=4,
            minute_events_accepted=0,
            invalid_minute_events=4,
        )
    )

    assert stats["success"] is False
    assert stats["degraded"] is True
    assert stats["minute_events_received"] == 4
    assert stats["minute_events_accepted"] == 0
    assert stats["invalid_minute_events"] == 4


def test_intraday_runner_degrades_mixed_valid_and_invalid_minute_events() -> None:
    stats = _run_with_client(
        _stream_client(
            minute_events_received=3,
            minute_events_accepted=2,
            invalid_minute_events=1,
        )
    )

    assert stats["success"] is True
    assert stats["degraded"] is True
    assert stats["minute_events_received"] == 3
    assert stats["minute_events_accepted"] == 2
    assert stats["invalid_minute_events"] == 1


def test_intraday_runner_rejects_unrecovered_provider_status_error() -> None:
    stats = _run_with_client(
        _stream_client(
            minute_events_received=1,
            minute_events_accepted=0,
            provider_status_errors=1,
        )
    )

    assert stats["success"] is False
    assert stats["degraded"] is True
    assert stats["provider_status_errors"] == 1
    assert stats["minute_events_accepted"] == 0


def test_intraday_runner_conservatively_rejects_unordered_provider_recovery() -> None:
    stats = _run_with_client(
        _stream_client(
            minute_events_received=2,
            minute_events_accepted=1,
            provider_status_errors=1,
        )
    )

    assert stats["success"] is False
    assert stats["degraded"] is True
    assert stats["provider_status_errors"] == 1
    assert stats["minute_events_accepted"] == 1


def test_intraday_runner_rejects_unrecovered_reconnect_after_valid_bar() -> None:
    stats = _run_with_client(
        _stream_client(
            minute_events_received=1,
            minute_events_accepted=1,
            reconnectable_failures=3,
            stream_recovery_pending=True,
        )
    )

    assert stats["success"] is False
    assert stats["degraded"] is True
    assert stats["reconnectable_failures"] == 3
    assert stats["stream_recovery_pending"] is True
    assert "proven recovered" in stats["error"]


def test_intraday_runner_degrades_reconnect_recovered_by_valid_bar() -> None:
    stats = _run_with_client(
        _stream_client(
            minute_events_received=2,
            minute_events_accepted=2,
            reconnectable_failures=1,
            stream_recovery_pending=False,
        )
    )

    assert stats["success"] is True
    assert stats["degraded"] is True
    assert stats["stream_recovery_pending"] is False
    assert "reconnectable stream failure" in stats["degraded_reasons"][0]
