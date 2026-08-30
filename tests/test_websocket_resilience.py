"""WebSocket ingestion lifecycle regressions with mocked I/O."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import psycopg
import pytest

from sawa.api import websocket_client
from sawa.api.websocket_client import PolygonWebSocketClient


class _FakeWebSocket:
    def __init__(self, response: object | None = None) -> None:
        self.response = response
        self.sent: list[dict[str, object]] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def recv(self) -> str:
        return json.dumps(self.response)

    async def close(self) -> None:
        self.closed = True


class _ScriptedWebSocket:
    def __init__(
        self,
        responses: list[str | None],
        stream_messages: list[str] | None = None,
    ) -> None:
        self.responses = responses
        self.stream_messages = stream_messages or []
        self.sent: list[dict[str, object]] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def recv(self) -> str:
        response = self.responses.pop(0)
        if response is None:
            await asyncio.Future()
        return cast(str, response)

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self) -> _ScriptedWebSocket:
        return self

    async def __anext__(self) -> str:
        if not self.stream_messages:
            raise StopAsyncIteration
        return self.stream_messages.pop(0)


class _ConnectionContext:
    def __enter__(self) -> object:
        return object()

    def __exit__(self, *args: object) -> None:
        return None


class _FrozenDateTime(datetime):
    current = datetime(2026, 8, 28, 15, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz: object | None = None) -> Any:
        if tz is None:
            return cls.current.replace(tzinfo=None)
        return cls.current


def _client(
    *,
    bar_size: int = 5,
    connect_timeout: float = websocket_client.DEFAULT_CONNECT_TIMEOUT_SECONDS,
    handshake_timeout: float = websocket_client.DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
    max_buffered_bars: int = websocket_client.DEFAULT_MAX_BUFFERED_BARS,
) -> PolygonWebSocketClient:
    return PolygonWebSocketClient(
        api_key="test-only",
        database_url="postgresql://unused",
        tickers=["aapl"],
        bar_size=bar_size,
        logger=logging.getLogger(__name__),
        connect_timeout=connect_timeout,
        handshake_timeout=handshake_timeout,
        max_buffered_bars=max_buffered_bars,
    )


def _bar() -> dict[str, object]:
    timestamp = datetime(2026, 1, 2, 14, 37, tzinfo=timezone.utc)
    return {
        "sym": "AAPL",
        "s": int(timestamp.timestamp() * 1000),
        "o": 100.0,
        "h": 102.0,
        "l": 99.0,
        "c": 101.0,
        "v": 500,
    }


def _buffered_bar() -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "timestamp": datetime(2026, 1, 2, 14, 0, tzinfo=timezone.utc),
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 500,
        "bar_size_minutes": 15,
    }


def _minute(
    hour: int,
    minute: int,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    day: int = 28,
) -> dict[str, object]:
    timestamp = datetime(2026, 8, day, hour, minute, tzinfo=timezone.utc)
    return {
        "sym": "AAPL",
        "s": int(timestamp.timestamp() * 1000),
        "o": open_,
        "h": high,
        "l": low,
        "c": close,
        "v": volume,
    }


def test_aggregated_bar_carries_configured_bar_size() -> None:
    client = _client(bar_size=15)

    client._aggregate_bar(_bar())
    client._flush_completed_bars(flush_all=True)

    assert len(client.buffer) == 1
    assert client.buffer[0]["bar_size_minutes"] == 15


@pytest.mark.asyncio
async def test_unaligned_or_noninteger_timestamp_cannot_mutate_or_recover_stream() -> None:
    client = _client(bar_size=5)
    client.stream_recovery_pending = True
    aligned_start = cast(int, _bar()["s"])
    malformed = []
    for start_ms in (
        aligned_start + 30_000,
        aligned_start + 0.5,
        float(aligned_start),
        True,
    ):
        malformed.append({"ev": "AM", **_bar(), "s": start_ms})

    await client._handle_message(json.dumps(malformed))

    assert client.minute_events_received == 4
    assert client.minute_events_accepted == 0
    assert client.invalid_minute_events == 4
    assert client.stream_recovery_pending is True
    assert client.bar_aggregator == {}
    assert client.max_event_time_by_ticker == {}
    assert client.last_received_wallclock_by_ticker == {}
    assert client.latest_session_by_ticker == {}
    assert client.finalized_through == {}
    assert client.buffer == []


def test_minutes_are_deduplicated_ordered_and_corrected_authoritatively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 28, 13, 50, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=5)

    # Deliberately arrive out of order, then repeat the latest minute exactly.
    latest = _minute(
        13, 31, open_=105, high=110, low=104, close=109, volume=70
    )
    earliest = _minute(
        13, 30, open_=100, high=120, low=99, close=105, volume=50
    )
    client._aggregate_bar(latest)
    client._aggregate_bar(earliest)
    client._aggregate_bar(latest)
    client._aggregate_bar(
        _minute(13, 35, open_=109, high=111, low=108, close=110, volume=20)
    )
    client._flush_completed_bars()

    assert len(client.buffer) == 1
    snapshot = client.buffer[0]
    assert snapshot["open"] == 100
    assert snapshot["close"] == 109
    assert snapshot["high"] == 120
    assert snapshot["volume"] == 120
    assert snapshot["source_minute_count"] == 2
    assert snapshot["source_minute_mask"] == 0b11

    # The provider's latest-arrival recalculation can lower high/volume and can
    # change both the earliest open and latest close.
    client._aggregate_bar(
        _minute(13, 30, open_=101, high=108, low=100, close=104, volume=30)
    )
    client._aggregate_bar(
        _minute(13, 31, open_=104, high=107, low=103, close=106, volume=40)
    )
    client._flush_completed_bars()

    corrected = client.buffer[0]
    assert corrected["open"] == 101
    assert corrected["close"] == 106
    assert corrected["high"] == 108
    assert corrected["volume"] == 70
    assert corrected["source_minute_count"] == 2


def test_horizon_finalizes_window_and_rejects_later_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 28, 13, 55, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=5)
    original = _minute(
        13, 30, open_=100, high=102, low=99, close=101, volume=50
    )
    client._aggregate_bar(original)
    # 09:51 ET is strictly beyond window_end + the 15-minute correction horizon.
    client._aggregate_bar(
        _minute(13, 51, open_=102, high=103, low=101, close=102, volume=10)
    )
    client._flush_completed_bars()

    client._aggregate_bar(
        _minute(13, 30, open_=90, high=91, low=89, close=90, volume=1)
    )

    assert client.late_events_rejected == 1
    assert ("AAPL", datetime(2026, 8, 28, 13, 30, tzinfo=timezone.utc)) not in (
        client.bar_aggregator
    )
    assert client.buffer[0]["open"] == 100


def test_delayed_feed_wall_clock_does_not_finalize_on_first_arrival(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # At 10:50 ET a 10:30 ET window is already delayed by 15 minutes, but it
    # must remain revision-capable until event time advances through the horizon.
    _FrozenDateTime.current = datetime(2026, 8, 28, 14, 50, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=5)
    client._aggregate_bar(
        _minute(14, 30, open_=100, high=102, low=99, close=101, volume=50)
    )

    client._flush_completed_bars()

    assert client.buffer == []
    assert len(client.bar_aggregator) == 1


def test_sixty_minute_windows_anchor_to_open_and_ignore_after_hours(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 28, 20, 5, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=60)
    client._aggregate_bar(
        _minute(13, 30, open_=100, high=102, low=99, close=101, volume=50)
    )
    client._aggregate_bar(
        _minute(19, 59, open_=101, high=103, low=100, close=102, volume=40)
    )
    client._aggregate_bar(
        _minute(20, 0, open_=102, high=104, low=101, close=103, volume=30)
    )

    starts = {key[1] for key in client.bar_aggregator}
    assert datetime(2026, 8, 28, 13, 30, tzinfo=timezone.utc) in starts
    assert datetime(2026, 8, 28, 19, 30, tzinfo=timezone.utc) in starts
    assert datetime(2026, 8, 28, 19, 0, tzinfo=timezone.utc) not in starts
    assert client.out_of_session_events_ignored == 1


def test_new_session_prunes_finalized_cutoff_and_rejects_old_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 29, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=5)
    client.finalized_through[(
        "AAPL",
        datetime(2026, 8, 28, tzinfo=timezone.utc).date(),
    )] = datetime(2026, 8, 28, 13, 30, tzinfo=timezone.utc)
    client.latest_session_by_ticker["AAPL"] = datetime(
        2026, 8, 28, tzinfo=timezone.utc
    ).date()

    client._aggregate_bar(
        _minute(
            13,
            30,
            day=29,
            open_=100,
            high=102,
            low=99,
            close=101,
            volume=50,
        )
    )
    client._aggregate_bar(
        _minute(
            13,
            31,
            day=28,
            open_=100,
            high=102,
            low=99,
            close=101,
            volume=50,
        )
    )

    assert set(client.finalized_through) == set()
    assert client.late_events_rejected == 1


@pytest.mark.asyncio
async def test_unexpected_periodic_flush_failure_stops_and_closes_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    socket = _FakeWebSocket()
    client.websocket = cast(Any, socket)
    client.running = True

    async def no_delay(_delay: float) -> None:
        return None

    def fail_flush(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("periodic sentinel")

    monkeypatch.setattr(websocket_client.asyncio, "sleep", no_delay)
    monkeypatch.setattr(client, "_flush_completed_bars", fail_flush)

    with pytest.raises(RuntimeError, match="periodic sentinel"):
        await client._periodic_flush()

    assert client.running is False
    assert socket.closed is True
    assert client.websocket is None


@pytest.mark.asyncio
async def test_silent_socket_connect_has_finite_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(connect_timeout=0.01)

    async def never_connect(_uri: str) -> Any:
        await asyncio.Future()

    monkeypatch.setattr(websocket_client.websockets, "connect", never_connect)

    with pytest.raises(ConnectionError, match="connection timed out"):
        await client.connect()

    assert client.websocket is None


@pytest.mark.asyncio
async def test_silent_auth_and_subscription_receives_have_finite_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(handshake_timeout=0.01)
    auth_socket = _ScriptedWebSocket(
        [json.dumps([{"status": "connected"}]), None]
    )

    async def fake_connect(_uri: str) -> _ScriptedWebSocket:
        return auth_socket

    monkeypatch.setattr(websocket_client.websockets, "connect", fake_connect)

    with pytest.raises(ConnectionError, match="Authentication handshake timed out"):
        await client.connect()
    await client._close_current_websocket()

    subscription_socket = _ScriptedWebSocket([None])
    client.websocket = cast(Any, subscription_socket)
    with pytest.raises(ConnectionError, match="Subscription handshake timed out"):
        await client.subscribe()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_auth_response",
    [
        "{",
        json.dumps(42),
        json.dumps([{"status": "unexpected"}]),
    ],
    ids=["malformed-json", "non-object", "unexpected-status"],
)
async def test_invalid_auth_handshake_reconnects_instead_of_terminating(
    monkeypatch: pytest.MonkeyPatch,
    invalid_auth_response: str,
) -> None:
    client = _client()
    connected = json.dumps([{"status": "connected"}])
    sockets = [
        _ScriptedWebSocket([connected, invalid_auth_response]),
        _ScriptedWebSocket([connected, invalid_auth_response]),
    ]
    connection_attempts = 0

    async def fake_connect(_uri: str) -> _ScriptedWebSocket:
        nonlocal connection_attempts
        socket = sockets[connection_attempts]
        connection_attempts += 1
        if connection_attempts == 2:
            client.running = False
        return socket

    async def fake_periodic_flush() -> None:
        await asyncio.Future()

    async def no_delay(_delay: float) -> None:
        return None

    monkeypatch.setattr(websocket_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(client, "_periodic_flush", fake_periodic_flush)
    monkeypatch.setattr(websocket_client.asyncio, "sleep", no_delay)

    await client.run()

    assert connection_attempts == 2
    assert all(socket.closed for socket in sockets)
    assert client.websocket is None


@pytest.mark.asyncio
async def test_subscription_provider_error_raises() -> None:
    client = _client()
    socket = _FakeWebSocket([{"status": "error", "message": "not entitled"}])
    client.websocket = cast(Any, socket)

    with pytest.raises(ConnectionError, match="Subscription failed: not entitled"):
        await client.subscribe()


@pytest.mark.asyncio
async def test_subscription_without_success_confirmation_raises() -> None:
    client = _client()
    socket = _FakeWebSocket([{"status": "connected", "message": "waiting"}])
    client.websocket = cast(Any, socket)

    with pytest.raises(ConnectionError, match="not confirmed"):
        await client.subscribe()


@pytest.mark.asyncio
async def test_subscription_explicit_success_is_accepted() -> None:
    client = _client()
    socket = _FakeWebSocket([{"status": "success", "message": "subscribed"}])
    client.websocket = cast(Any, socket)

    await client.subscribe()

    assert socket.sent == [{"action": "subscribe", "params": "AM.AAPL"}]


@pytest.mark.asyncio
async def test_periodic_write_failure_retries_same_buffer_then_clears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(bar_size=15)
    bar = _buffered_bar()
    client.buffer = [bar]
    connection_attempts = 0
    persisted: list[dict[str, Any]] = []

    def flaky_connect(unused: str) -> _ConnectionContext:
        nonlocal connection_attempts
        connection_attempts += 1
        if connection_attempts == 1:
            raise psycopg.OperationalError("database down")
        return _ConnectionContext()

    def capture_bars(conn: object, bars: list[dict[str, Any]], logger: logging.Logger) -> int:
        persisted.extend(bars)
        return len(bars)

    monkeypatch.setattr(websocket_client.psycopg, "connect", flaky_connect)
    monkeypatch.setattr(websocket_client, "load_intraday_bars", capture_bars)

    assert await client._batch_write_to_db() is False
    assert client.buffer == [bar]
    assert isinstance(client._last_write_error, psycopg.OperationalError)

    assert await client._batch_write_to_db() is True
    assert connection_attempts == 2
    assert persisted == [bar]
    assert client.buffer == []
    assert client._last_write_error is None


@pytest.mark.asyncio
async def test_slow_database_write_does_not_block_message_consumption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(bar_size=5)
    client.buffer = [_buffered_bar()]
    write_started = threading.Event()
    release_write = threading.Event()

    def slow_write(_bars: list[dict[str, Any]]) -> int:
        write_started.set()
        if not release_write.wait(timeout=1.0):
            raise TimeoutError("test did not release database write")
        return 1

    monkeypatch.setattr(client, "_write_bars_to_db", slow_write)
    release_failsafe = threading.Timer(1.0, release_write.set)
    release_failsafe.start()
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    write_task = asyncio.create_task(client._batch_write_to_db())

    try:
        while not write_started.is_set():
            await asyncio.sleep(0)
        valid_message = json.dumps({"ev": "AM", **_bar()})
        await client._handle_message(valid_message)
        consumed_after = loop.time() - started_at
    finally:
        release_write.set()
        release_failsafe.cancel()

    assert consumed_after < 0.25
    assert len(client.bar_aggregator) == 1
    assert await write_task is True


@pytest.mark.asyncio
async def test_sustained_database_outage_caps_retry_buffer_and_reports_loss(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _client(max_buffered_bars=3)
    base_timestamp = datetime(2026, 1, 2, 14, 0, tzinfo=timezone.utc)

    def database_down(_bars: list[dict[str, Any]]) -> int:
        raise psycopg.OperationalError("database remains unavailable")

    monkeypatch.setattr(client, "_write_bars_to_db", database_down)

    with caplog.at_level(logging.ERROR):
        for offset in range(10):
            bar = _buffered_bar()
            bar["timestamp"] = base_timestamp + timedelta(minutes=offset)
            client._buffer_snapshot(bar)
            assert await client._batch_write_to_db() is False
            assert len(client.buffer) <= client.max_buffered_bars

    assert [bar["timestamp"] for bar in client.buffer] == [
        base_timestamp + timedelta(minutes=offset) for offset in range(7, 10)
    ]
    assert client.dropped_buffered_bars == 7
    assert "DATA LOSS" in caplog.text
    assert "total dropped=7" in caplog.text


def test_buffer_overflow_eviction_is_constant_work_per_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cap = 100
    overflow_events = 25
    client = _client(max_buffered_bars=cap)
    base_timestamp = datetime(2026, 1, 2, 14, 0, tzinfo=timezone.utc)
    identity_calls = 0
    original_identity = client._buffer_identity

    def counted_identity(bar: dict[str, Any]) -> tuple[Any, Any, Any]:
        nonlocal identity_calls
        identity_calls += 1
        return original_identity(bar)

    monkeypatch.setattr(client, "_buffer_identity", counted_identity)

    for offset in range(cap + overflow_events):
        bar = _buffered_bar()
        bar["timestamp"] = base_timestamp + timedelta(minutes=offset)
        client._buffer_snapshot(bar)

    assert len(client.buffer) == cap
    # One identity calculation per incoming snapshot. Reindexing the full cap
    # after every eviction would make this grow by cap * overflow_events.
    assert identity_calls == cap + overflow_events
    assert client.dropped_buffered_bars == overflow_events


def test_evicted_identity_can_return_as_a_fresh_corrected_snapshot() -> None:
    client = _client(max_buffered_bars=2)
    base_timestamp = datetime(2026, 1, 2, 14, 0, tzinfo=timezone.utc)

    bars: list[dict[str, Any]] = []
    for offset in range(3):
        bar = _buffered_bar()
        bar["timestamp"] = base_timestamp + timedelta(minutes=offset)
        bars.append(bar)
        client._buffer_snapshot(bar)

    corrected_evicted = dict(bars[0])
    corrected_evicted["close"] = 123.0
    client._buffer_snapshot(corrected_evicted)

    assert [bar["timestamp"] for bar in client.buffer] == [
        base_timestamp + timedelta(minutes=2),
        base_timestamp,
    ]
    assert client.buffer[-1]["close"] == 123.0
    assert client.dropped_buffered_bars == 2


@pytest.mark.asyncio
async def test_failed_detached_write_restores_same_identity_latest_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    original = _buffered_bar()
    original["source_minute_mask"] = 0b11
    client.buffer = [original]
    write_started = threading.Event()
    release_write = threading.Event()

    def delayed_failure(_bars: list[dict[str, Any]]) -> int:
        write_started.set()
        if not release_write.wait(timeout=1.0):
            raise TimeoutError("test did not release database write")
        raise psycopg.OperationalError("database down")

    monkeypatch.setattr(client, "_write_bars_to_db", delayed_failure)
    release_failsafe = threading.Timer(1.0, release_write.set)
    release_failsafe.start()
    write_task = asyncio.create_task(client._batch_write_to_db())

    try:
        while not write_started.is_set():
            await asyncio.sleep(0)
        correction = dict(original)
        correction["close"] = 123.0
        client._buffer_snapshot(correction)
    finally:
        release_write.set()
        release_failsafe.cancel()

    assert await write_task is False
    assert len(client.buffer) == 1
    assert client.buffer[0]["close"] == 123.0


@pytest.mark.asyncio
async def test_failed_write_then_correction_retries_authoritative_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 28, 13, 50, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=5)
    client._aggregate_bar(
        _minute(13, 30, open_=100, high=120, low=99, close=105, volume=50)
    )
    client._aggregate_bar(
        _minute(13, 31, open_=105, high=110, low=104, close=109, volume=70)
    )
    client._aggregate_bar(
        _minute(13, 35, open_=109, high=111, low=108, close=110, volume=20)
    )
    client._flush_completed_bars()

    connection_attempts = 0
    persisted: list[dict[str, Any]] = []

    def flaky_connect(_unused: str) -> _ConnectionContext:
        nonlocal connection_attempts
        connection_attempts += 1
        if connection_attempts == 1:
            raise psycopg.OperationalError("database down")
        return _ConnectionContext()

    def capture_bars(
        _conn: object, bars: list[dict[str, Any]], _logger: logging.Logger
    ) -> int:
        persisted.extend(dict(bar) for bar in bars)
        return len(bars)

    monkeypatch.setattr(websocket_client.psycopg, "connect", flaky_connect)
    monkeypatch.setattr(websocket_client, "load_intraday_bars", capture_bars)

    assert await client._batch_write_to_db() is False
    client._aggregate_bar(
        _minute(13, 30, open_=101, high=108, low=100, close=104, volume=30)
    )
    client._flush_completed_bars()
    assert await client._batch_write_to_db() is True

    assert connection_attempts == 2
    assert len(persisted) == 1
    assert persisted[0]["open"] == 101
    assert persisted[0]["high"] == 110
    assert persisted[0]["volume"] == 100
    assert client.buffer == []


@pytest.mark.asyncio
async def test_non_object_message_item_does_not_hide_valid_peer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 28, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=5)
    valid = {"ev": "AM", **_minute(
        13, 30, open_=100, high=102, low=99, close=101, volume=50
    )}

    await client._handle_message(json.dumps([42, valid]))

    assert len(client.bar_aggregator) == 1


@pytest.mark.asyncio
async def test_huge_integer_message_item_does_not_hide_valid_peer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 28, 13, 50, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=5)
    malformed = {"ev": "AM", **_minute(
        13, 30, open_=100, high=102, low=99, close=101, volume=50
    )}
    malformed["o"] = 10**1000
    valid = {"ev": "AM", **_minute(
        13, 31, open_=100, high=102, low=99, close=101, volume=50
    )}

    await client._handle_message(json.dumps([malformed, valid]))

    assert len(client.bar_aggregator) == 1
    aggregate = next(iter(client.bar_aggregator.values()))
    assert aggregate["source_minute_count"] == 1


@pytest.mark.asyncio
async def test_midstream_provider_error_is_counted_raised_and_redacted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _client()
    secret = "midstream-provider-secret"
    message = json.dumps(
        {
            "ev": "status",
            "status": "error",
            "message": f"access denied api_key={secret}",
        }
    )

    with caplog.at_level(logging.DEBUG), pytest.raises(ConnectionError) as exc_info:
        await client._handle_message(message)

    assert client.provider_status_errors == 1
    assert secret not in str(exc_info.value)
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_midstream_provider_error_closes_socket_and_reconnects(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _client()
    secret = "midstream-reconnect-secret"
    handshake = [
        json.dumps([{"status": "connected"}]),
        json.dumps([{"status": "auth_success"}]),
        json.dumps([{"status": "success", "message": "subscribed"}]),
    ]
    provider_error = json.dumps(
        {
            "ev": "status",
            "status": "error",
            "message": f"access denied apiKey: {secret}",
        }
    )
    sockets = [
        _ScriptedWebSocket(list(handshake), [provider_error]),
        _ScriptedWebSocket(list(handshake)),
    ]
    connection_attempts = 0

    async def fake_connect(_uri: str) -> _ScriptedWebSocket:
        nonlocal connection_attempts
        socket = sockets[connection_attempts]
        connection_attempts += 1
        if connection_attempts == 2:
            client.running = False
        return socket

    async def fake_periodic_flush() -> None:
        await asyncio.Future()

    async def no_delay(_delay: float) -> None:
        return None

    monkeypatch.setattr(websocket_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(client, "_periodic_flush", fake_periodic_flush)
    monkeypatch.setattr(websocket_client.asyncio, "sleep", no_delay)

    with caplog.at_level(logging.DEBUG):
        await client.run()

    assert connection_attempts == 2
    assert client.provider_status_errors == 1
    assert client.reconnectable_failures == 1
    assert client.stream_recovery_pending is True
    assert all(socket.closed for socket in sockets)
    assert client.websocket is None
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_accepted_bar_cannot_mask_unrecovered_reconnect_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    connected = json.dumps([{"status": "connected"}])
    auth_success = json.dumps([{"status": "auth_success"}])
    subscribed = json.dumps([{"status": "success", "message": "subscribed"}])
    valid_bar = json.dumps({"ev": "AM", **_bar()})
    sockets = [
        _ScriptedWebSocket([connected, auth_success, subscribed], [valid_bar]),
        _ScriptedWebSocket(
            [connected, json.dumps([{"status": "error", "message": "denied"}])]
        ),
    ]
    connection_attempts = 0

    async def fake_connect(_uri: str) -> _ScriptedWebSocket:
        nonlocal connection_attempts
        socket = sockets[connection_attempts]
        connection_attempts += 1
        if connection_attempts == 2:
            client.running = False
        return socket

    async def fake_periodic_flush() -> None:
        await asyncio.Future()

    async def no_delay(_delay: float) -> None:
        return None

    monkeypatch.setattr(websocket_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(client, "_periodic_flush", fake_periodic_flush)
    monkeypatch.setattr(client, "_write_bars_to_db", lambda bars: len(bars))
    monkeypatch.setattr(websocket_client.asyncio, "sleep", no_delay)

    await client.run()

    assert connection_attempts == 2
    assert client.minute_events_accepted == 1
    assert client.reconnectable_failures == 2
    assert client.stream_recovery_pending is True
    assert all(socket.closed for socket in sockets)


@pytest.mark.asyncio
async def test_valid_bar_after_reconnect_clears_pending_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    handshake = [
        json.dumps([{"status": "connected"}]),
        json.dumps([{"status": "auth_success"}]),
        json.dumps([{"status": "success", "message": "subscribed"}]),
    ]
    valid_bar = json.dumps({"ev": "AM", **_bar()})
    sockets = [
        _ScriptedWebSocket(list(handshake), [valid_bar]),
        _ScriptedWebSocket(list(handshake), [valid_bar]),
    ]
    connection_attempts = 0
    handled_messages = 0
    original_handle_message = client._handle_message

    async def fake_connect(_uri: str) -> _ScriptedWebSocket:
        nonlocal connection_attempts
        socket = sockets[connection_attempts]
        connection_attempts += 1
        return socket

    async def handle_then_stop(message: str) -> None:
        nonlocal handled_messages
        await original_handle_message(message)
        handled_messages += 1
        if handled_messages == 2:
            client.running = False

    async def fake_periodic_flush() -> None:
        await asyncio.Future()

    async def no_delay(_delay: float) -> None:
        return None

    monkeypatch.setattr(websocket_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(client, "_handle_message", handle_then_stop)
    monkeypatch.setattr(client, "_periodic_flush", fake_periodic_flush)
    monkeypatch.setattr(client, "_write_bars_to_db", lambda bars: len(bars))
    monkeypatch.setattr(websocket_client.asyncio, "sleep", no_delay)

    await client.run()

    assert connection_attempts == 2
    assert client.minute_events_accepted == 2
    assert client.reconnectable_failures == 1
    assert client.stream_recovery_pending is False
    assert all(socket.closed for socket in sockets)


@pytest.mark.asyncio
async def test_rejected_bar_does_not_clear_pending_stream_recovery() -> None:
    client = _client()
    client.reconnectable_failures = 1
    client.stream_recovery_pending = True
    after_hours = _bar()
    after_hours_timestamp = datetime(2026, 1, 2, 22, 0, tzinfo=timezone.utc)
    after_hours["s"] = int(after_hours_timestamp.timestamp() * 1000)

    await client._handle_message(json.dumps({"ev": "AM", **after_hours}))

    assert client.minute_events_accepted == 0
    assert client.out_of_session_events_ignored == 1
    assert client.stream_recovery_pending is True


@pytest.mark.asyncio
async def test_malformed_json_is_counted_without_logging_raw_secret(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _client()
    secret = "malformed-frame-secret"

    with caplog.at_level(logging.DEBUG):
        await client._handle_message(f'{{"api_key":"{secret}"')

    assert client.malformed_messages == 1
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_unexpected_aggregate_failure_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()

    def explode(_bar: dict[str, Any]) -> bool:
        raise RuntimeError("aggregate sentinel")

    monkeypatch.setattr(client, "_aggregate_bar", explode)

    with pytest.raises(RuntimeError, match="aggregate sentinel"):
        await client._handle_message(json.dumps({"ev": "AM", **_bar()}))


@pytest.mark.asyncio
async def test_all_malformed_minute_events_have_exact_telemetry() -> None:
    client = _client()
    missing_ohlcv = {"ev": "AM", "sym": "AAPL", "s": _bar()["s"]}
    invalid_ticker = {"ev": "AM", **_bar(), "sym": "NOT/SUBSCRIBED"}

    await client._handle_message(json.dumps([missing_ohlcv, invalid_ticker]))

    assert client.minute_events_received == 2
    assert client.minute_events_accepted == 0
    assert client.invalid_minute_events == 2


@pytest.mark.asyncio
async def test_mixed_valid_and_invalid_minute_events_have_exact_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 28, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client()
    valid = {"ev": "AM", **_minute(
        13, 30, open_=100, high=102, low=99, close=101, volume=50
    )}
    invalid_price = dict(valid)
    invalid_price["o"] = -1
    missing_volume = dict(valid)
    missing_volume.pop("v")

    await client._handle_message(json.dumps([invalid_price, valid, missing_volume]))

    assert client.minute_events_received == 3
    assert client.minute_events_accepted == 1
    assert client.invalid_minute_events == 2


def test_aggregate_volume_overflow_rejects_only_offending_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 28, 13, 50, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=5)
    client._aggregate_bar(
        _minute(
            13,
            30,
            open_=100,
            high=102,
            low=99,
            close=101,
            volume=websocket_client.MAX_VOLUME,
        )
    )
    client._aggregate_bar(
        _minute(13, 31, open_=101, high=103, low=100, close=102, volume=1)
    )

    aggregate = next(iter(client.bar_aggregator.values()))
    assert aggregate["volume"] == websocket_client.MAX_VOLUME
    assert aggregate["source_minute_count"] == 1
    assert aggregate["source_minute_mask"] == 0b1


def test_out_of_range_values_and_future_time_do_not_poison_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime.current = datetime(2026, 8, 28, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(websocket_client, "datetime", _FrozenDateTime)
    client = _client(bar_size=5)
    too_large = _minute(
        13,
        30,
        open_=1_000_000_000_000,
        high=1_000_000_000_001,
        low=999_999_999_999,
        close=1_000_000_000_000,
        volume=50,
    )
    fractional_volume = _minute(
        13, 31, open_=100, high=102, low=99, close=101, volume=50
    )
    fractional_volume["v"] = 1.5
    future = _minute(
        13,
        30,
        day=29,
        open_=100,
        high=102,
        low=99,
        close=101,
        volume=50,
    )
    below_scale = _minute(
        13,
        32,
        open_=0.000000001,
        high=0.000000002,
        low=0.000000001,
        close=0.000000001,
        volume=1,
    )

    client._aggregate_bar(too_large)
    client._aggregate_bar(fractional_volume)
    client._aggregate_bar(future)
    client._aggregate_bar(below_scale)

    assert client.bar_aggregator == {}
    assert client.max_event_time_by_ticker == {}
    assert client.latest_session_by_ticker == {}


@pytest.mark.asyncio
async def test_periodic_task_is_cancelled_and_awaited() -> None:
    client = _client()
    cleaned_up = asyncio.Event()

    async def periodic_worker() -> None:
        try:
            await asyncio.Future()
        finally:
            cleaned_up.set()

    task = asyncio.create_task(periodic_worker())
    await asyncio.sleep(0)

    await client._cancel_periodic_flush(task)

    assert task.cancelled()
    assert cleaned_up.is_set()


@pytest.mark.asyncio
async def test_failed_subscriptions_close_each_socket_before_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    sockets = [_FakeWebSocket(), _FakeWebSocket()]
    connection_attempts = 0
    subscription_attempts = 0

    async def fake_connect() -> None:
        nonlocal connection_attempts
        client.websocket = cast(Any, sockets[connection_attempts])
        connection_attempts += 1

    async def fake_subscribe() -> None:
        nonlocal subscription_attempts
        subscription_attempts += 1
        if subscription_attempts == 2:
            client.running = False
        raise ConnectionError("subscription rejected")

    async def fake_periodic_flush() -> None:
        await asyncio.Future()

    async def no_delay(delay: float) -> None:
        return None

    monkeypatch.setattr(client, "connect", fake_connect)
    monkeypatch.setattr(client, "subscribe", fake_subscribe)
    monkeypatch.setattr(client, "_periodic_flush", fake_periodic_flush)
    monkeypatch.setattr(websocket_client.asyncio, "sleep", no_delay)

    await client.run()

    assert connection_attempts == 2
    assert all(socket.closed for socket in sockets)
    assert client.websocket is None


@pytest.mark.asyncio
async def test_final_flush_failure_raises_after_socket_is_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(bar_size=15)
    bar = _buffered_bar()
    client.buffer = [bar]
    socket = _FakeWebSocket()
    client.websocket = cast(Any, socket)
    monkeypatch.setattr(
        websocket_client.psycopg,
        "connect",
        lambda unused: (_ for _ in ()).throw(psycopg.OperationalError("database down")),
    )

    with pytest.raises(RuntimeError, match="1 bars still buffered") as exc_info:
        await client.shutdown()

    assert isinstance(exc_info.value.__cause__, psycopg.OperationalError)
    assert client.buffer == [bar]
    assert socket.closed is True
    assert client.websocket is None


@pytest.mark.asyncio
async def test_successful_shutdown_flushes_buffer_and_closes_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(bar_size=15)
    client.buffer = [_buffered_bar()]
    socket = _FakeWebSocket()
    client.websocket = cast(Any, socket)
    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(websocket_client.psycopg, "connect", lambda unused: _ConnectionContext())

    def capture_bars(conn: object, bars: list[dict[str, Any]], logger: logging.Logger) -> int:
        captured.extend(bars)
        return len(bars)

    monkeypatch.setattr(
        websocket_client,
        "load_intraday_bars",
        capture_bars,
    )

    await client.shutdown()

    assert captured[0]["bar_size_minutes"] == 15
    assert client.buffer == []
    assert socket.closed is True
    assert client.websocket is None
