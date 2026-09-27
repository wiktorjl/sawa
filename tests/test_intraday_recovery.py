"""Offline regressions for signal shutdown and delayed intraday reconciliation."""

import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import pytest

from sawa.api import websocket_client
from sawa.api.websocket_client import PolygonWebSocketClient
from sawa.utils.market_hours import regular_session_close


def client(**kwargs):
    return PolygonWebSocketClient("offline-key", "offline-db", ["AAPL"], **kwargs)


def minute(stamp, volume=100, close=10):
    return {"t": int(stamp.timestamp() * 1000), "o": 10, "h": 12, "l": 9, "c": close, "v": volume}


def test_scheduled_early_closes_and_full_holidays():
    assert regular_session_close(date(2026, 11, 27)).hour == 13
    assert regular_session_close(date(2026, 12, 24)).hour == 13
    assert regular_session_close(date(2025, 7, 3)).hour == 13
    assert regular_session_close(date(2026, 7, 3)) is None
    assert regular_session_close(date(2026, 9, 25)).hour == 16


def test_early_close_minutes_are_not_aggregated_as_regular_session():
    stream = client(recover_history=False)
    stamp = datetime(2025, 11, 28, 18, 0, tzinfo=timezone.utc)  # 13:00 ET
    row = minute(stamp)
    assert not stream._aggregate_bar({"sym": "AAPL", "s": row["t"], **row})
    assert not stream.bar_aggregator
    assert stream.out_of_session_events_ignored == 1


def test_rest_reconstruction_fills_lineage_and_preserves_live_correction():
    stream = client(recover_history=False)
    start = datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc)
    rows = [minute(start + timedelta(minutes=i)) for i in range(5)]
    correction = minute(start + timedelta(minutes=2), volume=50, close=11)
    assert stream._aggregate_bar({"sym": "AAPL", "s": correction["t"], **correction})
    recovered = stream._merge_history_minutes("AAPL", rows, start, start + timedelta(minutes=5))
    assert recovered == 5
    assert len(stream.buffer) == 1
    bar = stream.buffer[0]
    assert bar["source_minute_count"] == 5
    assert bar["source_minute_mask"] == 31
    assert bar["volume"] == 450
    assert (
        stream.bar_aggregator[("AAPL", start)]["_minutes"][start + timedelta(minutes=2)]["close"]
        == 11
    )


def test_malformed_recovery_does_not_publish_a_partial_ticker():
    stream = client(recover_history=False)
    start = datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc)
    rows = [minute(start), {**minute(start + timedelta(minutes=1)), "h": 1}]
    with pytest.raises(ValueError, match="invalid OHLCV"):
        stream._merge_history_minutes("AAPL", rows, start, start + timedelta(minutes=5))
    assert not stream.buffer
    assert not stream.bar_aggregator


def test_recent_recovered_lineage_accepts_later_single_minute_correction(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 25, 13, 51, tzinfo=timezone.utc)

    monkeypatch.setattr(websocket_client, "datetime", Clock)
    stream = client(recover_history=False)
    start = datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc)
    stream._merge_history_minutes(
        "AAPL", [minute(start + timedelta(minutes=i)) for i in range(5)],
        start, start + timedelta(minutes=5),
    )
    correction = minute(start + timedelta(minutes=2), volume=20)
    assert stream._aggregate_bar({"sym": "AAPL", "s": correction["t"], **correction})
    corrected = stream.bar_aggregator[("AAPL", start)]
    assert corrected["source_minute_mask"] == 31
    assert corrected["volume"] == 420


def test_reconnect_mid_bucket_combines_recovered_prefix_with_live_suffix(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 25, 14, 5, tzinfo=timezone.utc)

    monkeypatch.setattr(websocket_client, "datetime", Clock)
    stream = client(recover_history=False)
    start = datetime(2026, 9, 25, 13, 45, tzinfo=timezone.utc)
    stream._merge_history_minutes(
        "AAPL", [minute(start + timedelta(minutes=i)) for i in range(3)],
        start, start + timedelta(minutes=3),
    )
    for i in (3, 4):
        row = minute(start + timedelta(minutes=i))
        assert stream._aggregate_bar({"sym": "AAPL", "s": row["t"], **row})
    bar = stream.bar_aggregator[("AAPL", start)]
    assert bar["source_minute_mask"] == 31
    assert bar["source_minute_count"] == 5
    assert bar["volume"] == 500


@pytest.mark.asyncio
async def test_startup_and_reconnect_queue_current_session_without_blocking(monkeypatch):
    class Clock(datetime):
        current = datetime(2026, 9, 25, 14, 3, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz) if tz else cls.current.replace(tzinfo=None)

    monkeypatch.setattr(websocket_client, "datetime", Clock)
    stream = client()
    with mock.patch.object(stream, "_start_history_worker") as worker:
        stream._schedule_history_recovery()
        start, end = stream._history_windows["AAPL"]
        assert start == datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc)
        assert end == datetime(2026, 9, 25, 13, 48, tzinfo=timezone.utc)
        Clock.current += timedelta(minutes=10)
        stream._schedule_history_recovery()
        assert stream._history_windows["AAPL"][1] == end + timedelta(minutes=10)
        assert worker.call_count == 2


@pytest.mark.asyncio
async def test_recovery_uses_raw_minutes_and_keeps_failed_windows_retryable(monkeypatch):
    stream = client()
    start = datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc)
    stream._history_windows["AAPL"] = (start, start + timedelta(minutes=5))
    fetch = mock.AsyncMock(side_effect=[RuntimeError("offline failure"), [minute(start)]])
    monkeypatch.setattr(
        websocket_client,
        "AsyncPolygonClient",
        mock.Mock(return_value=mock.Mock(get_aggregates=fetch)),
    )
    await stream._recover_history()
    assert stream.history_recovery_pending
    assert stream.history_recovery_failures == 1
    await stream._recover_history()
    assert not stream.history_recovery_pending
    assert stream.history_minutes_recovered == 1
    assert fetch.call_args.kwargs["adjusted"] is False
    assert fetch.call_args.kwargs["timespan"] == "minute"
    assert fetch.call_args.kwargs["sort"] == "asc"


@pytest.mark.asyncio
async def test_slow_recovery_extends_old_partial_prefix_to_complete_lineage(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 25, 14, 40, tzinfo=timezone.utc)

    monkeypatch.setattr(websocket_client, "datetime", Clock)
    stream = client()
    start = datetime(2026, 9, 25, 13, 45, tzinfo=timezone.utc)
    # Reconnect at10:03 queued only source minutes through09:47. This ticker
    # reaches the worker later, after its live09:48/49 state has been finalized.
    stream._history_windows["AAPL"] = (start, start + timedelta(minutes=3))
    rows = [minute(start + timedelta(minutes=i)) for i in range(5)]
    monkeypatch.setattr(
        websocket_client, "AsyncPolygonClient",
        mock.Mock(return_value=mock.Mock(get_aggregates=mock.AsyncMock(return_value=rows))),
    )
    await stream._recover_history()
    assert stream.buffer[0]["source_minute_mask"] == 31
    assert stream.buffer[0]["volume"] == 500
    assert not stream.history_recovery_pending


@pytest.mark.parametrize("first_signal", ["SIGINT", "SIGTERM"])
def test_background_inherited_sigint_is_replaced_and_repeated_signals_drain(first_signal):
    program = """
import asyncio, logging, os, signal
from sawa.intraday import _run_stream
signal.signal(signal.SIGINT, signal.SIG_IGN)
class Stream:
    async def run(self):
        try:
            await asyncio.sleep(60)
        finally:
            await asyncio.sleep(0.03)
            print("final flush completed", flush=True)
async def main():
    loop = asyncio.get_running_loop()
    loop.call_later(0.01, os.kill, os.getpid(), getattr(signal, "FIRST"))
    loop.call_later(0.02, os.kill, os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(_run_stream(Stream(), logging.getLogger("offline")), 1)
asyncio.run(main())
""".replace("FIRST", first_signal)
    result = subprocess.run(
        [sys.executable, "-c", program], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "final flush completed"
