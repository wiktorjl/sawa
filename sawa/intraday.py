"""
Intraday price streaming via WebSocket.

Purpose: Stream real-time 5-minute bars during market hours.
Re-entrant: Safe to restart (upsert by ticker/timestamp).
Uses WebSocket for live data (15-min delayed).
"""

import asyncio
import logging
import os
import signal
from pathlib import Path
from typing import Any

import psycopg

from sawa.api.websocket_client import PolygonWebSocketClient
from sawa.database import get_symbols_from_db
from sawa.utils import setup_logging
from sawa.utils.security import open_private_text, redact_sensitive_text


async def _run_stream(client: PolygonWebSocketClient, logger: logging.Logger) -> None:
    """Install handlers even when a background shell inherited SIGINT ignored.

    Cancel the consumer exactly once; its finally block owns the final database
    flush. Repeated signals must not cancel that flush while it is in flight.
    """
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(client.run())
    requested = False
    previous: dict[signal.Signals, Any] = {}

    def stop(signum: signal.Signals) -> None:
        nonlocal requested
        if not requested:
            requested = True
            logger.info("Received %s; draining intraday writes", signum.name)
            task.cancel()

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.getsignal(signum)
            loop.add_signal_handler(signum, stop, signum)
        try:
            await task
        except asyncio.CancelledError:
            if not requested:
                raise
    finally:
        for signum, handler in previous.items():
            loop.remove_signal_handler(signum)
            signal.signal(signum, handler)


def run_intraday(
    api_key: str,
    database_url: str,
    bar_size: int = 5,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Stream intraday prices via WebSocket.

    Args:
        api_key: Polygon API key
        database_url: PostgreSQL connection URL
        bar_size: Bar interval in minutes (default: 5)
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}
    client: PolygonWebSocketClient | None = None

    def record_stream_outcome() -> None:
        def counter(name: str) -> int:
            value = getattr(client, name, 0) if client is not None else 0
            return value if isinstance(value, int) and not isinstance(value, bool) else 0

        telemetry_names = (
            "dropped_buffered_bars",
            "minute_events_received",
            "minute_events_accepted",
            "invalid_minute_events",
            "malformed_messages",
            "provider_status_errors",
            "reconnectable_failures",
            "late_events_rejected",
            "out_of_session_events_ignored",
            "history_minutes_recovered",
            "history_recovery_failures",
        )
        for name in telemetry_names:
            stats[name] = counter(name)
        stats["stream_recovery_pending"] = bool(
            getattr(client, "stream_recovery_pending", False) if client is not None else False
        )
        # bool() on a test/injected mock is not evidence of unfinished work.
        history_pending = getattr(client, "history_recovery_pending", False)
        stats["history_recovery_pending"] = history_pending is True

        dropped = stats["dropped_buffered_bars"]
        accepted = stats["minute_events_accepted"]
        invalid = stats["invalid_minute_events"]
        malformed = stats["malformed_messages"]
        provider_errors = stats["provider_status_errors"]
        reconnectable_failures = stats["reconnectable_failures"]
        recovery_pending = stats["stream_recovery_pending"]
        hard_failures: list[str] = []
        degraded_reasons: list[str] = []

        if dropped:
            hard_failures.append(
                f"data loss: {dropped} unpersisted intraday bar(s) were dropped; "
                "historical recovery is required"
            )
        if accepted == 0 and stats["history_minutes_recovered"] == 0:
            hard_failures.append(
                "stream stopped without accepting any valid regular-session minute bars"
            )
        if invalid:
            degraded_reasons.append(f"rejected {invalid} invalid minute event(s)")
        if malformed:
            degraded_reasons.append(f"rejected {malformed} malformed message(s)")
        if provider_errors:
            # Provider-declared status failures remain fatal even if transport
            # later resumes; a valid bar does not retract the provider error.
            hard_failures.append("provider reported a stream status error")
            degraded_reasons.append(f"provider reported {provider_errors} stream status error(s)")
        if reconnectable_failures:
            degraded_reasons.append(
                f"encountered {reconnectable_failures} reconnectable stream failure(s)"
            )
        if recovery_pending:
            hard_failures.append(
                "stream stopped before a reconnecting transport/startup failure "
                "was proven recovered by a valid minute bar"
            )
        if stats["history_recovery_pending"]:
            hard_failures.append("historical intraday reconciliation is incomplete")
        if stats["history_recovery_failures"]:
            degraded_reasons.append(
                f"encountered {stats['history_recovery_failures']} "
                "historical recovery request failure(s)"
            )
        late = stats["late_events_rejected"]
        if late:
            degraded_reasons.append(f"rejected {late} late minute event(s)")
        outside = stats["out_of_session_events_ignored"]
        if outside:
            degraded_reasons.append(f"ignored {outside} out-of-session minute event(s)")

        if hard_failures:
            stats["success"] = False
            stats["degraded"] = True
            stats["error"] = "; ".join(hard_failures)
            logger.error(stats["error"])
        else:
            stats["success"] = True
            if degraded_reasons:
                stats["degraded"] = True
        if degraded_reasons:
            stats["degraded_reasons"] = degraded_reasons

    logger.info("=" * 60)
    logger.info("INTRADAY STREAMING - WebSocket (15-min delayed)")
    logger.info("=" * 60)

    try:
        # Get symbols from database
        with psycopg.connect(database_url) as conn:
            symbols = get_symbols_from_db(conn)

        if not symbols:
            logger.error("No symbols in database. Run coldstart first.")
            return stats

        logger.info(f"Found {len(symbols)} symbols in database")
        stats["symbols"] = len(symbols)

        # Initialize WebSocket client
        client = PolygonWebSocketClient(
            api_key=api_key,
            database_url=database_url,
            tickers=symbols,
            bar_size=bar_size,
            logger=logger,
        )

        # Run WebSocket client (blocks until interrupted)
        logger.info("Starting WebSocket connection...")
        logger.info("Press Ctrl+C to stop")
        asyncio.run(_run_stream(client, logger))

        record_stream_outcome()
        logger.info("WebSocket streaming stopped")

    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        record_stream_outcome()
    except Exception as e:
        safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
        logger.error("Intraday streaming failed: %s", safe_error)
        stats["error"] = safe_error
        raise
    finally:
        # The scheduler survives across processes and cannot wait() on a child
        # launched by an earlier cron tick. Publish a completion result only
        # after final persistence, so process disappearance isn't called success.
        exit_file = os.environ.get("SAWA_INTRADAY_EXIT_FILE")
        if exit_file:
            with open_private_text(Path(exit_file), "w") as handle:
                handle.write("0\n" if stats.get("success") else "1\n")

    return stats
