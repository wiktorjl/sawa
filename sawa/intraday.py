"""
Intraday price streaming via WebSocket.

Purpose: Stream real-time 5-minute bars during market hours.
Re-entrant: Safe to restart (upsert by ticker/timestamp).
Uses WebSocket for live data (15-min delayed).
"""

import asyncio
import logging
from typing import Any

import psycopg

from sawa.api.websocket_client import PolygonWebSocketClient
from sawa.database import get_symbols_from_db
from sawa.utils import setup_logging
from sawa.utils.security import redact_sensitive_text


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
        )
        for name in telemetry_names:
            stats[name] = counter(name)
        stats["stream_recovery_pending"] = bool(
            getattr(client, "stream_recovery_pending", False)
            if client is not None
            else False
        )

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
        if accepted == 0:
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
            degraded_reasons.append(
                f"provider reported {provider_errors} stream status error(s)"
            )
        if reconnectable_failures:
            degraded_reasons.append(
                "encountered "
                f"{reconnectable_failures} reconnectable stream failure(s)"
            )
        if recovery_pending:
            hard_failures.append(
                "stream stopped before a reconnecting transport/startup failure "
                "was proven recovered by a valid minute bar"
            )
        late = stats["late_events_rejected"]
        if late:
            degraded_reasons.append(f"rejected {late} late minute event(s)")
        outside = stats["out_of_session_events_ignored"]
        if outside:
            degraded_reasons.append(
                f"ignored {outside} out-of-session minute event(s)"
            )

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
        asyncio.run(client.run())

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

    return stats
