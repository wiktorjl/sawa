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

        stats["success"] = True
        logger.info("WebSocket streaming stopped")

    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        stats["success"] = True
    except Exception as e:
        logger.error(f"Intraday streaming failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
