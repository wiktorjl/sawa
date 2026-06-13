"""WebSocket client for streaming intraday bars from Polygon.io."""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
import websockets
from websockets.asyncio.client import ClientConnection

from sawa.database.intraday_load import load_intraday_bars

# Polygon WebSocket URLs
DELAYED_WEBSOCKET_URL = "wss://delayed.polygon.io/stocks"
REALTIME_WEBSOCKET_URL = "wss://socket.polygon.io/stocks"

# Reconnect backoff ceiling (seconds).
MAX_RECONNECT_BACKOFF = 60.0
# Wall-clock backstop: force-flush a window only if it is this many minutes
# older than its own end, so a fully stalled stream cannot grow memory without
# bound. It is deliberately generous (well beyond the 15-min feed delay) so it
# never pre-empts the event-time watermark during normal operation.
STREAM_STALL_MINUTES = 30


class PolygonWebSocketClient:
    """
    WebSocket client for streaming intraday bars from Polygon.io.

    Automatically handles:
    - Connection and authentication
    - Subscription to tickers
    - Aggregating 1-min bars into 5-min bars
    - Batched database writes
    - Reconnection with exponential backoff
    - Graceful shutdown
    """

    def __init__(
        self,
        api_key: str,
        database_url: str,
        tickers: list[str],
        bar_size: int = 5,
        batch_size: int = 100,
        batch_timeout: float = 30.0,
        logger: logging.Logger | None = None,
    ):
        """
        Args:
            api_key: Polygon API key
            database_url: PostgreSQL connection string
            tickers: List of ticker symbols to stream
            bar_size: Bar interval in minutes (default: 5)
            batch_size: Buffer size before database write
            batch_timeout: Max seconds to buffer before forced write
            logger: Logger instance
        """
        self.api_key = api_key
        self.database_url = database_url
        self.tickers = [t.upper() for t in tickers]
        self.bar_size = bar_size
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.logger = logger or logging.getLogger(__name__)

        # State
        self.websocket: ClientConnection | None = None
        self.running = False
        self.buffer: list[dict[str, Any]] = []
        self.last_flush = datetime.now(timezone.utc)

        # For aggregating 1-min bars into 5-min bars
        self.bar_aggregator: dict[tuple[str, datetime], dict[str, Any]] = {}

        # Event-time watermark: the latest window start observed across all
        # tickers. A window is complete once a bar from a strictly later window
        # has arrived. This replaces a wall-clock cutoff, which is wrong on the
        # delayed feed where bars arrive ~15 min after their event time.
        self.max_bar_start: datetime | None = None

        # Try delayed endpoint by default (fallback to real-time if access granted)
        self.uri = DELAYED_WEBSOCKET_URL

    async def connect(self) -> None:
        """Establish WebSocket connection and authenticate."""
        self.logger.info(f"Connecting to {self.uri}...")

        try:
            self.websocket = await websockets.connect(self.uri)
            self.logger.info("✓ Connected!")

            # Authenticate
            auth_msg = {"action": "auth", "params": self.api_key}
            await self.websocket.send(json.dumps(auth_msg))

            # Wait for connection confirmation
            response = await self.websocket.recv()
            data = json.loads(response)
            self.logger.debug(f"Auth response: {response!r}")

            # Wait for auth_success
            response = await self.websocket.recv()
            data = json.loads(response)
            self.logger.debug(f"Auth status: {response!r}")

            # Check for auth success
            if isinstance(data, list):
                for item in data:
                    if item.get("status") == "auth_success":
                        self.logger.info("✓ Authenticated!")
                        return

            raise ConnectionError("Authentication failed")

        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            raise

    async def subscribe(self) -> None:
        """Subscribe to aggregate minute bars for all tickers."""
        if self.websocket is None:
            raise RuntimeError("WebSocket not connected")

        # Polygon allows subscribing to multiple tickers at once
        # Format: "AM.AAPL,AM.MSFT,AM.GOOGL,..."
        params = ",".join([f"AM.{ticker}" for ticker in self.tickers])

        subscribe_msg = {"action": "subscribe", "params": params}
        await self.websocket.send(json.dumps(subscribe_msg))

        # Wait for subscription confirmation
        response = await self.websocket.recv()
        self.logger.debug(f"Subscription response: {response!r}")

        # Check for errors (e.g., real-time access denied)
        data = json.loads(response)
        if isinstance(data, list):
            for item in data:
                if item.get("status") == "error":
                    self.logger.warning(f"Subscription warning: {item.get('message')}")
                    # If real-time fails, we're already on delayed endpoint
                elif item.get("status") == "success":
                    self.logger.info(f"✓ Subscribed to {len(self.tickers)} tickers")
                    self.logger.info(f"📈 Monitoring: {self.bar_size}-minute bars (15-min delayed)")
                    self.logger.info(
                        f"💾 Auto-save: Every {self.batch_size} bars or {self.batch_timeout}s"
                    )
                    # Show sample of tickers
                    sample = ", ".join(self.tickers[:10])
                    if len(self.tickers) > 10:
                        sample += f", ... (+{len(self.tickers) - 10} more)"
                    self.logger.info(f"📊 Tickers: {sample}")

    def _aggregate_bar(self, bar_data: dict[str, Any]) -> None:
        """
        Aggregate 1-minute bars into N-minute bars.

        Args:
            bar_data: Raw 1-minute bar from Polygon
        """
        ticker = bar_data.get("sym")
        start_ms = bar_data.get("s")  # Start timestamp in milliseconds

        if not ticker or not start_ms:
            return

        # Convert to datetime and round down to bar_size boundary
        bar_time = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        rounded_minute = (bar_time.minute // self.bar_size) * self.bar_size
        bar_start = bar_time.replace(minute=rounded_minute, second=0, microsecond=0)

        # Advance the event-time watermark used by _flush_completed_bars.
        if self.max_bar_start is None or bar_start > self.max_bar_start:
            self.max_bar_start = bar_start

        key = (ticker, bar_start)

        if key not in self.bar_aggregator:
            # First bar in this window
            self.bar_aggregator[key] = {
                "ticker": ticker,
                "timestamp": bar_start,
                "open": bar_data.get("o"),
                "high": bar_data.get("h"),
                "low": bar_data.get("l"),
                "close": bar_data.get("c"),
                "volume": bar_data.get("v", 0),
                "bar_count": 1,
            }
        else:
            # Update existing bar
            agg = self.bar_aggregator[key]
            agg["high"] = max(agg["high"], bar_data.get("h", agg["high"]))
            agg["low"] = min(agg["low"], bar_data.get("l", agg["low"]))
            agg["close"] = bar_data.get("c")  # Last close
            agg["volume"] += bar_data.get("v", 0)
            agg["bar_count"] += 1

    def _flush_completed_bars(self, flush_all: bool = False) -> None:
        """Move completed bars from aggregator to buffer.

        A window ``[W, W+bar_size)`` is complete once we have observed a 1-min
        bar belonging to a later window (the event-time watermark). We cannot
        use wall-clock time: on the 15-min delayed feed every bar arrives long
        after its event time, so a wall-clock cutoff would flush each window
        after its first 1-min bar and the later bars would re-create — then
        overwrite — the same window, leaving only the last 1-min bar's data.

        Args:
            flush_all: Drain every window regardless of completeness (used at
                shutdown, when no later bars will ever arrive).
        """
        if flush_all:
            completed_keys = list(self.bar_aggregator.keys())
        elif self.max_bar_start is None:
            return
        else:
            bar_delta = timedelta(minutes=self.bar_size)
            watermark = self.max_bar_start
            # Backstop so a fully stalled stream cannot grow memory unbounded.
            stale_cutoff = datetime.now(timezone.utc) - timedelta(
                minutes=self.bar_size + STREAM_STALL_MINUTES
            )
            completed_keys = [
                key
                for key, bar in self.bar_aggregator.items()
                if bar["timestamp"] + bar_delta <= watermark
                or bar["timestamp"] < stale_cutoff
            ]

        for key in completed_keys:
            bar = self.bar_aggregator.pop(key)
            bar_count = bar.pop("bar_count", None)  # Remove internal tracking field
            self.buffer.append(bar)

            # Log completed bar with price info
            timestamp_str = bar["timestamp"].strftime("%Y-%m-%d %H:%M")
            self.logger.info(
                f"📊 {bar['ticker']:5s} | {timestamp_str} | "
                f"O:{bar['open']:7.2f} H:{bar['high']:7.2f} "
                f"L:{bar['low']:7.2f} C:{bar['close']:7.2f} | "
                f"Vol:{bar['volume']:,} | ({bar_count} 1-min bars)"
            )

    async def _handle_message(self, message: str) -> None:
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)

            if not isinstance(data, list):
                data = [data]

            bars_received = 0
            for item in data:
                event_type = item.get("ev")

                if event_type == "AM":  # Aggregate Minute
                    self._aggregate_bar(item)
                    bars_received += 1
                elif event_type == "status":
                    self.logger.debug(f"Status: {item.get('message')}")

            # Log when we receive bars (but not too verbose)
            if bars_received > 0:
                self.logger.debug(f"📥 Received {bars_received} 1-min bars (aggregating...)")

        except json.JSONDecodeError:
            self.logger.warning(f"Failed to parse message: {message[:100]}")
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")

    async def _batch_write_to_db(self) -> None:
        """Write buffered bars to database."""
        if not self.buffer:
            return

        try:
            # Get summary before clearing buffer
            tickers = set(bar["ticker"] for bar in self.buffer)
            time_range = (
                min(bar["timestamp"] for bar in self.buffer),
                max(bar["timestamp"] for bar in self.buffer),
            )

            with psycopg.connect(self.database_url) as conn:
                inserted = load_intraday_bars(conn, self.buffer, self.logger)

            self.logger.info(
                f"💾 Saved {inserted} bars to database | "
                f"{len(tickers)} tickers | "
                f"{time_range[0].strftime('%H:%M')} - {time_range[1].strftime('%H:%M')}"
            )
            self.buffer.clear()
            self.last_flush = datetime.now(timezone.utc)

        except Exception as e:
            self.logger.error(f"Database write failed: {e}")
            # Keep buffer for retry

    async def _periodic_flush(self) -> None:
        """Periodically flush buffer and completed bars."""
        while self.running:
            await asyncio.sleep(10)  # Check every 10 seconds

            # Move completed bars to buffer
            self._flush_completed_bars()

            # Flush buffer if needed
            should_flush = (
                len(self.buffer) >= self.batch_size
                or (
                    datetime.now(timezone.utc) - self.last_flush
                ).total_seconds() >= self.batch_timeout
            )

            if should_flush:
                await self._batch_write_to_db()

    async def run(self) -> None:
        """Main event loop - run until interrupted.

        Reconnects with exponential backoff on connection loss so a mid-session
        disconnect does not silently end the stream (and drop bars) until the
        next scheduler tick. The in-memory aggregator and buffer persist across
        reconnects, so a transient blip loses no already-received data.
        """
        self.running = True
        # The flush task outlives individual connections.
        flush_task = asyncio.create_task(self._periodic_flush())
        backoff = 1.0

        try:
            while self.running:
                try:
                    await self.connect()
                    await self.subscribe()

                    if self.websocket is None:
                        raise RuntimeError("WebSocket not connected")

                    backoff = 1.0  # reset after a successful connect+subscribe
                    self.logger.info("Streaming started. Press Ctrl+C to stop.")

                    async for message in self.websocket:
                        if not self.running:
                            break
                        if isinstance(message, str):
                            await self._handle_message(message)

                    if not self.running:
                        break
                    self.logger.warning("Stream ended by server; will reconnect")
                except KeyboardInterrupt:
                    self.logger.info("Interrupted by user")
                    break
                except websockets.exceptions.ConnectionClosed:
                    self.logger.warning("Connection closed")
                except (OSError, websockets.exceptions.WebSocketException) as e:
                    self.logger.warning(f"WebSocket error: {e}")

                if not self.running:
                    break
                self.logger.info(f"Reconnecting in {backoff:.0f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_RECONNECT_BACKOFF)
        finally:
            self.running = False
            flush_task.cancel()
            await self.shutdown()

    async def shutdown(self) -> None:
        """Graceful shutdown - flush buffer and close connection."""
        self.logger.info("Shutting down...")
        self.running = False

        # Flush any remaining bars — no later bars will arrive, so drain all.
        self._flush_completed_bars(flush_all=True)
        if self.buffer:
            await self._batch_write_to_db()

        # Close WebSocket
        if self.websocket:
            await self.websocket.close()

        self.logger.info("✓ Shutdown complete")
