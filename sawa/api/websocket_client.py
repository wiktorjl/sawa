"""WebSocket client for streaming intraday bars from Polygon.io."""

import asyncio
import json
import logging
import math
import re
from collections import OrderedDict
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import websockets
from websockets.asyncio.client import ClientConnection

from sawa.database.intraday_load import load_intraday_bars
from sawa.utils.security import redact_sensitive_text

# Polygon WebSocket URLs
DELAYED_WEBSOCKET_URL = "wss://delayed.polygon.io/stocks"
REALTIME_WEBSOCKET_URL = "wss://socket.polygon.io/stocks"

# Reconnect backoff ceiling (seconds).
MAX_RECONNECT_BACKOFF = 60.0
# Every stage that can otherwise leave startup waiting forever has an explicit
# deadline.  These are constructor-configurable for deployments and tests.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 10.0
# A complete regular session at the default five-minute interval for roughly
# 500 symbols is about 39,000 bars.  Retain a little more than that, but choose
# a finite ceiling so an extended database outage cannot exhaust process memory.
DEFAULT_MAX_BUFFERED_BARS = 50_000
# Wall-clock backstop: force-flush a window only if it is this many minutes
# older than its own end, so a fully stalled stream cannot grow memory without
# bound. It is deliberately generous (well beyond the 15-min feed delay) so it
# never pre-empts the event-time watermark during normal operation.
STREAM_STALL_MINUTES = 30
# Massive/Polygon can rebroadcast a recalculated minute for 15 minutes after
# late trades arrive. Keep minute-level state for that full correction horizon.
CORRECTION_HORIZON_MINUTES = 15
MARKET_TIMEZONE = ZoneInfo("America/New_York")
REGULAR_SESSION_OPEN = time(9, 30)
REGULAR_SESSION_CLOSE = time(16, 0)
ALLOWED_BAR_SIZES = {1, 5, 15, 30, 60}
TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,9}$")
MAX_PRICE_EXCLUSIVE = 1_000_000_000_000
MAX_PRICE_DECIMAL = Decimal("1000000000000")
PRICE_SCALE = Decimal("0.00000001")
MAX_VOLUME = 9_223_372_036_854_775_807
MAX_EVENT_CLOCK_SKEW_MINUTES = 5
MAX_SAFE_ERROR_CHARS = 500


def _safe_error(value: object) -> str:
    """Return a bounded credential-safe diagnostic for stream boundaries."""
    redacted = redact_sensitive_text(value).replace("\r", " ").replace("\n", " ")
    return redacted[:MAX_SAFE_ERROR_CHARS]


def _safe_exception(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {_safe_error(exc)}"


def _price_rounds_to_numeric_20_8(value: float | int) -> bool:
    """Match PostgreSQL positive NUMERIC(20,8) representability."""
    try:
        decimal_value = Decimal(str(value))
        if not 0 < decimal_value < MAX_PRICE_DECIMAL:
            return False
        rounded = decimal_value.quantize(PRICE_SCALE, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return False
    return 0 < rounded < MAX_PRICE_DECIMAL


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
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        handshake_timeout: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
        max_buffered_bars: int = DEFAULT_MAX_BUFFERED_BARS,
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
            connect_timeout: Maximum seconds allowed to establish a socket
            handshake_timeout: Maximum seconds allowed for each auth/subscribe
                send or receive operation
            max_buffered_bars: Maximum completed bars retained while database
                persistence is unavailable. Once full, the oldest bar is
                dropped and the loss is counted and logged.
        """
        self.api_key = api_key
        self.database_url = database_url
        normalized_tickers: list[str] = []
        for ticker in tickers:
            if not isinstance(ticker, str):
                raise ValueError(f"invalid ticker symbol: {ticker!r}")
            normalized = ticker.strip().upper()
            if not TICKER_PATTERN.fullmatch(normalized):
                raise ValueError(f"invalid ticker symbol: {ticker!r}")
            if normalized not in normalized_tickers:
                normalized_tickers.append(normalized)
        if not normalized_tickers:
            raise ValueError("tickers cannot be empty")
        self.tickers = normalized_tickers
        self._ticker_set = set(normalized_tickers)
        self.bar_size = bar_size
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.logger = logger or logging.getLogger(__name__)

        if isinstance(bar_size, bool) or bar_size not in ALLOWED_BAR_SIZES:
            raise ValueError(f"bar_size must be one of {sorted(ALLOWED_BAR_SIZES)}")
        if (
            isinstance(connect_timeout, bool)
            or not isinstance(connect_timeout, (int, float))
            or not math.isfinite(connect_timeout)
            or connect_timeout <= 0
        ):
            raise ValueError("connect_timeout must be a finite positive number")
        if (
            isinstance(handshake_timeout, bool)
            or not isinstance(handshake_timeout, (int, float))
            or not math.isfinite(handshake_timeout)
            or handshake_timeout <= 0
        ):
            raise ValueError("handshake_timeout must be a finite positive number")
        if (
            isinstance(max_buffered_bars, bool)
            or not isinstance(max_buffered_bars, int)
            or max_buffered_bars <= 0
        ):
            raise ValueError("max_buffered_bars must be a positive integer")
        self.connect_timeout = float(connect_timeout)
        self.handshake_timeout = float(handshake_timeout)
        self.max_buffered_bars = max_buffered_bars

        # State
        self.websocket: ClientConnection | None = None
        self.running = False
        # Preserve retry order while making identity replacement and oldest-bar
        # eviction O(1).  A list plus identity-to-position map still becomes
        # quadratic once the cap is full because removing index zero shifts and
        # reindexes every remaining entry for each new bar.
        self._buffered_snapshots: OrderedDict[
            tuple[Any, Any, Any], dict[str, Any]
        ] = OrderedDict()
        # Exact, process-lifetime loss telemetry for the bounded persistence
        # retry buffer. A non-zero value means bars were not persisted by this
        # process and should be recovered from the upstream historical API.
        self.dropped_buffered_bars = 0
        self.minute_events_received = 0
        self.minute_events_accepted = 0
        self.invalid_minute_events = 0
        self.malformed_messages = 0
        self.provider_status_errors = 0
        # A reconnect handshake proves only that the control plane is back.
        # Recovery is established only by a subsequent valid minute event.
        self.reconnectable_failures = 0
        self.stream_recovery_pending = False
        self.last_flush = datetime.now(timezone.utc)
        self._last_write_error: Exception | None = None

        # Active windows retain their source minutes through the provider's
        # correction horizon. A rebroadcast for the same minute replaces that
        # minute authoritatively and the N-minute bar is recomputed.
        self.bar_aggregator: dict[tuple[str, datetime], dict[str, Any]] = {}

        # Per-ticker event-time watermark: the latest window start observed for
        # each ticker. A ticker's window is complete once a bar from a strictly
        # later window of THAT SAME ticker has arrived. A single global
        # watermark is wrong here: a liquid ticker advancing it into the next
        # window would prematurely flush a thin ticker's still-partial window
        # before all of its in-window minutes arrive, and the thin ticker's
        # later minutes would then re-create — and corrupt — the same window.
        # We still use a wall-clock backstop so a stalled stream cannot grow
        # memory without bound; the per-ticker watermark is the normal path
        # because on the 15-min delayed feed every bar arrives long after its
        # event time, so a wall-clock cutoff alone would flush each window
        # after its first 1-min bar.
        self.max_event_time_by_ticker: dict[str, datetime] = {}
        self.last_received_wallclock_by_ticker: dict[str, datetime] = {}
        # O(tickers) finalized watermarks reject replay/late events only after
        # their correction horizon. One current market-date entry is retained
        # per ticker instead of millions of exact-window tombstones.
        self.finalized_through: dict[tuple[str, date], datetime] = {}
        self.latest_session_by_ticker: dict[str, date] = {}
        self.late_events_rejected = 0
        self.out_of_session_events_ignored = 0

        # Try delayed endpoint by default (fallback to real-time if access granted)
        self.uri = DELAYED_WEBSOCKET_URL

    @property
    def buffer(self) -> list[dict[str, Any]]:
        """Return buffered snapshots in persistence order.

        The list-shaped compatibility view keeps existing callers and
        diagnostics simple while the authoritative store remains an ordered
        identity map with constant-time replacement and eviction.
        """
        return list(self._buffered_snapshots.values())

    @buffer.setter
    def buffer(self, bars: list[dict[str, Any]]) -> None:
        """Replace the retry buffer from a list-shaped compatibility value."""
        rebuilt: OrderedDict[tuple[Any, Any, Any], dict[str, Any]] = OrderedDict()
        for bar in bars:
            snapshot = {
                key: value for key, value in bar.items() if not key.startswith("_")
            }
            identity = self._buffer_identity(snapshot)
            buffered = rebuilt.get(identity)
            if buffered is None or self._snapshot_supersedes(snapshot, buffered):
                rebuilt[identity] = snapshot
        self._buffered_snapshots = rebuilt
        self._enforce_buffer_limit()

    async def _send_handshake_message(
        self, message: dict[str, str], phase: str
    ) -> None:
        """Send one startup message without allowing a silent socket to hang."""
        if self.websocket is None:
            raise ConnectionError(f"{phase} failed: WebSocket not connected")
        try:
            await asyncio.wait_for(
                self.websocket.send(json.dumps(message)),
                timeout=self.handshake_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise ConnectionError(
                f"{phase} send timed out after {self.handshake_timeout:g}s"
            ) from exc

    async def _recv_handshake_statuses(self, phase: str) -> list[dict[str, Any]]:
        """Receive and validate a provider handshake status envelope."""
        if self.websocket is None:
            raise ConnectionError(f"{phase} failed: WebSocket not connected")
        try:
            response = await asyncio.wait_for(
                self.websocket.recv(), timeout=self.handshake_timeout
            )
        except asyncio.TimeoutError as exc:
            raise ConnectionError(
                f"{phase} timed out after {self.handshake_timeout:g}s"
            ) from exc

        self.logger.debug("%s response received", phase)
        try:
            data = json.loads(response)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
            raise ConnectionError(f"{phase} returned malformed JSON") from exc

        if isinstance(data, dict):
            return [data]
        if isinstance(data, list) and data and all(
            isinstance(item, dict) for item in data
        ):
            return data
        raise ConnectionError(f"{phase} returned a non-object status response")

    @staticmethod
    def _provider_errors(statuses: list[dict[str, Any]]) -> list[str]:
        """Extract provider error text from a validated status envelope."""
        return [
            _safe_error(item.get("message") or "unknown provider error")
            for item in statuses
            if item.get("status") == "error"
        ]

    async def connect(self) -> None:
        """Establish WebSocket connection and authenticate."""
        self.logger.info(f"Connecting to {self.uri}...")

        try:
            try:
                self.websocket = await asyncio.wait_for(
                    websockets.connect(self.uri), timeout=self.connect_timeout
                )
            except asyncio.TimeoutError as exc:
                raise ConnectionError(
                    f"WebSocket connection timed out after {self.connect_timeout:g}s"
                ) from exc
            self.logger.info("✓ Connected!")

            # Authenticate only after the socket exists. Every send/receive in
            # the startup protocol is bounded and every invalid response is a
            # ConnectionError, which the run loop treats as reconnectable.
            auth_msg = {"action": "auth", "params": self.api_key}
            await self._send_handshake_message(auth_msg, "Authentication")

            connected_statuses = await self._recv_handshake_statuses(
                "Connection handshake"
            )
            errors = self._provider_errors(connected_statuses)
            if errors:
                raise ConnectionError(
                    f"Connection handshake failed: {'; '.join(errors)}"
                )
            if not any(
                item.get("status") == "connected" for item in connected_statuses
            ):
                raise ConnectionError("Connection was not confirmed by provider")

            auth_statuses = await self._recv_handshake_statuses(
                "Authentication handshake"
            )
            errors = self._provider_errors(auth_statuses)
            if errors:
                raise ConnectionError(f"Authentication failed: {'; '.join(errors)}")
            if not any(item.get("status") == "auth_success" for item in auth_statuses):
                raise ConnectionError("Authentication was not confirmed by provider")

            self.logger.info("✓ Authenticated!")
        except asyncio.CancelledError:
            raise
        except ConnectionError as exc:
            self.logger.error("Connection failed: %s", _safe_exception(exc))
            raise
        except Exception as exc:
            # Startup implementation/provider surprises are connection-local;
            # normalize them so run() closes this socket and reconnects.
            error = ConnectionError(
                f"Unexpected connection handshake failure: {_safe_exception(exc)}"
            )
            self.logger.error("Connection failed: %s", _safe_exception(error))
            raise error from exc

    async def subscribe(self) -> None:
        """Subscribe to aggregate minute bars for all tickers."""
        if self.websocket is None:
            raise RuntimeError("WebSocket not connected")

        # Polygon allows subscribing to multiple tickers at once
        # Format: "AM.AAPL,AM.MSFT,AM.GOOGL,..."
        params = ",".join([f"AM.{ticker}" for ticker in self.tickers])

        try:
            subscribe_msg = {"action": "subscribe", "params": params}
            await self._send_handshake_message(subscribe_msg, "Subscription")

            # A connection is not usable until Polygon explicitly confirms the
            # subscription. Errors and ambiguous status responses must not fall
            # through into a healthy-looking stream.
            statuses = await self._recv_handshake_statuses(
                "Subscription handshake"
            )
            errors = self._provider_errors(statuses)
            if errors:
                raise ConnectionError(f"Subscription failed: {'; '.join(errors)}")

            confirmed = any(item.get("status") == "success" for item in statuses)
            if not confirmed:
                raise ConnectionError("Subscription was not confirmed by provider")
        except asyncio.CancelledError:
            raise
        except ConnectionError:
            raise
        except Exception as exc:
            raise ConnectionError(
                "Unexpected subscription handshake failure: "
                f"{_safe_exception(exc)}"
            ) from exc

        self.logger.info(f"✓ Subscribed to {len(self.tickers)} tickers")
        self.logger.info(f"📈 Monitoring: {self.bar_size}-minute bars (15-min delayed)")
        self.logger.info(f"💾 Auto-save: Every {self.batch_size} bars or {self.batch_timeout}s")
        sample = ", ".join(self.tickers[:10])
        if len(self.tickers) > 10:
            sample += f", ... (+{len(self.tickers) - 10} more)"
        self.logger.info(f"📊 Tickers: {sample}")

    def _aggregate_bar(self, bar_data: dict[str, Any]) -> bool:
        """
        Aggregate 1-minute bars into N-minute bars.

        Args:
            bar_data: Raw 1-minute bar from Polygon
        """
        ticker = bar_data.get("sym")
        start_ms = bar_data.get("s")  # Start timestamp in milliseconds

        if not isinstance(ticker, str):
            self.invalid_minute_events += 1
            self.logger.warning("Ignoring minute bar without a valid ticker")
            return False
        ticker = ticker.strip().upper()
        if ticker not in self._ticker_set or not TICKER_PATTERN.fullmatch(ticker):
            self.logger.warning("Ignoring minute bar for an unsubscribed/invalid ticker")
            self.invalid_minute_events += 1
            return False
        if (
            isinstance(start_ms, bool)
            or not isinstance(start_ms, int)
            or start_ms < 0
            or start_ms > 10_000_000_000_000
            or start_ms % 60_000 != 0
        ):
            self.logger.warning(f"Ignoring malformed minute timestamp for {ticker}")
            self.invalid_minute_events += 1
            return False

        numeric_fields: dict[str, float | int] = {}
        for field in ("o", "h", "l", "c", "v"):
            value = bar_data.get(field)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or (isinstance(value, float) and not math.isfinite(value))
            ):
                self.logger.warning(
                    f"Ignoring malformed minute bar for {ticker}: invalid {field}"
                )
                self.invalid_minute_events += 1
                return False
            numeric_fields[field] = value

        if (
            any(
                not _price_rounds_to_numeric_20_8(numeric_fields[field])
                for field in ("o", "h", "l", "c")
            )
            or any(
                numeric_fields[field] >= MAX_PRICE_EXCLUSIVE
                for field in ("o", "h", "l", "c")
            )
            or numeric_fields["v"] < 0
            or numeric_fields["v"] > MAX_VOLUME
            or not float(numeric_fields["v"]).is_integer()
            or numeric_fields["h"]
            < max(numeric_fields["o"], numeric_fields["l"], numeric_fields["c"])
            or numeric_fields["l"]
            > min(numeric_fields["o"], numeric_fields["h"], numeric_fields["c"])
        ):
            self.logger.warning(f"Ignoring malformed OHLCV minute bar for {ticker}")
            self.invalid_minute_events += 1
            return False

        # Anchor every interval to the 09:30 New York session open. UTC/hour
        # flooring misaligns 60-minute windows (09:30 would become 09:00).
        try:
            event_time = datetime.fromtimestamp(
                start_ms / 1000, tz=timezone.utc
            ).replace(second=0, microsecond=0)
        except (OverflowError, OSError, ValueError):
            self.logger.warning(f"Ignoring out-of-range minute timestamp for {ticker}")
            self.invalid_minute_events += 1
            return False
        if event_time > datetime.now(timezone.utc) + timedelta(
            minutes=MAX_EVENT_CLOCK_SKEW_MINUTES
        ):
            self.logger.warning(f"Ignoring future minute timestamp for {ticker}")
            self.invalid_minute_events += 1
            return False
        numeric_fields["v"] = int(numeric_fields["v"])
        local_event_time = event_time.astimezone(MARKET_TIMEZONE)
        local_market_time = local_event_time.time().replace(tzinfo=None)
        if not REGULAR_SESSION_OPEN <= local_market_time < REGULAR_SESSION_CLOSE:
            self.out_of_session_events_ignored += 1
            self.logger.debug(f"Ignoring out-of-session minute bar for {ticker}")
            return False

        session_anchor = local_event_time.replace(
            hour=REGULAR_SESSION_OPEN.hour,
            minute=REGULAR_SESSION_OPEN.minute,
            second=0,
            microsecond=0,
        )
        elapsed_minutes = int((local_event_time - session_anchor).total_seconds() // 60)
        window_offset = (elapsed_minutes // self.bar_size) * self.bar_size
        bar_start = (session_anchor + timedelta(minutes=window_offset)).astimezone(
            timezone.utc
        )
        market_date = local_event_time.date()

        latest_session = self.latest_session_by_ticker.get(ticker)
        if latest_session is not None and market_date < latest_session:
            self.late_events_rejected += 1
            self.logger.warning(
                f"Rejecting prior-session minute for {ticker} after newer session began"
            )
            return False
        if latest_session is None or market_date > latest_session:
            self.latest_session_by_ticker[ticker] = market_date
            for cutoff_key in list(self.finalized_through):
                if cutoff_key[0] == ticker and cutoff_key[1] != market_date:
                    del self.finalized_through[cutoff_key]

        finalized_cutoff = self.finalized_through.get((ticker, market_date))
        if finalized_cutoff is not None and bar_start <= finalized_cutoff:
            self.late_events_rejected += 1
            self.logger.warning(
                f"Rejecting minute for finalized {self.bar_size}-minute window: "
                f"{ticker} {bar_start.isoformat()}"
            )
            return False

        prev_watermark = self.max_event_time_by_ticker.get(ticker)
        if prev_watermark is None or event_time > prev_watermark:
            self.max_event_time_by_ticker[ticker] = event_time
        self.last_received_wallclock_by_ticker[ticker] = datetime.now(timezone.utc)

        key = (ticker, bar_start)

        if key not in self.bar_aggregator:
            self.bar_aggregator[key] = {
                "ticker": ticker,
                "timestamp": bar_start,
                "bar_size_minutes": self.bar_size,
                "_minutes": {},
                "_dirty": False,
            }

        agg = self.bar_aggregator[key]
        minutes: dict[datetime, dict[str, float | int]] = agg["_minutes"]
        minute = {
            "open": numeric_fields["o"],
            "high": numeric_fields["h"],
            "low": numeric_fields["l"],
            "close": numeric_fields["c"],
            "volume": numeric_fields["v"],
        }
        if minutes.get(event_time) == minute:
            return True

        # Latest arrival wins for a repeated minute: provider rebroadcasts are
        # full recalculations that may legitimately lower high or volume.
        previous_minute = minutes.get(event_time)
        minutes[event_time] = minute
        ordered_minutes = [minutes[timestamp] for timestamp in sorted(minutes)]
        total_volume = sum(item["volume"] for item in ordered_minutes)
        if total_volume > MAX_VOLUME:
            if previous_minute is None:
                del minutes[event_time]
            else:
                minutes[event_time] = previous_minute
            self.logger.warning(
                f"Ignoring minute that would overflow aggregate volume for {ticker}"
            )
            self.invalid_minute_events += 1
            return False
        agg.update(
            {
                "open": ordered_minutes[0]["open"],
                "high": max(item["high"] for item in ordered_minutes),
                "low": min(item["low"] for item in ordered_minutes),
                "close": ordered_minutes[-1]["close"],
                "volume": total_volume,
                "source_minute_count": len(ordered_minutes),
                "source_minute_mask": sum(
                    1
                    << int((timestamp - bar_start).total_seconds() // 60)
                    for timestamp in minutes
                ),
                "_dirty": True,
            }
        )
        return True

    @staticmethod
    def _buffer_identity(bar: dict[str, Any]) -> tuple[Any, Any, Any]:
        """Return the database identity of one buffered aggregate."""
        return (
            bar["ticker"],
            bar["timestamp"],
            bar.get("bar_size_minutes", 5),
        )

    @staticmethod
    def _snapshot_supersedes(
        incoming: dict[str, Any], buffered: dict[str, Any]
    ) -> bool:
        """Return whether an incoming revision authoritatively replaces one bar."""
        incoming_mask = incoming.get("source_minute_mask")
        buffered_mask = buffered.get("source_minute_mask")
        return (
            not isinstance(incoming_mask, int)
            or not isinstance(buffered_mask, int)
            or incoming_mask | buffered_mask == incoming_mask
        )

    def _buffer_snapshot(self, bar: dict[str, Any]) -> None:
        """Coalesce a corrected authoritative snapshot into the write buffer."""
        snapshot = {
            key: value for key, value in bar.items() if not key.startswith("_")
        }
        identity = self._buffer_identity(snapshot)
        buffered = self._buffered_snapshots.get(identity)
        if buffered is not None:
            if self._snapshot_supersedes(snapshot, buffered):
                self._buffered_snapshots[identity] = snapshot
            return
        self._buffered_snapshots[identity] = snapshot
        self._enforce_buffer_limit()

    def _enforce_buffer_limit(self) -> None:
        """Drop oldest retry entries at the configured cap, with loss telemetry."""
        overflow = len(self._buffered_snapshots) - self.max_buffered_bars
        if overflow <= 0:
            return

        for _ in range(overflow):
            self._buffered_snapshots.popitem(last=False)
        self.dropped_buffered_bars += overflow
        self.logger.error(
            "DATA LOSS: persistence retry buffer exceeded its %d-bar cap; "
            "dropped %d oldest unpersisted bar(s) (total dropped=%d). "
            "Recover the missing interval from historical data.",
            self.max_buffered_bars,
            overflow,
            self.dropped_buffered_bars,
        )

    def _flush_completed_bars(self, flush_all: bool = False) -> None:
        """Move completed bars from aggregator to buffer.

        A ticker's window ``[W, W+bar_size)`` is complete once we have observed
        a 1-min bar of THAT SAME ticker belonging to a later window (its
        per-ticker event-time watermark). A single global watermark is wrong:
        a liquid ticker advancing it into a later window would prematurely flush
        a thin ticker's still-partial window, and the thin ticker's remaining
        in-window minutes would then re-create — and corrupt — the same window.
        We cannot use wall-clock time alone either: on the 15-min delayed feed
        every bar arrives long after its event time, so a wall-clock cutoff
        would flush each window after its first 1-min bar. The wall-clock
        ``stale_cutoff`` is kept only as a memory backstop for a stalled stream.

        Args:
            flush_all: Drain every window regardless of completeness (used at
                shutdown, when no later bars will ever arrive).
        """
        bar_delta = timedelta(minutes=self.bar_size)
        correction_delta = timedelta(minutes=CORRECTION_HORIZON_MINUTES)
        now = datetime.now(timezone.utc)

        for key, bar in list(self.bar_aggregator.items()):
            ticker = key[0]
            bar_end = bar["timestamp"] + bar_delta
            watermark = self.max_event_time_by_ticker.get(ticker)
            last_received = self.last_received_wallclock_by_ticker.get(ticker)
            stalled = last_received is not None and now > last_received + timedelta(
                minutes=STREAM_STALL_MINUTES
            )
            stalled_beyond_corrections = (
                last_received is not None
                and now
                > last_received
                + timedelta(
                    minutes=STREAM_STALL_MINUTES + CORRECTION_HORIZON_MINUTES
                )
            )
            complete = flush_all or (
                (watermark is not None and watermark >= bar_end)
                or stalled
            )
            finalized = flush_all or (
                (watermark is not None and watermark > bar_end + correction_delta)
                or stalled_beyond_corrections
            )

            if complete and bar["_dirty"]:
                self._buffer_snapshot(bar)
                bar["_dirty"] = False
                timestamp_str = bar["timestamp"].strftime("%Y-%m-%d %H:%M")
                self.logger.info(
                    f"📊 {bar['ticker']:5s} | {timestamp_str} | "
                    f"O:{bar['open']:7.2f} H:{bar['high']:7.2f} "
                    f"L:{bar['low']:7.2f} C:{bar['close']:7.2f} | "
                    f"Vol:{bar['volume']:,} | "
                    f"({bar['source_minute_count']} 1-min bars)"
                )

            if finalized:
                self.bar_aggregator.pop(key)
                market_date = bar["timestamp"].astimezone(MARKET_TIMEZONE).date()
                cutoff_key = (ticker, market_date)
                prior_cutoff = self.finalized_through.get(cutoff_key)
                if prior_cutoff is None or bar["timestamp"] > prior_cutoff:
                    self.finalized_through[cutoff_key] = bar["timestamp"]

    async def _handle_message(self, message: str) -> None:
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            self.malformed_messages += 1
            self.logger.warning("Ignoring malformed WebSocket JSON message")
            return

        if not isinstance(data, list):
            data = [data]

        bars_received = 0
        for item in data:
            if not isinstance(item, dict):
                self.malformed_messages += 1
                self.logger.warning("Ignoring non-object WebSocket message item")
                continue
            event_type = item.get("ev")

            if event_type == "AM":  # Aggregate Minute
                self.minute_events_received += 1
                bars_received += 1
                if self._aggregate_bar(item):
                    self.minute_events_accepted += 1
                    self.stream_recovery_pending = False
            elif event_type == "status":
                errors = self._provider_errors([item])
                if errors:
                    self.provider_status_errors += 1
                    raise ConnectionError(
                        "Provider stream status error: " + "; ".join(errors)
                    )
                self.logger.debug("Provider stream status received")

        # Log when we receive bars (but not too verbose)
        if bars_received > 0:
            self.logger.debug(
                "📥 Received %d 1-min bars (%d accepted)",
                bars_received,
                self.minute_events_accepted,
            )

    def _write_bars_to_db(self, bars: list[dict[str, Any]]) -> int:
        """Perform one complete blocking database write in a worker thread."""
        with psycopg.connect(self.database_url) as conn:
            return load_intraday_bars(conn, bars, self.logger)

    def _restore_failed_batch(self, failed_bars: list[dict[str, Any]]) -> None:
        """Restore a failed snapshot ahead of bars received while it was writing."""
        received_during_write = list(self._buffered_snapshots.values())
        merged: OrderedDict[tuple[Any, Any, Any], dict[str, Any]] = OrderedDict()

        # Merge in linear time: a 50,000-bar outage snapshot must not stall the
        # event loop with quadratic identity scans when a write fails.
        for bars in (failed_bars, received_during_write):
            for bar in bars:
                snapshot = {
                    key: value
                    for key, value in bar.items()
                    if not key.startswith("_")
                }
                identity = self._buffer_identity(snapshot)
                buffered = merged.get(identity)
                if buffered is None or self._snapshot_supersedes(snapshot, buffered):
                    merged[identity] = snapshot

        self._buffered_snapshots = merged
        self._enforce_buffer_limit()

    async def _batch_write_to_db(self) -> bool:
        """Write buffered bars off-loop, retaining a bounded retry set on failure."""
        if not self._buffered_snapshots:
            self._last_write_error = None
            return True

        # Detach a stable snapshot. Incoming messages can continue to aggregate
        # while psycopg and load_intraday_bars execute in a worker thread.
        bars_to_write = list(self._buffered_snapshots.values())
        self._buffered_snapshots.clear()
        try:
            tickers = set(bar["ticker"] for bar in bars_to_write)
            time_range = (
                min(bar["timestamp"] for bar in bars_to_write),
                max(bar["timestamp"] for bar in bars_to_write),
            )

            inserted = await asyncio.to_thread(self._write_bars_to_db, bars_to_write)

            self.logger.info(
                f"💾 Saved {inserted} bars to database | "
                f"{len(tickers)} tickers | "
                f"{time_range[0].strftime('%H:%M')} - {time_range[1].strftime('%H:%M')}"
            )
            self.last_flush = datetime.now(timezone.utc)
            self._last_write_error = None
            return True

        except asyncio.CancelledError:
            # The worker thread may still finish (threads cannot be forcefully
            # cancelled), and the database upsert is idempotent. Retaining the
            # snapshot avoids silent loss; a later retry may be redundant.
            self._restore_failed_batch(bars_to_write)
            raise
        except Exception as exc:
            self._last_write_error = exc
            self._restore_failed_batch(bars_to_write)
            self.logger.error(
                "Database write failed; retaining %d/%d bars for retry: %s",
                len(self._buffered_snapshots),
                self.max_buffered_bars,
                _safe_exception(exc),
            )
            return False

    async def _periodic_flush(self) -> None:
        """Periodically flush buffer and completed bars."""
        try:
            while self.running:
                await asyncio.sleep(10)  # Check every 10 seconds

                self._flush_completed_bars()
                should_flush = (
                    len(self._buffered_snapshots) >= self.batch_size
                    or (
                        datetime.now(timezone.utc) - self.last_flush
                    ).total_seconds() >= self.batch_timeout
                )

                if should_flush:
                    await self._batch_write_to_db()
        except asyncio.CancelledError:
            raise
        except Exception:
            # A dead background task would otherwise leave the socket consuming
            # messages indefinitely without completion/persistence supervision.
            self.running = False
            close_error = await self._close_current_websocket()
            if close_error is not None:
                self.logger.error(
                    "WebSocket close after periodic flush failure failed: %s",
                    _safe_exception(close_error),
                )
            raise

    async def _cancel_periodic_flush(self, flush_task: asyncio.Task[None]) -> None:
        """Cancel and await the periodic task so it cannot race final flush."""
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass

    async def _close_current_websocket(self) -> Exception | None:
        """Close and clear the current socket, returning any close error."""
        websocket = self.websocket
        self.websocket = None
        if websocket is None:
            return None

        try:
            await websocket.close()
        except Exception as e:
            return e
        return None

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
                    self.reconnectable_failures += 1
                    self.stream_recovery_pending = True
                    self.logger.warning("Stream ended by server; will reconnect")
                except KeyboardInterrupt:
                    self.logger.info("Interrupted by user")
                    break
                except websockets.exceptions.ConnectionClosed:
                    self.reconnectable_failures += 1
                    self.stream_recovery_pending = True
                    self.logger.warning("Connection closed")
                except (OSError, websockets.exceptions.WebSocketException) as e:
                    self.reconnectable_failures += 1
                    self.stream_recovery_pending = True
                    self.logger.warning("WebSocket error: %s", _safe_exception(e))

                # A failed auth/subscription or an ended stream must not be
                # overwritten by the next connect() while its socket is open.
                close_error = await self._close_current_websocket()
                if close_error is not None:
                    self.logger.warning(
                        "Failed to close disconnected WebSocket: %s",
                        _safe_exception(close_error),
                    )

                if not self.running:
                    break
                self.logger.info(f"Reconnecting in {backoff:.0f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_RECONNECT_BACKOFF)
        finally:
            self.running = False
            periodic_error: Exception | None = None
            try:
                await self._cancel_periodic_flush(flush_task)
            except Exception as e:
                periodic_error = e
                self.logger.error(
                    "Periodic flush task failed: %s", _safe_exception(e)
                )
            shutdown_error: Exception | None = None
            try:
                await self.shutdown()
            except Exception as e:
                shutdown_error = e
            if periodic_error is not None:
                if shutdown_error is not None:
                    raise periodic_error from shutdown_error
                raise periodic_error
            if shutdown_error is not None:
                raise shutdown_error

    async def shutdown(self) -> None:
        """Graceful shutdown - flush buffer and close connection."""
        self.logger.info("Shutting down...")
        self.running = False

        flush_error: Exception | None = None
        flush_cause: Exception | None = None
        close_error: Exception | None = None
        try:
            # No later bars will arrive, so drain every partial window before
            # the one final persistence attempt.
            self._flush_completed_bars(flush_all=True)
            if self._buffered_snapshots:
                persisted = await self._batch_write_to_db()
                if not persisted or self._buffered_snapshots:
                    flush_cause = self._last_write_error
                    flush_error = RuntimeError(
                        "Final database flush failed with "
                        f"{len(self._buffered_snapshots)} bars still buffered"
                    )
        except Exception as e:
            flush_error = e
        finally:
            close_error = await self._close_current_websocket()
            if close_error is not None:
                self.logger.error(
                    "WebSocket close failed: %s", _safe_exception(close_error)
                )

        if flush_error is not None:
            if flush_cause is not None:
                raise flush_error from flush_cause
            raise flush_error
        if close_error is not None:
            raise close_error

        self.logger.info("✓ Shutdown complete")
