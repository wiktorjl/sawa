import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Condition

import pytest

from mcp_server import monitoring


class _CapHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _logger_with_capture() -> tuple[logging.Logger, _CapHandler]:
    log = logging.getLogger(f"test.mcp.{id(object())}")
    log.handlers.clear()
    log.setLevel(logging.DEBUG)
    handler = _CapHandler()
    log.addHandler(handler)
    log.propagate = False
    return log, handler


def test_file_logging_setup_failure_is_best_effort_and_closes_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.was_closed = False

        def setFormatter(self, fmt: logging.Formatter | None) -> None:  # noqa: N802
            raise OSError("formatter setup failed")

        def close(self) -> None:
            self.was_closed = True
            super().close()

    handler = FailingHandler()
    monkeypatch.setattr(
        monitoring,
        "_PrivateTimedRotatingFileHandler",
        lambda *args, **kwargs: handler,
    )
    log, _capture = _logger_with_capture()

    assert monitoring.configure_file_logging(log) is None
    assert handler.was_closed is True
    assert handler not in logging.getLogger().handlers


def test_monitoring_bounds_utf8_errors_and_tool_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log, capture = _logger_with_capture()
    spy = _SpyNotifier()
    monkeypatch.setattr(monitoring, "_FAILURE_ALERT_THRESHOLD", 1)
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: spy)
    secret = "monitor-secret"

    monitoring.record_call_outcome(
        "tool-" + "x" * 10_000,
        success=False,
        duration_ms=1.0,
        logger=log,
        error=RuntimeError(f"api_key={secret} " + "界" * 10_000),
    )

    messages = [record.getMessage() for record in capture.records]
    assert messages
    assert all(len(message.encode("utf-8")) < 3000 for message in messages)
    assert secret not in "".join(messages)
    assert spy.wait_for_calls(1)
    assert len(spy.calls) == 1
    assert len(spy.calls[0]["body"].encode("utf-8")) < 2400
    assert secret not in spy.calls[0]["body"]
    assert all(len(tag.encode("utf-8")) <= 128 for tag in spy.calls[0]["tags"])


class _SpyNotifier:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self._condition = Condition()

    def send(self, *, title, body, level, tags=None) -> bool:
        with self._condition:
            self.calls.append(
                {"title": title, "body": body, "level": level, "tags": tags or []}
            )
            self._condition.notify_all()
        return True

    def wait_for_calls(self, count: int) -> bool:
        with self._condition:
            return self._condition.wait_for(lambda: len(self.calls) >= count, timeout=1)


@pytest.fixture(autouse=True)
def _reset_counters(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Persist the cross-process failure counter under a temp dir so tests do
    # not touch the real ~/.sawa/logs store and stay isolated from each other.
    monkeypatch.setenv("MCP_LOG_DIR", str(tmp_path))
    monitoring.reset_counters()


def test_success_logs_ok_line() -> None:
    log, cap = _logger_with_capture()
    monitoring.record_call_outcome(
        "get_stock_prices", success=True, duration_ms=12.5, logger=log
    )
    msgs = [r.getMessage() for r in cap.records]
    assert any("tool=get_stock_prices" in m and "status=ok" in m for m in msgs)


def test_failure_increments_counter_without_alert(monkeypatch: pytest.MonkeyPatch) -> None:
    log, _ = _logger_with_capture()
    spy = _SpyNotifier()
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: spy)

    monitoring.record_call_outcome(
        "screen_stocks",
        success=False,
        duration_ms=10.0,
        logger=log,
        error=RuntimeError("boom"),
    )
    monitoring.record_call_outcome(
        "screen_stocks",
        success=False,
        duration_ms=10.0,
        logger=log,
        error=RuntimeError("boom"),
    )

    assert spy.calls == []  # below default threshold (3)


def test_third_failure_fires_alert(monkeypatch: pytest.MonkeyPatch) -> None:
    log, _ = _logger_with_capture()
    spy = _SpyNotifier()
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: spy)

    for _ in range(3):
        monitoring.record_call_outcome(
            "screen_stocks",
            success=False,
            duration_ms=5.0,
            logger=log,
            error=ValueError("bad input"),
        )

    assert spy.wait_for_calls(1)
    assert len(spy.calls) == 1
    call = spy.calls[0]
    assert call["title"] == "Sawa MCP: screen_stocks failing"
    assert "screen_stocks" in call["tags"]
    assert "ValueError: bad input" in call["body"]


def test_alert_and_log_redact_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    log, cap = _logger_with_capture()
    spy = _SpyNotifier()
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: spy)
    secret = "notify-secret"

    for _ in range(3):
        monitoring.record_call_outcome(
            "screen_stocks",
            success=False,
            duration_ms=5.0,
            logger=log,
            error=RuntimeError(str({"apiKey": secret, "status": "failed"})),
        )

    assert spy.wait_for_calls(1)
    assert len(spy.calls) == 1
    assert secret not in spy.calls[0]["body"]
    assert "apiKey': <redacted>" in spy.calls[0]["body"]
    assert all(secret not in record.getMessage() for record in cap.records)


def test_success_resets_counter(monkeypatch: pytest.MonkeyPatch) -> None:
    log, cap = _logger_with_capture()
    spy = _SpyNotifier()
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: spy)

    # Two failures, then success — counter resets.
    for _ in range(2):
        monitoring.record_call_outcome(
            "x", success=False, duration_ms=1.0, logger=log, error=RuntimeError("e")
        )
    monitoring.record_call_outcome("x", success=True, duration_ms=1.0, logger=log)

    # Two more failures should not alert.
    for _ in range(2):
        monitoring.record_call_outcome(
            "x", success=False, duration_ms=1.0, logger=log, error=RuntimeError("e")
        )

    assert spy.calls == []
    msgs = [r.getMessage() for r in cap.records]
    assert any("recovered after" in m for m in msgs)


def test_alert_resets_streak(monkeypatch: pytest.MonkeyPatch) -> None:
    log, _ = _logger_with_capture()
    spy = _SpyNotifier()
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: spy)

    for _ in range(3):
        monitoring.record_call_outcome(
            "y", success=False, duration_ms=1.0, logger=log, error=ConnectionError("c")
        )
    assert spy.wait_for_calls(1)
    assert len(spy.calls) == 1

    # Two further failures should not re-alert (streak reset after firing).
    for _ in range(2):
        monitoring.record_call_outcome(
            "y", success=False, duration_ms=1.0, logger=log, error=ConnectionError("c")
        )
    assert spy.wait_for_calls(1)
    assert len(spy.calls) == 1

    # Third additional failure forms a new streak of three and re-alerts.
    monitoring.record_call_outcome(
        "y", success=False, duration_ms=1.0, logger=log, error=ConnectionError("c")
    )
    assert spy.wait_for_calls(2)
    assert len(spy.calls) == 2


def test_single_failure_logs_warning_with_context(monkeypatch: pytest.MonkeyPatch) -> None:
    log, cap = _logger_with_capture()
    spy = _SpyNotifier()
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: spy)

    monitoring.record_call_outcome(
        "screen_stocks",
        success=False,
        duration_ms=10.0,
        logger=log,
        error=RuntimeError("boom"),
    )

    # Even a single failure is logged at WARNING with actionable context.
    warnings = [r for r in cap.records if r.levelno == logging.WARNING]
    assert warnings, "expected a WARNING log on the first failure"
    msg = warnings[0].getMessage()
    assert "tool=screen_stocks" in msg
    assert "consecutive_failures=1" in msg
    assert "boom" in msg
    # Below threshold, so no alert yet.
    assert spy.calls == []


def test_counter_persists_across_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate ephemeral processes: each call only shares state via the
    # persisted JSON file, never an in-memory dict. Three failures in a row
    # should still cross the threshold and alert.
    spy = _SpyNotifier()
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: spy)

    for _ in range(3):
        log, _ = _logger_with_capture()
        monitoring.record_call_outcome(
            "screen_stocks",
            success=False,
            duration_ms=5.0,
            logger=log,
            error=ValueError("bad input"),
        )

    assert spy.wait_for_calls(1)
    assert len(spy.calls) == 1


def test_separate_tools_track_independently(monkeypatch: pytest.MonkeyPatch) -> None:
    log, _ = _logger_with_capture()
    spy = _SpyNotifier()
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: spy)

    for _ in range(2):
        monitoring.record_call_outcome(
            "a", success=False, duration_ms=1.0, logger=log, error=RuntimeError("e")
        )
    for _ in range(2):
        monitoring.record_call_outcome(
            "b", success=False, duration_ms=1.0, logger=log, error=RuntimeError("e")
        )
    # Each tool below threshold individually.
    assert spy.calls == []


@pytest.mark.parametrize(
    "payload",
    [
        '{"list_companies": NaN}',
        '{"list_companies": Infinity}',
        '{"list_companies": true}',
        '{"list_companies": -1}',
        '{"list_companies": 1.5}',
        '{"list_companies": 1e308}',
    ],
)
def test_corrupt_counter_values_are_ignored(payload: str) -> None:
    state = monitoring._state_file()
    state.write_text(payload, encoding="utf-8")

    assert monitoring._load_counts() == {}


def test_concurrent_failures_do_not_lose_counter_updates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log, _ = _logger_with_capture()
    monkeypatch.setattr(monitoring, "_FAILURE_ALERT_THRESHOLD", 1000)

    def fail_once(_: int) -> None:
        monitoring.record_call_outcome(
            "concurrent",
            success=False,
            duration_ms=1.0,
            logger=log,
            error=RuntimeError("boom"),
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(fail_once, range(50)))

    assert monitoring._load_counts() == {"concurrent": 50}


def test_pending_alert_survives_failed_delivery_and_newer_generation() -> None:
    monitoring._queue_failure_alert(
        tool="screen_stocks",
        count=3,
        err_type="RuntimeError",
        safe_error="first",
    )
    first = monitoring._claim_pending_alert()
    assert first is not None

    monitoring._complete_pending_alert(first, delivered=False)
    retained = monitoring._load_pending_alerts()["screen_stocks"]
    assert retained["lease_id"] == ""

    second = monitoring._claim_pending_alert()
    assert second is not None
    monitoring._queue_failure_alert(
        tool="screen_stocks",
        count=3,
        err_type="ValueError",
        safe_error="newer",
    )
    monitoring._complete_pending_alert(second, delivered=True)

    newer = monitoring._load_pending_alerts()["screen_stocks"]
    assert newer["generation"] == 2
    assert newer["error"] == "newer"
    assert newer["lease_id"] == ""

    final = monitoring._claim_pending_alert()
    assert final is not None
    monitoring._complete_pending_alert(final, delivered=True)
    assert monitoring._load_pending_alerts() == {}


def test_invalid_failure_threshold_uses_safe_positive_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKEN_THRESHOLD", "not-an-int")
    assert monitoring._positive_int_env("BROKEN_THRESHOLD", 3) == 3
    monkeypatch.setenv("BROKEN_THRESHOLD", "0")
    assert monitoring._positive_int_env("BROKEN_THRESHOLD", 3) == 1


def test_failed_durable_enqueue_retains_threshold_streak_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log, capture = _logger_with_capture()
    monkeypatch.setattr(monitoring, "_FAILURE_ALERT_THRESHOLD", 1)
    real_save = monitoring._save_pending_alerts
    monkeypatch.setattr(monitoring, "_save_pending_alerts", lambda _pending: False)
    monkeypatch.setattr(monitoring, "_wake_alert_worker", lambda _logger: None)

    monitoring.record_call_outcome(
        "known_tool",
        success=False,
        duration_ms=1,
        logger=log,
        error=RuntimeError("provider failed"),
    )

    assert monitoring._load_counts() == {"known_tool": 1}
    assert monitoring._load_pending_alerts() == {}

    monkeypatch.setattr(monitoring, "_save_pending_alerts", real_save)
    monitoring.record_call_outcome(
        "known_tool",
        success=False,
        duration_ms=1,
        logger=log,
        error=RuntimeError("provider failed again"),
    )

    assert monitoring._load_counts() == {"known_tool": 0}
    assert "known_tool" in monitoring._load_pending_alerts()
    messages = "\n".join(record.getMessage() for record in capture.records)
    assert messages.count("streak retained for retry") == 1


def test_claim_is_not_returned_when_lease_cannot_be_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert monitoring._queue_failure_alert(
        tool="known_tool",
        count=3,
        err_type="RuntimeError",
        safe_error="failed",
    )
    monkeypatch.setattr(monitoring, "_save_pending_alerts", lambda _pending: False)

    assert monitoring._claim_pending_alert() is None


def test_alert_delivery_cycle_recovers_after_transient_internal_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log, capture = _logger_with_capture()
    claimed = {
        "tool": "known_tool",
        "count": 3,
        "error_type": "RuntimeError",
        "error": "failed",
        "generation": 1,
        "lease_id": "lease",
        "lease_until": 1.0,
    }
    calls = iter([RuntimeError("temporary lock failure"), claimed, None])

    def claim():
        value = next(calls)
        if isinstance(value, Exception):
            raise value
        return value

    completed: list[tuple[dict, bool]] = []
    monkeypatch.setattr(monitoring, "_claim_pending_alert", claim)
    monkeypatch.setattr(monitoring, "_send_failure_alert", lambda *_a, **_k: True)
    monkeypatch.setattr(
        monitoring,
        "_complete_pending_alert",
        lambda alert, delivered: completed.append((alert, delivered)),
    )

    monitoring._deliver_available_alerts(log)
    monitoring._deliver_available_alerts(log)

    assert completed == [(claimed, True)]
    assert any("durable alert worker failed" in r.getMessage() for r in capture.records)
