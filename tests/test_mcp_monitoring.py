import logging

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


class _SpyNotifier:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def send(self, *, title, body, level, tags=None) -> bool:
        self.calls.append(
            {"title": title, "body": body, "level": level, "tags": tags or []}
        )
        return True


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
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

    assert len(spy.calls) == 1
    call = spy.calls[0]
    assert call["title"] == "Sawa MCP: screen_stocks failing"
    assert "screen_stocks" in call["tags"]
    assert "ValueError: bad input" in call["body"]


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
    assert len(spy.calls) == 1

    # Two further failures should not re-alert (streak reset after firing).
    for _ in range(2):
        monitoring.record_call_outcome(
            "y", success=False, duration_ms=1.0, logger=log, error=ConnectionError("c")
        )
    assert len(spy.calls) == 1

    # Third additional failure forms a new streak of three and re-alerts.
    monitoring.record_call_outcome(
        "y", success=False, duration_ms=1.0, logger=log, error=ConnectionError("c")
    )
    assert len(spy.calls) == 2


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
