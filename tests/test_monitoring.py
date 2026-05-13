import logging

import pytest

from sawa.utils.monitoring import monitored_run
from sawa.utils.notify import NotificationLevel, Notifier


class _CapHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _logger_with_capture() -> tuple[logging.Logger, _CapHandler]:
    log = logging.getLogger(f"test.monitoring.{id(object())}")
    log.handlers.clear()
    log.setLevel(logging.DEBUG)
    handler = _CapHandler()
    log.addHandler(handler)
    log.propagate = False
    return log, handler


class _SpyNotifier:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def send(
        self,
        *,
        title: str,
        body: str,
        level: NotificationLevel = NotificationLevel.INFO,
        tags: list[str] | None = None,
    ) -> bool:
        self.calls.append(
            {"title": title, "body": body, "level": level, "tags": tags or []}
        )
        return True


def test_monitored_run_emits_success_notification() -> None:
    log, cap = _logger_with_capture()
    spy = _SpyNotifier()

    with monitored_run("daily", logger=log, notifier=spy) as ctx:
        ctx["stats"] = {"success": True, "prices_inserted": 42}

    assert len(spy.calls) == 1
    call = spy.calls[0]
    assert call["title"] == "Sawa: daily complete"
    assert call["level"] == NotificationLevel.INFO
    assert "daily" in call["tags"]
    assert "prices_inserted: 42" in call["body"]
    # success: True is not echoed back into the body
    assert "success" not in call["body"]
    # log line includes start + complete
    msgs = [r.getMessage() for r in cap.records]
    assert any("[daily] starting" in m for m in msgs)
    assert any("[daily] complete" in m for m in msgs)


def test_monitored_run_emits_failure_notification() -> None:
    log, cap = _logger_with_capture()
    spy = _SpyNotifier()

    with pytest.raises(RuntimeError, match="boom"):
        with monitored_run("weekly", logger=log, notifier=spy) as ctx:
            ctx["stats"] = {"overviews": 7}
            raise RuntimeError("boom")

    assert len(spy.calls) == 1
    call = spy.calls[0]
    assert call["title"] == "Sawa: weekly FAILED"
    assert call["level"] == NotificationLevel.ERROR
    assert "RuntimeError: boom" in call["body"]
    assert "overviews: 7" in call["body"]
    # Failure path logs at ERROR
    assert any(r.levelno == logging.ERROR for r in cap.records)


def test_monitored_run_failure_with_no_partial_stats() -> None:
    log, _ = _logger_with_capture()
    spy = _SpyNotifier()

    with pytest.raises(ValueError):
        with monitored_run("daily", logger=log, notifier=spy):
            raise ValueError("nope")

    assert spy.calls[0]["title"] == "Sawa: daily FAILED"
    assert "Partial stats" not in spy.calls[0]["body"]


def test_monitored_run_respects_send_success_env(monkeypatch) -> None:
    monkeypatch.setenv("SAWA_NOTIFY_SUCCESS", "0")
    log, _ = _logger_with_capture()
    spy = _SpyNotifier()

    with monitored_run("daily", logger=log, notifier=spy) as ctx:
        ctx["stats"] = {"success": True}

    assert spy.calls == []


def test_monitored_run_explicit_send_success_overrides_env(monkeypatch) -> None:
    monkeypatch.setenv("SAWA_NOTIFY_SUCCESS", "0")
    log, _ = _logger_with_capture()
    spy = _SpyNotifier()

    with monitored_run("daily", logger=log, notifier=spy, send_success=True) as ctx:
        ctx["stats"] = {"success": True}

    assert len(spy.calls) == 1


def test_monitored_run_failure_alerts_even_when_success_disabled(monkeypatch) -> None:
    monkeypatch.setenv("SAWA_NOTIFY_SUCCESS", "0")
    log, _ = _logger_with_capture()
    spy = _SpyNotifier()

    with pytest.raises(RuntimeError):
        with monitored_run("daily", logger=log, notifier=spy):
            raise RuntimeError("still alerts")

    assert len(spy.calls) == 1
    assert spy.calls[0]["level"] == NotificationLevel.ERROR


def test_monitored_run_uses_default_notifier_when_none_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NTFY_TOPIC", raising=False)
    monkeypatch.delenv("SAWA_NOTIFIER", raising=False)
    log, _ = _logger_with_capture()

    # No notifier passed → falls back to get_notifier() which returns
    # NullNotifier when NTFY_TOPIC is unset. Should not raise.
    with monitored_run("daily", logger=log) as ctx:
        ctx["stats"] = {"success": True}


def test_monitored_run_protocol_compatibility() -> None:
    """Spy notifier satisfies the Notifier protocol."""
    spy: Notifier = _SpyNotifier()
    assert spy.send(title="t", body="b") is True
