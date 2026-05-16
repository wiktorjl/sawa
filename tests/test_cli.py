"""CLI behavior tests."""

import logging

from sawa.cli import _log_schema_only_warning, _redact_database_url


def test_redact_database_url_removes_password() -> None:
    redacted = _redact_database_url("postgresql://alice:secret@db.example.com:5432/prod")

    assert redacted == "postgresql://alice:***@db.example.com:5432/prod"
    assert "secret" not in redacted


def test_schema_only_warning_is_loud_and_mentions_safe_upgrade(caplog) -> None:
    logger = logging.getLogger("test-schema-only-warning")

    with caplog.at_level(logging.WARNING, logger=logger.name):
        _log_schema_only_warning(logger, "postgresql://alice:secret@db.example.com/prod")

    assert "DESTRUCTIVE COMMAND: sawa coldstart --schema-only" in caplog.text
    assert "DROP AND RECREATE every table" in caplog.text
    assert "Do not run this against production" in caplog.text
    assert "sawa coldstart --no-drop" in caplog.text
    assert "postgresql://alice:***@db.example.com/prod" in caplog.text
    assert "secret" not in caplog.text
