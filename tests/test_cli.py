"""CLI behavior tests."""

import logging
from unittest import mock

import pytest

from sawa import cli
from sawa.cli import _log_schema_only_warning, _redact_database_url


def test_redact_database_url_removes_password() -> None:
    redacted = _redact_database_url("postgresql://alice:secret@db.example.com:5432/prod")

    assert redacted == "postgresql://db.example.com:5432/prod"
    assert "secret" not in redacted


def test_redact_database_url_removes_query_fragment_and_userinfo() -> None:
    secret = "ssl-secret"
    redacted = _redact_database_url(
        "postgresql://alice:password@db.example.com/prod"
        f"?sslpassword={secret}&token=query-secret#fragment-secret"
    )

    assert redacted == "postgresql://db.example.com/prod"
    assert secret not in redacted
    assert "query-secret" not in redacted
    assert "fragment-secret" not in redacted


def test_schema_only_warning_is_loud_and_mentions_safe_upgrade(caplog) -> None:
    logger = logging.getLogger("test-schema-only-warning")

    with caplog.at_level(logging.WARNING, logger=logger.name):
        _log_schema_only_warning(logger, "postgresql://alice:secret@db.example.com/prod")

    assert "DESTRUCTIVE COMMAND: sawa coldstart --schema-only" in caplog.text
    assert "DROP AND RECREATE every table" in caplog.text
    assert "Do not run this against production" in caplog.text
    assert "sawa coldstart --no-drop" in caplog.text
    assert "postgresql://db.example.com/prod" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.parametrize(
    "argv",
    [
        ["sawa", "daily", "--news-only", "--skip-news"],
        ["sawa", "corporate-actions", "--splits-only", "--dividends-only"],
        ["sawa", "corporate-actions", "--splits-only", "--include-earnings"],
    ],
)
def test_mutually_exclusive_workflow_modes_are_rejected(argv: list[str]) -> None:
    with mock.patch("sys.argv", argv), pytest.raises(SystemExit) as exc_info:
        cli.main()

    assert exc_info.value.code == 2
