"""CLI behavior tests."""

import logging
from argparse import Namespace
from unittest import mock

import pytest

from sawa import cli
from sawa.cli import _log_schema_only_warning, _redact_database_url


@pytest.mark.parametrize("failure", [False, True])
def test_treasury_command_refreshes_only_treasury_and_reports_failure(failure):
    args = Namespace(api_key="offline", database_url="offline", verbose=False, log_dir=None)
    with mock.patch.object(cli, "setup_logging", return_value=logging.getLogger(__name__)), \
         mock.patch.object(cli, "monitored_run") as monitored, \
         mock.patch("sawa.daily.refresh_treasury_yields") as refresh, \
         mock.patch("sawa.daily.run_daily") as daily:
        stats = {}
        monitored.return_value.__enter__.return_value = stats
        if failure:
            refresh.side_effect = RuntimeError("offline provider failure")
        else:
            refresh.return_value = 22
        assert cli.cmd_treasury(args) == int(failure)
        daily.assert_not_called()
        refresh.assert_called_once()
        if not failure:
            assert stats["stats"] == {"success": True, "treasury_yields": 22}


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
