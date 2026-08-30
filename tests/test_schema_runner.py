"""Transactional schema-runner regressions."""

import logging
from pathlib import Path
from unittest import mock

import psycopg
import pytest

from sawa.database import schema
from sawa.database.schema import execute_sql_files_atomically


class _Cursor:
    def __init__(self, executed: list[str]) -> None:
        self.executed = executed

    def __enter__(self):
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: str) -> None:
        self.executed.append(statement)
        if statement == "FAIL":
            raise psycopg.ProgrammingError("sentinel schema failure")


class _Connection:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self.executed)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _sql_file(tmp_path: Path, name: str, contents: str) -> Path:
    path = tmp_path / name
    path.write_text(contents)
    return path


def test_schema_set_commits_once_after_every_file_succeeds(tmp_path: Path) -> None:
    conn = _Connection()
    files = [
        _sql_file(tmp_path, "01_one.sql", "ONE"),
        _sql_file(tmp_path, "02_two.sql", "TWO"),
    ]

    success, failures = execute_sql_files_atomically(
        conn, files, False, logging.getLogger(__name__)
    )

    assert (success, failures) == (2, [])
    assert conn.executed == ["SET LOCAL search_path TO public", "ONE", "TWO"]
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_schema_set_rolls_everything_back_and_stops_on_failure(tmp_path: Path) -> None:
    conn = _Connection()
    files = [
        _sql_file(tmp_path, "01_one.sql", "ONE"),
        _sql_file(tmp_path, "02_fail.sql", "FAIL"),
        _sql_file(tmp_path, "03_never.sql", "THREE"),
    ]

    success, failures = execute_sql_files_atomically(
        conn, files, False, logging.getLogger(__name__)
    )

    assert (success, failures) == (0, ["02_fail.sql"])
    assert conn.executed == ["SET LOCAL search_path TO public", "ONE", "FAIL"]
    assert conn.commits == 0
    assert conn.rollbacks == 1


@pytest.mark.parametrize(
    ("missing_tables", "expected_rc", "expected_commits", "expected_rollbacks"),
    [(["companies"], 1, 0, 1), ([], 0, 1, 0)],
)
def test_schema_main_commits_only_after_required_object_verification(
    tmp_path: Path,
    missing_tables: list[str],
    expected_rc: int,
    expected_commits: int,
    expected_rollbacks: int,
) -> None:
    sql_file = _sql_file(tmp_path, "00_setup.sql", "SELECT 1;")
    conn = mock.MagicMock(name="schema_main_connection")
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None

    with (
        mock.patch(
            "sys.argv",
            [
                "sawa-schema",
                "--database-url",
                "offline-only",
                "--schema-dir",
                str(tmp_path),
                "--force",
            ],
        ),
        mock.patch.object(schema, "get_sql_files", return_value=[sql_file]),
        mock.patch.object(schema, "validate_schema_files"),
        mock.patch.object(schema.psycopg, "connect", return_value=conn),
        mock.patch.object(
            schema,
            "execute_sql_files_atomically",
            return_value=(1, []),
        ) as execute_schema,
        mock.patch.object(schema, "verify_tables", return_value=missing_tables),
        mock.patch.object(schema, "verify_views", return_value=[]),
        mock.patch.object(schema, "verify_materialized_views", return_value=[]),
    ):
        rc = schema.main()

    assert rc == expected_rc
    execute_schema.assert_called_once_with(
        conn,
        [sql_file],
        False,
        mock.ANY,
        commit=False,
    )
    assert conn.commit.call_count == expected_commits
    assert conn.rollback.call_count == expected_rollbacks


def test_schema_main_redacts_database_error_details(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "schema-ssl-secret"
    sql_file = _sql_file(tmp_path, "00_setup.sql", "SELECT 1;")
    logger = logging.getLogger("schema-main-redaction")
    failure = psycopg.ProgrammingError(
        f"connection failed sslpassword={secret} host=db.invalid"
    )

    with (
        mock.patch(
            "sys.argv",
            [
                "sawa-schema",
                "--database-url",
                "offline-only",
                "--schema-dir",
                str(tmp_path),
                "--force",
            ],
        ),
        mock.patch.object(schema, "setup_logging", return_value=logger),
        mock.patch.object(schema, "get_sql_files", return_value=[sql_file]),
        mock.patch.object(schema, "validate_schema_files"),
        mock.patch.object(schema.psycopg, "connect", side_effect=failure),
        caplog.at_level(logging.ERROR, logger=logger.name),
    ):
        rc = schema.main()

    assert rc == 1
    assert secret not in caplog.text
    assert "<redacted>" in caplog.text
