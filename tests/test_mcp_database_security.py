"""Security regressions for the MCP raw-query boundary and audit sink."""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from typing import Any

import psycopg
import pytest

from mcp_server import database
from sawa.utils.logging import install_redaction_filters
from sawa.utils.security import open_private_text


class _FakeCursor:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[object, object, object]] = []
        self.rows = rows or []
        self.position = 0
        self.stream_calls: list[tuple[object, object]] = []
        self.stream_closed = 0
        self.fetchall_called = False

    def __enter__(self):
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(
        self,
        query: object,
        params: object = None,
        *,
        prepare: bool | None = None,
    ) -> None:
        self.calls.append((query, params, prepare))

    def fetchall(self) -> list[dict[str, Any]]:
        self.fetchall_called = True
        return self.rows

    def stream(self, query: object, params: object = None):
        self.stream_calls.append((query, params))

        def generate():
            try:
                while self.position < len(self.rows):
                    row = self.rows[self.position]
                    self.position += 1
                    yield row
            finally:
                self.stream_closed += 1

        return generate()


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.cursor_names: list[str] = []

    def cursor(self, name: str = "") -> _FakeCursor:
        self.cursor_names.append(name)
        return self._cursor


class _ConnectionInfo:
    transaction_status = psycopg.pq.TransactionStatus.IDLE


class _ConfiguredConnection(_FakeConnection):
    def __init__(self, cursor: _FakeCursor) -> None:
        super().__init__(cursor)
        self.row_factory: object = None
        self.autocommit = False
        self.info = _ConnectionInfo()
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _executed_sql(cursor: _FakeCursor) -> list[str]:
    return [
        statement if isinstance(statement, str) else statement.as_string()
        for statement, _params, _prepare in cursor.calls
    ]


def test_pool_configure_sets_readonly_and_trusted_search_path() -> None:
    cursor = _FakeCursor()
    connection = _ConfiguredConnection(cursor)

    database._configure_connection(connection)  # type: ignore[arg-type]

    assert _executed_sql(cursor) == [
        "SET default_transaction_read_only = on",
        "SET search_path TO pg_catalog, public",
    ]
    assert connection.commits == 1


def test_pool_reset_reapplies_readonly_and_trusted_search_path() -> None:
    cursor = _FakeCursor()
    connection = _ConfiguredConnection(cursor)

    database._reset_connection(connection)  # type: ignore[arg-type]

    assert _executed_sql(cursor) == [
        "DISCARD ALL",
        "SET default_transaction_read_only = on",
        "SET search_path TO pg_catalog, public",
    ]
    assert connection.autocommit is False


def test_pool_lazy_initialization_is_thread_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[object] = []

    class FakePool:
        def __init__(self, **_kwargs: object) -> None:
            # Widen the race window so an unlocked implementation reliably
            # constructs more than one singleton under this test.
            time.sleep(0.01)
            self.closed = 0
            created.append(self)

        def close(self) -> None:
            self.closed += 1

    monkeypatch.setattr(database, "_pool", None)
    monkeypatch.setattr(database, "_pool_exit_handler_registered", True)
    monkeypatch.setattr(database, "ConnectionPool", FakePool)
    monkeypatch.setattr(database, "get_database_url", lambda: "postgresql://test")

    with ThreadPoolExecutor(max_workers=16) as executor:
        pools = list(executor.map(lambda _index: database._get_pool(), range(32)))

    assert len(created) == 1
    assert all(pool is created[0] for pool in pools)

    database.close_pool()
    assert created[0].closed == 1  # type: ignore[attr-defined]


@pytest.mark.parametrize("query", ["SELECT '100%' AS pct", "SELECT 5 % 2 AS remainder"])
@pytest.mark.parametrize("params", [None, {}])
def test_no_param_query_uses_stream_protocol_without_empty_mapping(
    query: str,
    params: dict[str, Any] | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"value": 1}])
    connection = _FakeConnection(cursor)

    @contextmanager
    def fake_connection():
        yield connection

    monkeypatch.setattr(database, "get_connection", fake_connection)

    assert database.execute_query(query, params) == [{"value": 1}]
    streamed_query, streamed_params = cursor.stream_calls[-1]
    assert streamed_query is not None
    assert streamed_params is None
    assert cursor.fetchall_called is False
    assert cursor.stream_closed == 1
    assert connection.cursor_names == ["", ""]


def test_result_cap_streams_and_fails_before_fetching_every_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        [
            {"value": "small"},
            {"value": "x" * 200},
            {"value": "must-not-be-needed"},
        ]
    )
    connection = _FakeConnection(cursor)

    @contextmanager
    def fake_connection():
        yield connection

    monkeypatch.setattr(database, "get_connection", fake_connection)
    monkeypatch.setattr(database, "MAX_RESULT_BYTES", 100)
    with pytest.raises(ValueError, match="maximum serialized size"):
        database.execute_query("SELECT value FROM large_result")

    assert cursor.fetchall_called is False
    assert cursor.position == 2
    assert cursor.stream_closed == 1


def test_composable_queries_obey_universal_streamed_row_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor([{"n": 1}, {"n": 2}, {"n": 3}])
    connection = _FakeConnection(cursor)

    @contextmanager
    def fake_connection():
        yield connection

    monkeypatch.setattr(database, "get_connection", fake_connection)
    monkeypatch.setattr(database, "MAX_ROWS", 2)

    assert database.execute_query(database.sql.SQL("SELECT n FROM values")) == [
        {"n": 1},
        {"n": 2},
    ]
    assert cursor.fetchall_called is False
    # The fake stream ignores the SQL LIMIT, so the Python defense-in-depth
    # guard observes (but does not append) its third row.
    assert cursor.position == 3
    assert cursor.stream_closed == 1
    streamed_query, _ = cursor.stream_calls[-1]
    assert "LIMIT 2" in streamed_query.as_string()


def test_query_audit_is_private_and_redacts_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log_dir = tmp_path / "audit"
    text_log = log_dir / "execute_query.log"
    jsonl_log = log_dir / "execute_query.jsonl"
    monkeypatch.setattr(database, "QUERY_LOG_DIR", log_dir)
    monkeypatch.setattr(database, "QUERY_LOG_FILE", text_log)
    monkeypatch.setattr(database, "QUERY_LOG_JSONL_FILE", jsonl_log)

    sql = "SELECT * FROM companies WHERE token = 'literal-secret' AND ticker = %(ticker)s"
    params = {"ticker": "private-ticker", "access_token": "parameter-secret"}
    database.log_execute_query(sql, params)
    database.log_execute_query_result(
        sql,
        params,
        duration_ms=1.0,
        row_count=None,
        success=False,
        error="GET https://example.test/data?apiKey=url-secret failed",
    )

    combined = text_log.read_text() + jsonl_log.read_text()
    assert "literal-secret" not in combined
    assert "private-ticker" not in combined
    assert "parameter-secret" not in combined
    assert "url-secret" not in combined
    assert "<redacted>" in combined
    assert log_dir.stat().st_mode & 0o777 == 0o700
    assert text_log.stat().st_mode & 0o777 == 0o600
    assert jsonl_log.stat().st_mode & 0o777 == 0o600

    record = json.loads(jsonl_log.read_text().splitlines()[0])
    assert record["params"] == {
        "access_token": "<redacted>",
        "ticker": "<redacted>",
    }


def test_sensitive_provider_error_url_is_redacted() -> None:
    from sawa.domain.exceptions import ProviderError

    error = ProviderError(
        "request failed",
        "polygon",
        ValueError("https://api.example.test/data?apiKey=top-secret"),
    )

    assert "top-secret" not in str(error)
    assert "apiKey=<redacted>" in str(error)


def test_provider_error_redacts_credential_in_base_message() -> None:
    from sawa.domain.exceptions import ProviderError

    error = ProviderError(
        "GET https://api.example.test/data?apiKey=base-secret failed",
        "polygon",
    )

    assert "base-secret" not in str(error)
    assert "apiKey=<redacted>" in str(error)


def test_redaction_filter_protects_preconfigured_handler_and_traceback() -> None:
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s\n%(exc_text)s"))
    logger = logging.getLogger(f"security-test-{id(stream)}")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    install_redaction_filters(logger)

    try:
        raise ValueError("https://example.test/?apiKey=trace-secret")
    except ValueError:
        logger.exception("Bearer message-secret")

    rendered = stream.getvalue()
    assert "trace-secret" not in rendered
    assert "message-secret" not in rendered
    assert rendered.count("<redacted>") >= 2


def test_private_read_refuses_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_text("sensitive")
    link = tmp_path / "link"
    link.symlink_to(target)

    with pytest.raises(OSError):
        open_private_text(link, "r")


@pytest.mark.parametrize("raw", ["not-an-int", "-1", "999999999999999999999"])
def test_integer_settings_fail_safe_without_import_crash(
    raw: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MCP_TEST_INTEGER", raw)

    assert database._bounded_env_int(
        "MCP_TEST_INTEGER",
        7,
        minimum=0,
        maximum=100,
    ) == 7
