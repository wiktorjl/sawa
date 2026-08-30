"""Destructive coldstart operations fail closed and stay transactional."""

import inspect
import logging
from pathlib import Path
from unittest import mock

import psycopg
import pytest

from sawa import coldstart
from sawa.database.schema import REQUIRED_SCHEMA_FILENAMES, drop_all_tables


def _sql_text(statement: object) -> str:
    if isinstance(statement, str):
        return statement
    return statement.as_string()  # type: ignore[attr-defined,no-any-return]


def _complete_schema_files(tmp_path: Path) -> list[Path]:
    schema_dir = tmp_path / "complete-schema"
    schema_dir.mkdir(exist_ok=True)
    paths = [schema_dir / name for name in sorted(REQUIRED_SCHEMA_FILENAMES)]
    for path in paths:
        path.write_text("-- offline schema sentinel\n", encoding="utf-8")
    return paths


class _CountCursor:
    def __init__(self, table_count: int = 1, failure: Exception | None = None) -> None:
        self.table_count = table_count
        self.failure = failure

    def __enter__(self) -> "_CountCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: object, params: object = None) -> None:
        if self.failure is not None:
            raise self.failure

    def fetchone(self) -> tuple[int]:
        return (self.table_count,)


class _CountConnection:
    def __init__(self, table_count: int = 1, failure: Exception | None = None) -> None:
        self.cursor_instance = _CountCursor(table_count, failure)

    def __enter__(self) -> "_CountConnection":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self) -> _CountCursor:
        return self.cursor_instance


def _run(
    tmp_path: Path,
    *,
    drop_only: bool = False,
    confirm_drop: bool = True,
) -> dict[str, object]:
    return coldstart.run_coldstart(
        api_key=None,
        s3_access_key=None,
        s3_secret_key=None,
        database_url="test-only",
        schema_dir=tmp_path / "schema",
        output_dir=tmp_path / "output",
        drop_tables=not drop_only,
        drop_only=drop_only,
        confirm_drop=confirm_drop,
        skip_downloads=True,
        logger=logging.getLogger(__name__),
    )


def test_coldstart_preserves_existing_tables_by_default() -> None:
    assert inspect.signature(coldstart.run_coldstart).parameters["drop_tables"].default is False


def test_load_only_rejects_drop_existing_before_connect(tmp_path: Path) -> None:
    with mock.patch.object(psycopg, "connect") as connect:
        with pytest.raises(ValueError, match="load-only mode cannot be combined"):
            coldstart.run_coldstart(
                api_key=None,
                s3_access_key=None,
                s3_secret_key=None,
                database_url="must-not-connect",
                schema_dir=tmp_path / "schema",
                output_dir=tmp_path / "output",
                drop_tables=True,
                load_only=True,
                logger=logging.getLogger(__name__),
            )

    connect.assert_not_called()


def test_load_only_empty_cache_is_a_failed_run(tmp_path: Path) -> None:
    conn = mock.MagicMock(name="offline_connection")
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None

    with mock.patch.object(psycopg, "connect", return_value=conn), mock.patch.object(
        coldstart,
        "get_existing_tickers_from_db",
        return_value=set(),
    ):
        stats = coldstart.run_coldstart(
            api_key=None,
            s3_access_key=None,
            s3_secret_key=None,
            database_url="offline-only",
            schema_dir=tmp_path / "schema",
            output_dir=tmp_path / "empty-cache",
            load_only=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert stats["overviews"] == 0
    assert stats["prices"] == 0
    assert stats["fatal_reasons"] == [
        "provider step failed (overviews)",
        "provider step failed (prices)",
    ]


def test_load_only_never_fetches_missing_companies_even_with_api_key(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "cache"
    overviews_csv = output_dir / "overviews" / "overviews.csv"
    overviews_csv.parent.mkdir(parents=True)
    overviews_csv.write_text("ticker,name\nAAPL,Apple\n", encoding="utf-8")
    prices_dir = output_dir / "prices"
    prices_dir.mkdir(parents=True)
    (prices_dir / "AAPL.csv").write_text(
        "date,symbol,open,high,low,close,volume\n"
        "2026-01-02,AAPL,1,1,1,1,1\n",
        encoding="utf-8",
    )

    conn = mock.MagicMock(name="offline_connection")
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    company_result = coldstart.PersistenceResult(
        1,
        table="companies",
        artifact_found=True,
        source_rows=1,
        eligible_rows=1,
    )
    price_result = coldstart.PersistenceResult(
        1,
        table="stock_prices",
        artifact_found=True,
        source_rows=1,
        eligible_rows=1,
    )

    with (
        mock.patch.object(psycopg, "connect", return_value=conn),
        mock.patch.object(coldstart, "load_companies", return_value=company_result),
        mock.patch.object(coldstart, "load_prices", return_value=price_result),
        mock.patch.object(
            coldstart,
            "get_tickers_from_csv_files",
            return_value={"MSFT"},
        ),
        mock.patch.object(
            coldstart,
            "get_existing_tickers_from_db",
            return_value={"AAPL"},
        ),
        mock.patch.object(coldstart, "PolygonClient") as polygon_client,
        mock.patch.object(coldstart, "fetch_missing_companies") as fetch_missing,
    ):
        stats = coldstart.run_coldstart(
            api_key="must-not-be-used",
            s3_access_key=None,
            s3_secret_key=None,
            database_url="offline-only",
            schema_dir=tmp_path / "schema",
            output_dir=output_dir,
            load_only=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is True
    polygon_client.assert_not_called()
    fetch_missing.assert_not_called()


def _index_connection(
    *,
    existing: int,
    eligible: list[str],
    fail_insert: bool = False,
) -> tuple[mock.MagicMock, mock.MagicMock, list[str]]:
    statements: list[str] = []
    cur = mock.MagicMock(name="index_cursor")
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = None
    cur.fetchone.return_value = (7, existing)
    cur.fetchall.return_value = [(ticker,) for ticker in eligible]
    cur.rowcount = 1

    def execute(statement: object, _params: object = None) -> None:
        rendered = _sql_text(statement)
        statements.append(rendered)
        if fail_insert and "INSERT INTO index_constituents" in rendered:
            raise RuntimeError("simulated insert failure")

    cur.execute.side_effect = execute
    conn = mock.MagicMock(name="index_connection")
    conn.cursor.return_value = cur
    return conn, cur, statements


def test_empty_index_source_preserves_existing_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = mock.MagicMock(name="unused_connection")
    monkeypatch.setattr(
        coldstart,
        "_index_fetchers",
        lambda _api_key: [("sp500", lambda _logger: [])],
    )

    result = coldstart.populate_index_constituents(
        conn,
        logging.getLogger(__name__),
        api_key="explicit-key",
    )

    assert result["sp500"] == 0
    assert "no symbols" in result.failures["sp500"]
    conn.cursor.assert_not_called()


def test_catastrophic_index_shrink_is_rejected_before_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, _cur, statements = _index_connection(existing=5, eligible=["AAPL"])
    monkeypatch.setattr(
        coldstart,
        "_index_fetchers",
        lambda _api_key: [("sp500", lambda _logger: ["AAPL"])],
    )

    result = coldstart.populate_index_constituents(
        conn,
        logging.getLogger(__name__),
        minimum_source_counts={"sp500": 1},
    )

    assert "preservation threshold" in result.failures["sp500"]
    assert not any("DELETE FROM index_constituents" in sql for sql in statements)
    conn.commit.assert_not_called()


def test_index_insert_failure_rolls_back_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, _cur, statements = _index_connection(
        existing=2,
        eligible=["AAPL", "MSFT"],
        fail_insert=True,
    )
    monkeypatch.setattr(
        coldstart,
        "_index_fetchers",
        lambda _api_key: [("sp500", lambda _logger: ["AAPL", "MSFT"])],
    )

    result = coldstart.populate_index_constituents(
        conn,
        logging.getLogger(__name__),
        minimum_source_counts={"sp500": 1},
    )

    assert result.failures["sp500"] == "simulated insert failure"
    assert any("DELETE FROM index_constituents" in sql for sql in statements)
    assert "ROLLBACK TO SAVEPOINT index_refresh" in statements
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_index_refresh_uses_explicit_api_key_and_commits_complete_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, _cur, statements = _index_connection(
        existing=2,
        eligible=["AAPL", "MSFT"],
    )
    received: list[str | None] = []

    def fetchers(api_key: str | None):
        received.append(api_key)
        return [("sp500", lambda _logger: ["AAPL", "MSFT"])]

    monkeypatch.setattr(coldstart, "_index_fetchers", fetchers)

    result = coldstart.populate_index_constituents(
        conn,
        logging.getLogger(__name__),
        api_key="explicit-key",
        minimum_source_counts={"sp500": 1},
    )

    assert received == ["explicit-key"]
    assert result == {"sp500": 2}
    assert result.failures == {}
    assert statements.count("RELEASE SAVEPOINT index_refresh") == 1
    conn.commit.assert_called_once()


def test_fresh_index_rejects_one_symbol_source_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = mock.MagicMock(name="fresh_index_connection")
    monkeypatch.setattr(
        coldstart,
        "_index_fetchers",
        lambda _api_key: [("sp500", lambda _logger: ["AAPL"])],
    )

    result = coldstart.populate_index_constituents(conn, logging.getLogger(__name__))

    assert "absolute completeness threshold" in result.failures["sp500"]
    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()


def test_index_source_above_plausible_maximum_is_rejected_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbols = [
        chr(ord("A") + index // 26) + chr(ord("A") + index % 26)
        for index in range(601)
    ]
    conn = mock.MagicMock(name="oversized_index_connection")
    monkeypatch.setattr(
        coldstart,
        "_index_fetchers",
        lambda _api_key: [("sp500", lambda _logger: symbols)],
    )

    result = coldstart.populate_index_constituents(conn, logging.getLogger(__name__))

    assert "maximum plausibility threshold" in result.failures["sp500"]
    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()


def test_fresh_index_rejects_eligible_identity_collapse_before_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbols = [
        chr(ord("A") + index // 26) + chr(ord("A") + index % 26)
        for index in range(450)
    ]
    conn, _cur, statements = _index_connection(existing=0, eligible=["AA"])
    monkeypatch.setattr(
        coldstart,
        "_index_fetchers",
        lambda _api_key: [("sp500", lambda _logger: symbols)],
    )

    result = coldstart.populate_index_constituents(conn, logging.getLogger(__name__))

    assert "source threshold" in result.failures["sp500"]
    assert not any("DELETE FROM index_constituents" in sql for sql in statements)
    conn.commit.assert_not_called()


def test_noninteractive_existing_schema_requires_explicit_confirmation(
    tmp_path: Path,
) -> None:
    conn = _CountConnection(table_count=1)
    with mock.patch.object(psycopg, "connect", return_value=conn), mock.patch.object(
        coldstart, "drop_all_tables"
    ) as drop, mock.patch("sys.stdin.isatty", return_value=False):
        stats = _run(tmp_path, drop_only=True, confirm_drop=False)

    assert stats["success"] is False
    assert stats["aborted"] is True
    drop.assert_not_called()


@pytest.mark.parametrize("drop_only", [False, True])
def test_drop_failure_stops_before_success_or_schema(
    tmp_path: Path,
    drop_only: bool,
) -> None:
    conn = _CountConnection(table_count=1)
    with mock.patch.object(psycopg, "connect", return_value=conn), mock.patch.object(
        coldstart, "drop_all_tables", return_value=False
    ) as drop, mock.patch.object(
        coldstart,
        "get_sql_files",
        return_value=_complete_schema_files(tmp_path),
    ) as get_sql_files:
        stats = _run(tmp_path, drop_only=drop_only)

    assert stats["success"] is False
    assert stats["error"] == "destructive schema cleanup failed"
    drop.assert_called_once()
    if drop_only:
        get_sql_files.assert_not_called()
    else:
        get_sql_files.assert_called_once()


@pytest.mark.parametrize(
    "schema_files",
    [[], [Path("00_setup.sql")]],
)
def test_schema_only_invalid_schema_fails_before_connect_or_drop(
    tmp_path: Path,
    schema_files: list[Path],
) -> None:
    with mock.patch.object(
        coldstart,
        "get_sql_files",
        return_value=[tmp_path / path.name for path in schema_files],
    ), mock.patch.object(psycopg, "connect") as connect, mock.patch.object(
        coldstart, "drop_all_tables"
    ) as drop:
        with pytest.raises(ValueError, match="schema directory"):
            coldstart.run_coldstart(
                api_key=None,
                s3_access_key=None,
                s3_secret_key=None,
                database_url="must-not-connect",
                schema_dir=tmp_path / "schema",
                output_dir=tmp_path / "output",
                drop_tables=True,
                schema_only=True,
                confirm_drop=True,
                logger=logging.getLogger(__name__),
            )

    connect.assert_not_called()
    drop.assert_not_called()


@pytest.mark.parametrize(
    ("missing_tables", "expected_success", "expected_commits", "expected_rollbacks"),
    [(["companies"], False, 0, 1), ([], True, 1, 0)],
)
def test_no_drop_schema_commits_only_after_required_object_verification(
    tmp_path: Path,
    missing_tables: list[str],
    expected_success: bool,
    expected_commits: int,
    expected_rollbacks: int,
) -> None:
    conn = mock.MagicMock(name="schema_transaction")
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    sql_file = tmp_path / "00_setup.sql"
    sql_file.write_text("SELECT 1;\n", encoding="utf-8")

    with (
        mock.patch.object(psycopg, "connect", return_value=conn),
        mock.patch.object(coldstart, "get_sql_files", return_value=[sql_file]),
        mock.patch.object(coldstart, "validate_schema_files"),
        mock.patch.object(
            coldstart,
            "execute_sql_files_atomically",
            return_value=(1, []),
        ) as execute_schema,
        mock.patch.object(coldstart, "verify_tables", return_value=missing_tables),
        mock.patch.object(coldstart, "verify_views", return_value=[]),
        mock.patch.object(coldstart, "verify_materialized_views", return_value=[]),
    ):
        stats = coldstart.run_coldstart(
            api_key=None,
            s3_access_key=None,
            s3_secret_key=None,
            database_url="offline-only",
            schema_dir=tmp_path,
            output_dir=tmp_path / "output",
            schema_only=True,
            drop_tables=False,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is expected_success
    execute_schema.assert_called_once_with(
        conn,
        [sql_file],
        dry_run=False,
        logger=mock.ANY,
        commit=False,
    )
    assert conn.commit.call_count == expected_commits
    assert conn.rollback.call_count == expected_rollbacks
    if missing_tables:
        assert stats["missing_schema_objects"] == {"tables": missing_tables}


def test_existing_ticker_query_error_is_not_treated_as_empty() -> None:
    cursor = mock.MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = None
    cursor.execute.side_effect = RuntimeError("ticker inventory failed")
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor

    with pytest.raises(RuntimeError, match="ticker inventory failed"):
        coldstart.get_existing_tickers_from_db(conn)


def test_table_inventory_failure_fails_closed_before_drop(tmp_path: Path) -> None:
    conn = _CountConnection(failure=RuntimeError("inventory unavailable"))
    with mock.patch.object(psycopg, "connect", return_value=conn), mock.patch.object(
        coldstart, "drop_all_tables"
    ) as drop:
        with pytest.raises(RuntimeError, match="inventory unavailable"):
            _run(tmp_path, drop_only=True)

    drop.assert_not_called()


class _DropCursor:
    def __init__(self) -> None:
        self.last_statement = ""

    def __enter__(self) -> "_DropCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: object, params: object = None) -> None:
        self.last_statement = _sql_text(statement)
        if self.last_statement.startswith("DROP FUNCTION"):
            raise psycopg.errors.SyntaxError("simulated function failure")

    def fetchall(self) -> list[tuple[str, ...]]:
        if "pg_tables" in self.last_statement:
            return [("companies",)]
        if "pg_proc" in self.last_statement:
            return [("broken_function", "")]
        raise AssertionError(f"unexpected fetch after {self.last_statement}")


class _DropConnection:
    def __init__(self) -> None:
        self.cursor_instance = _DropCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _DropCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_drop_tables_and_functions_use_one_transaction() -> None:
    conn = _DropConnection()

    result = drop_all_tables(conn, dry_run=False, logger=logging.getLogger(__name__))

    assert result is False
    assert conn.commits == 0
    assert conn.rollbacks == 1
