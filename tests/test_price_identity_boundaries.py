"""Reviewed ticker-reuse archives cannot be undone by historical price writers."""

from datetime import date
from unittest import mock

import pytest

from sawa import add_symbol, daily, split_adjust
from sawa.database import load
from sawa.database.price_identity import (
    filter_identity_price_rows,
    get_identity_price_cutoffs,
)
from sawa.domain.exceptions import ProviderError
from tests.test_split_adjust import _FakeConn

CUTOFF = date(2025, 8, 29)


def connection(*, exists=True, cik="0001130713", records=None):
    conn = mock.MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = ("stock_prices_unadjustable_archive" if exists else None,)
    cur.fetchall.return_value = (
        [("BBBY", CUTOFF, "0001130713", cik)] if records is None else records
    )
    return conn


def price(ticker="BBBY", session="2025-08-29", **overrides):
    return {
        "ticker": ticker, "date": session, "open": 10, "high": 12,
        "low": 9, "close": 11, "volume": 100, **overrides,
    }


def test_absent_archive_needs_no_migration_or_second_query():
    conn = connection(exists=False)
    assert get_identity_price_cutoffs(conn) == {}
    assert conn.cursor.return_value.__enter__.return_value.execute.call_count == 1


def test_only_identity_archives_supply_persistent_cutoffs():
    conn = connection(cik="1130713")
    assert get_identity_price_cutoffs(conn, ["BBBY"]) == {"BBBY": CUTOFF}
    query, params = conn.cursor.return_value.__enter__.return_value.execute.call_args.args
    assert "->>'archive_reason' = 'identity_mismatch'" in query
    assert "to_jsonb(a)" in query  # legacy archive columns may be absent
    assert params == (["BBBY"],)
    assert get_identity_price_cutoffs(connection(records=[])) == {}


@pytest.mark.parametrize("cik", [None, "bad", "0000886158"])
def test_changed_company_identity_requires_new_review(cik):
    with pytest.raises(ValueError, match="requires identity review"):
        get_identity_price_cutoffs(connection(cik=cik))


def test_filter_keeps_boundary_other_tickers_and_never_changes_caller_rows():
    rows = [price(session="2023-05-02"), price(), price("AAPL", "2021-01-04")]
    kept, excluded = filter_identity_price_rows(rows, {"BBBY": CUTOFF})
    assert kept == rows[1:]
    assert excluded == 1 and len(rows) == 3


def test_daily_writer_preserves_source_eligible_excluded_and_invalid_counts():
    conn = connection()
    rows = [price(session="2023-05-02"), price(), price(high=1)]
    result = daily.insert_prices(conn, rows, mock.Mock())
    assert int(result) == 1
    assert (result.source_rows, result.eligible_rows, result.excluded_identity_rows) == (3, 1, 1)
    assert result.skipped_rows == 1 and not result.fully_persisted
    cur = conn.cursor.return_value.__enter__.return_value
    writes = [
        call for call in cur.execute.call_args_list
        if "INSERT INTO stock_prices" in str(call.args[0])
    ]
    assert len(writes) == 1 and writes[0].args[1][1] == "2025-08-29"
    with pytest.raises(RuntimeError, match="persisted"):
        load.require_complete_persistence(result, expected_rows=3)


def test_all_invalid_daily_rows_report_source_loss_without_any_database_query():
    conn = connection()
    result = daily.insert_prices(conn, [price(high=1)], mock.Mock())
    assert result == 0 and result.source_rows == result.skipped_rows == 1
    assert result.eligible_rows == 0 and not result.fully_persisted
    conn.cursor.assert_not_called()


def test_only_known_identity_exclusions_can_complete_a_rest_replay():
    result = daily.insert_prices(
        connection(), [price(session="2023-05-02"), price()], mock.Mock(), commit=False,
    )
    assert result == 1 and result.fully_persisted
    load.require_complete_persistence(result, expected_rows=2, require_nonempty=True)


@pytest.mark.parametrize("loader", ["generic", "bootstrap"])
@pytest.mark.parametrize("only_old", [False, True])
def test_csv_replay_excludes_old_issuer_before_split_transformation(
    tmp_path, loader, only_old,
):
    path = tmp_path / "BBBY.csv"
    path.write_text(
        "symbol,date,open,high,low,close,volume\nBBBY,2023-05-02,10,12,9,11,100\n"
        + ("" if only_old else "BBBY,2025-08-29,10,12,9,11,100\n")
    )
    transform = mock.Mock(side_effect=lambda row: row)
    adjuster = mock.Mock(adjust_row=transform)
    with mock.patch.object(load, "_insert_rows", side_effect=lambda *a, **k: len(a[3])):
        if loader == "generic":
            result = load.load_csv_to_table(
                connection(), path, "stock_prices", load.PRICE_COLUMNS,
                strict=True, row_transform=transform,
            )
        else:
            adjuster = mock.MagicMock(adjust_row=transform)
            result = load.load_prices(connection(), tmp_path, split_adjuster=adjuster)
    expected = 0 if only_old else 1
    assert int(result) == result.eligible_rows == expected
    assert result.source_rows == expected + 1
    assert result.excluded_identity_rows == 1 and result.skipped_rows == 0
    assert transform.call_count == expected
    load.require_complete_persistence(result, expected_rows=expected + 1, require_nonempty=True)


def test_generic_low_level_writer_refuses_identity_filter_bypass():
    conn = connection()
    with pytest.raises(ValueError, match="Refusing direct insertion"):
        load._insert_rows(conn, "stock_prices", list(load.PRICE_COLUMNS.values()),
                          [price(session="2023-05-02")], True)
    cur = conn.cursor.return_value.__enter__.return_value
    assert not any("INSERT INTO" in str(c.args[0]) for c in cur.execute.call_args_list)


def test_onboarding_requires_at_least_one_current_issuer_price():
    with mock.patch.object(
        add_symbol, "fetch_prices_via_api", return_value=[price(session="2023-05-02")]
    ):
        with pytest.raises(ProviderError, match="no eligible current-issuer"):
            add_symbol.fetch_and_insert_prices(
                connection(), mock.Mock(), "BBBY", "2021-01-01", "2026-09-25", mock.Mock()
            )


def test_mixed_split_batch_uses_each_tickers_own_history_boundary():
    conn = _FakeConn()
    stored = {"AAPL": {date(2021, 1, 4)}, "BBBY": {CUTOFF}}
    fetched = [price("AAPL", "2021-01-04"), price(session="2023-05-02"), price()]
    with (
        mock.patch.object(split_adjust.psycopg, "connect", return_value=conn),
        mock.patch.object(split_adjust, "PolygonClient"),
        mock.patch.object(split_adjust, "SyncRateLimiter"),
        mock.patch.object(split_adjust, "get_earliest_price_date", return_value=date(2021, 1, 4)),
        mock.patch.object(split_adjust, "get_existing_price_dates", return_value=stored),
        mock.patch.object(split_adjust, "known_identity_conflict", return_value=None),
        mock.patch.object(split_adjust, "fetch_prices_via_api", return_value=fetched) as fetch,
        mock.patch.object(split_adjust, "insert_prices", return_value=2) as insert,
        mock.patch.object(split_adjust, "refresh_52week_extremes_if_needed"),
    ):
        result = split_adjust.refresh_split_adjusted_prices(
            "offline", "offline", tickers=["AAPL", "BBBY"], logger=mock.Mock()
        )
    assert result["success"] is True
    assert result["excluded_out_of_window_rows"] == 1
    assert fetch.call_args.kwargs["start_dates"] == {"AAPL": "2021-01-04", "BBBY": "2025-08-29"}
    assert insert.call_args.args[1] == [fetched[0], fetched[2]]


def test_automatic_restore_sql_also_blocks_legacy_rows_before_identity_cutoff():
    from tests.test_restore_quarantined_prices import restore

    assert "WHERE NOT EXISTS" in restore.INSERT_ROW
    assert "->>'archive_reason' = 'identity_mismatch'" in restore.INSERT_ROW
    assert "proposed.date < a.stale_basis_cutoff" in restore.INSERT_ROW
