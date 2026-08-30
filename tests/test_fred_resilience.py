"""Offline regression tests for partial FRED market-internals fetches."""

import logging
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from sawa.api.fred import FredClient, FredMarketInternalsResult, FredSeriesFailure
from sawa.database import load as database_load
from sawa.domain.exceptions import ProviderError


def _observations(value: str, date: str = "2026-08-28") -> list[dict[str, str]]:
    return [{"date": date, "value": value}]


def test_one_series_failure_retains_successful_rows_and_reports_failure() -> None:
    client = FredClient("test-key", logging.getLogger(__name__))

    def get_series(series_id: str, *_args: Any) -> list[dict[str, str]]:
        if series_id == "VXVCLS":
            raise RuntimeError(
                "request failed: https://fred.test?api_key=do-not-leak"
            )
        values = {"VIXCLS": "18.25", "BAMLH0A0HYM2": "3.10"}
        return _observations(values[series_id])

    try:
        with patch.object(client, "get_series", side_effect=get_series):
            result = client.get_market_internals("2026-08-28", "2026-08-28")
    finally:
        client.close()

    assert isinstance(result, FredMarketInternalsResult)
    assert result.rows == [
        {
            "date": "2026-08-28",
            "vix": "18.25",
            "vix3m": None,
            "hy_spread": "3.10",
        }
    ]
    assert result.failures == (
        FredSeriesFailure(
            field="vix3m",
            series_id="VXVCLS",
            error_type="RuntimeError",
            message="request failed: https://fred.test?api_key=<redacted>",
        ),
    )
    assert result.all_series_failed is False
    assert "do-not-leak" not in str(result.failure_details)


def test_all_series_failure_is_explicit_not_clean_empty_result() -> None:
    client = FredClient("test-key", logging.getLogger(__name__))

    try:
        with patch.object(client, "get_series", side_effect=OSError("provider down")):
            result = client.get_market_internals("2026-08-28", "2026-08-28")
    finally:
        client.close()

    assert result.rows == []
    assert result.all_series_failed is True
    assert [failure.field for failure in result.failures] == [
        "vix",
        "vix3m",
        "hy_spread",
    ]
    assert all(failure.error_type == "OSError" for failure in result.failures)


def test_all_successful_but_empty_series_are_explicit_failures() -> None:
    client = FredClient("test-key", logging.getLogger(__name__))

    try:
        with patch.object(client, "get_series", return_value=[]):
            result = client.get_market_internals("2026-08-01", "2026-08-28")
    finally:
        client.close()

    assert result.rows == []
    assert result.all_series_failed is True
    assert [failure.error_type for failure in result.failures] == [
        "EmptyResult",
        "EmptyResult",
        "EmptyResult",
    ]


def test_invalid_requested_range_fails_before_any_series_request() -> None:
    client = FredClient("test-key", logging.getLogger(__name__))

    try:
        with patch.object(client, "get_series") as get_series:
            with pytest.raises(ValueError, match="on or before"):
                client.get_market_internals("2026-08-29", "2026-08-28")
    finally:
        client.close()

    get_series.assert_not_called()


@pytest.mark.parametrize("bad_date", ["2026-8-01", "20260801", "2026-02-30"])
def test_requested_dates_require_strict_valid_iso_dates(bad_date: str) -> None:
    client = FredClient("test-key", logging.getLogger(__name__))

    try:
        with pytest.raises(ValueError):
            client.get_market_internals(bad_date, "2026-08-28")
    finally:
        client.close()


def test_get_series_rejects_observation_outside_requested_window() -> None:
    client = FredClient("test-key", logging.getLogger(__name__))
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "observations": [{"date": "2026-07-31", "value": "18.25"}]
    }

    try:
        with patch.object(client.client, "get", return_value=response):
            with pytest.raises(ProviderError, match="predates the requested window"):
                client.get_series("VIXCLS", "2026-08-01", "2026-08-28")
    finally:
        client.close()


@pytest.mark.parametrize(
    ("bad_date", "bad_value"),
    [
        ("2999-01-01", "18.25"),
        ("2026-08-28", "NaN"),
        ("2026-08-28", "Infinity"),
        ("2026-08-28", "-1"),
        ("2026-08-28", "10000"),
    ],
)
def test_invalid_provider_observation_becomes_typed_series_failure(
    bad_date: str,
    bad_value: str,
) -> None:
    client = FredClient("test-key", logging.getLogger(__name__))

    def get_series(series_id: str, *_args: Any) -> list[dict[str, str]]:
        if series_id == "VIXCLS":
            return _observations(bad_value, bad_date)
        return _observations("20.00")

    try:
        with patch.object(client, "get_series", side_effect=get_series):
            result = client.get_market_internals()
    finally:
        client.close()

    assert [failure.field for failure in result.failures] == ["vix"]
    assert result.failures[0].error_type == "ProviderError"
    assert all(row["vix"] is None for row in result.rows)


def test_hy_only_success_retains_rows_when_both_vix_series_fail() -> None:
    client = FredClient("test-key", logging.getLogger(__name__))

    def get_series(series_id: str, *_args: Any) -> list[dict[str, str]]:
        if series_id in {"VIXCLS", "VXVCLS"}:
            raise TimeoutError("volatility provider request failed")
        return _observations("3.10")

    try:
        with patch.object(client, "get_series", side_effect=get_series):
            result = client.get_market_internals("2026-08-28", "2026-08-28")
    finally:
        client.close()

    assert result.rows == [
        {
            "date": "2026-08-28",
            "vix": None,
            "vix3m": None,
            "hy_spread": "3.10",
        }
    ]
    assert [failure.field for failure in result.failures] == ["vix", "vix3m"]
    assert result.all_series_failed is False


def test_failure_state_resets_between_calls() -> None:
    client = FredClient("test-key", logging.getLogger(__name__))
    first_call = True

    def get_series(series_id: str, *_args: Any) -> list[dict[str, str]]:
        nonlocal first_call
        if first_call and series_id == "VIXCLS":
            raise TimeoutError("first call only")
        values = {
            "VIXCLS": "19.00",
            "VXVCLS": "20.00",
            "BAMLH0A0HYM2": "3.00",
        }
        if series_id == "BAMLH0A0HYM2":
            first_call = False
        return _observations(values[series_id])

    try:
        with patch.object(client, "get_series", side_effect=get_series):
            first_result = client.get_market_internals()
            second_result = client.get_market_internals()
    finally:
        client.close()

    assert [failure.field for failure in first_result.failures] == ["vix"]
    assert second_result.failures == ()
    assert second_result.rows == [
        {
            "date": "2026-08-28",
            "vix": "19.00",
            "vix3m": "20.00",
            "hy_spread": "3.00",
        }
    ]
    # The immutable first result is not rewritten by the later clean call.
    assert [failure.field for failure in first_result.failures] == ["vix"]


def test_partial_rows_load_with_coalesce_for_every_nullable_series() -> None:
    rows = [
        {
            "date": "2026-08-28",
            "vix": "18.25",
            "vix3m": None,
            "hy_spread": "3.10",
        }
    ]

    with patch.object(database_load, "_insert_rows", return_value=1) as insert_rows:
        loaded = database_load.load_market_internals(
            object(), rows, logging.getLogger(__name__)
        )

    assert loaded == 1
    assert insert_rows.call_args.args[3] == [
        {
            "date": "2026-08-28",
            "vix": "18.25",
            "vix3m": None,
            "hy_spread": "3.10",
            "put_call_ratio": None,
        }
    ]
    assert insert_rows.call_args.kwargs["coalesce_columns"] == [
        "vix",
        "vix3m",
        "hy_spread",
        "put_call_ratio",
    ]
    assert insert_rows.call_args.kwargs["strict"] is True


@pytest.mark.parametrize(
    "row",
    [
        {"date": "2026-8-28", "vix": "18.25"},
        {"date": "2026-02-30", "vix": "18.25"},
        {"date": "2999-01-01", "vix": "18.25"},
        {"date": "2026-08-28", "vix": "NaN"},
        {"date": "2026-08-28", "vix": "-0.1"},
        {"date": "2026-08-28", "vix": "10000"},
        {"date": "2026-08-28", "vix": ""},
    ],
)
def test_market_internals_loader_rejects_bad_dates_and_values(
    row: dict[str, str],
) -> None:
    with patch.object(database_load, "_insert_rows") as insert_rows:
        with pytest.raises(ValueError, match="Rejected 1 invalid"):
            database_load.load_market_internals(
                object(), [row], logging.getLogger(__name__)
            )

    insert_rows.assert_not_called()


def test_market_internals_loader_surfaces_short_write() -> None:
    rows = [{"date": date.today().isoformat(), "vix": "18.25"}]

    with patch.object(database_load, "_insert_rows", return_value=0):
        with pytest.raises(RuntimeError, match="Persisted only 0/1"):
            database_load.load_market_internals(
                object(), rows, logging.getLogger(__name__)
            )


def test_strict_insert_rolls_back_and_raises_on_first_database_error() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = None
    cursor.execute.side_effect = [
        None,
        psycopg.DataError("bad numeric"),
        None,
        None,
    ]
    conn.cursor.return_value = cursor

    with patch.object(database_load, "_get_primary_key", return_value=["date"]):
        with pytest.raises(RuntimeError, match="Atomic insert"):
            database_load._insert_rows(
                conn,
                "market_internals",
                ["date", "vix"],
                [{"date": date.today().isoformat(), "vix": "18.25"}],
                upsert=True,
                strict=True,
            )

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def test_market_internals_csv_rejects_bad_row_before_insert(tmp_path: Path) -> None:
    csv_path = tmp_path / "market_internals.csv"
    csv_path.write_text("date,vix\n2999-01-01,18.25\n", encoding="utf-8")

    with patch.object(database_load, "_insert_rows") as insert_rows:
        with pytest.raises(ValueError, match="invalid market_internals"):
            database_load.load_csv_to_table(
                object(),
                csv_path,
                "market_internals",
                {"date": "date", "vix": "vix"},
            )

    insert_rows.assert_not_called()


def test_partial_market_internals_csv_replay_uses_atomic_coalesce(
    tmp_path: Path,
) -> None:
    economy_dir = tmp_path / "economy"
    economy_dir.mkdir()
    csv_path = economy_dir / "market_internals.csv"
    csv_path.write_text(
        f"date,vix,vix3m,hy_spread\n{date.today().isoformat()},18.25,,3.10\n",
        encoding="utf-8",
    )

    with patch.object(database_load, "_insert_rows", return_value=1) as insert_rows:
        stats = database_load.load_economy(
            object(),
            economy_dir,
            logging.getLogger(__name__),
            only_tables={"market_internals"},
        )

    assert stats == {"market_internals": 1}
    assert insert_rows.call_args.args[3] == [
        {
            "date": date.today().isoformat(),
            "vix": "18.25",
            "vix3m": None,
            "hy_spread": "3.10",
            "put_call_ratio": None,
        }
    ]
    assert insert_rows.call_args.kwargs["coalesce_columns"] == [
        "vix",
        "vix3m",
        "hy_spread",
        "put_call_ratio",
    ]
    assert insert_rows.call_args.kwargs["strict"] is True
