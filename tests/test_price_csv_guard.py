"""Offline regressions for historical-price artifact validation."""

from __future__ import annotations

import gzip
import logging
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

import pytest

from sawa import coldstart
from sawa.api.s3 import BulkPriceRows, PolygonS3Client
from sawa.database import load as database_load
from sawa.domain.price_validation import (
    is_plausible_daily_price_date,
    is_valid_daily_ohlcv,
)


def _write_price_csv(path: Path, row: str) -> None:
    path.write_text(
        "symbol,date,open,high,low,close,volume\n" + row,
        encoding="utf-8",
    )


def test_csv_numeric_strings_use_the_exact_daily_storage_envelope() -> None:
    valid = {
        "open": "10",
        "high": "11",
        "low": "8",
        "close": "8.5",
        "volume": "100",
    }

    assert is_valid_daily_ohlcv(valid, allow_numeric_strings=True) is True
    assert (
        is_valid_daily_ohlcv(
            {**valid, "high": "9"},
            allow_numeric_strings=True,
        )
        is False
    )
    assert (
        is_valid_daily_ohlcv(
            {**valid, "low": "9"},
            allow_numeric_strings=True,
        )
        is False
    )
    assert (
        is_valid_daily_ohlcv(
            {**valid, "volume": "1.5"},
            allow_numeric_strings=True,
        )
        is False
    )


def test_daily_price_date_requires_real_strict_iso_and_bounded_future_skew() -> None:
    today = date(2026, 8, 29)

    assert is_plausible_daily_price_date("2026-08-29", today=today) is True
    assert is_plausible_daily_price_date("2026-08-30", today=today) is True
    assert is_plausible_daily_price_date("2026-08-31", today=today) is False
    assert is_plausible_daily_price_date("2026-02-30", today=today) is False
    assert is_plausible_daily_price_date("20260829", today=today) is False


def test_s3_parser_rejects_inconsistent_ohlc(tmp_path: Path) -> None:
    archive = tmp_path / "day.csv.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as stream:
        stream.write("ticker,open,close,high,low,volume\n")
        stream.write("AAPL,10,8.5,9,8,100\n")

    client = object.__new__(PolygonS3Client)
    client.logger = logging.getLogger(__name__)

    with pytest.raises(ValueError, match="malformed OHLCV"):
        client.parse_bulk_file(str(archive), symbols={"AAPL"})


def test_download_prices_rejects_malformed_typed_response_without_artifact(
    tmp_path: Path,
) -> None:
    class _MalformedS3:
        def download_and_parse(
            self,
            target_date: date,
            symbols: set[str],
        ) -> BulkPriceRows:
            return BulkPriceRows(
                [
                    {
                        "symbol": "AAPL",
                        "open": 10,
                        "high": 9,
                        "low": 8,
                        "close": 8.5,
                        "volume": 100,
                    }
                ],
                source_found=True,
            )

    result = coldstart.download_prices(
        _MalformedS3(),  # type: ignore[arg-type]
        {"AAPL"},
        date(2026, 1, 2),
        date(2026, 1, 2),
        ["2026-01-02"],
        tmp_path,
        logging.getLogger(__name__),
    )

    assert int(result) == 0
    assert result.sourced_dates == 1
    assert result.failed_dates == {
        "2026-01-02": (
            "invalid bulk price data: bulk price row has malformed OHLCV data"
        )
    }
    assert result.artifact_files == set()
    assert list(tmp_path.glob("*.csv")) == []


def test_cached_price_preflight_rejects_later_bad_file_before_any_insert(
    tmp_path: Path,
) -> None:
    _write_price_csv(
        tmp_path / "AAPL.csv",
        "AAPL,2026-01-02,10,11,8,8.5,100\n",
    )
    _write_price_csv(
        tmp_path / "MSFT.csv",
        "MSFT,2026-01-02,10,9,8,8.5,100\n",
    )

    with mock.patch.object(database_load, "_insert_rows") as insert:
        with pytest.raises(ValueError, match="MSFT.csv at CSV row 2"):
            database_load.load_prices(object(), tmp_path)

    insert.assert_not_called()


def test_cached_price_preflight_rejects_empty_later_file_before_any_insert(
    tmp_path: Path,
) -> None:
    _write_price_csv(
        tmp_path / "AAPL.csv",
        "AAPL,2026-01-02,10,11,8,8.5,100\n",
    )
    _write_price_csv(tmp_path / "MSFT.csv", "")

    with mock.patch.object(database_load, "_insert_rows") as insert:
        with pytest.raises(RuntimeError, match="persisted no rows"):
            database_load.load_prices(object(), tmp_path)

    insert.assert_not_called()


@pytest.mark.parametrize(
    "bad_date",
    [
        "2026-02-30",
        (date.today() + timedelta(days=2)).isoformat(),
    ],
)
def test_cached_price_preflight_rejects_impossible_or_future_date_before_insert(
    tmp_path: Path,
    bad_date: str,
) -> None:
    _write_price_csv(
        tmp_path / "AAPL.csv",
        f"AAPL,{bad_date},10,11,8,8.5,100\n",
    )

    with mock.patch.object(database_load, "_insert_rows") as insert:
        with pytest.raises(ValueError, match="malformed or future"):
            database_load.load_prices(object(), tmp_path)

    insert.assert_not_called()
