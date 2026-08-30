"""Temp-file lifecycle regressions for Polygon S3 downloads."""

import gzip
import logging
import os
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from sawa.api import s3
from sawa.api.s3 import PolygonS3Client


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetObject")


class _DownloadPlan:
    def __init__(self, outcomes: Iterable[Exception | None]) -> None:
        self.outcomes = list(outcomes)
        self.paths: list[str] = []

    def download_fileobj(self, bucket: str, key: str, fileobj: object) -> None:
        self.paths.append(fileobj.name)  # type: ignore[attr-defined]
        fileobj.write(b"partial")  # type: ignore[attr-defined]
        outcome = self.outcomes.pop(0)
        if outcome is not None:
            raise outcome
        fileobj.write(b"-complete")  # type: ignore[attr-defined]


def _s3_client(outcomes: Iterable[Exception | None]) -> tuple[PolygonS3Client, _DownloadPlan]:
    plan = _DownloadPlan(outcomes)
    client = object.__new__(PolygonS3Client)
    client.logger = logging.getLogger(__name__)
    client.max_retries = len(plan.outcomes)
    client.base_delay = 0
    client.max_delay = 0
    client.client = cast(Any, plan)
    return client, plan


def test_404_removes_partial_file_and_returns_none() -> None:
    client, plan = _s3_client([_client_error("404")])

    assert client.download_day(date(2026, 1, 3)) is None
    assert len(plan.paths) == 1
    assert not Path(plan.paths[0]).exists()


def test_access_denied_removes_partial_file_and_raises_permission_error() -> None:
    client, plan = _s3_client([_client_error("AccessDenied")])

    with pytest.raises(PermissionError, match="S3 access denied"):
        client.download_day(date(2026, 1, 2))

    assert not Path(plan.paths[0]).exists()


def test_transient_failure_is_cleaned_before_success_path_is_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, plan = _s3_client([_client_error("503"), None])
    monkeypatch.setattr(s3.time, "sleep", lambda delay: None)

    result = client.download_day(date(2026, 1, 2))

    assert result == plan.paths[1]
    assert not Path(plan.paths[0]).exists()
    assert Path(plan.paths[1]).read_bytes() == b"partial-complete"
    os.unlink(plan.paths[1])


def test_exhausted_retries_remove_every_partial_and_raise_last_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    errors = [
        EndpointConnectionError(endpoint_url="https://files.invalid"),
        EndpointConnectionError(endpoint_url="https://files.invalid"),
        EndpointConnectionError(endpoint_url="https://files.invalid"),
    ]
    client, plan = _s3_client(errors)
    sleeps: list[float] = []
    monkeypatch.setattr(s3.time, "sleep", sleeps.append)

    with pytest.raises(EndpointConnectionError) as exc_info:
        client.download_day(date(2026, 1, 2))

    assert exc_info.value is errors[-1]
    assert all(not Path(path).exists() for path in plan.paths)
    assert sleeps == [0, 0]


def test_unlink_failure_does_not_mask_primary_access_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client, plan = _s3_client([_client_error("403")])
    real_unlink = os.unlink
    monkeypatch.setattr(s3.os, "unlink", lambda path: (_ for _ in ()).throw(OSError("busy")))

    with pytest.raises(PermissionError, match="S3 access denied"):
        client.download_day(date(2026, 1, 2))

    assert "Failed to remove partial S3 download" in caplog.text
    assert Path(plan.paths[0]).exists()
    real_unlink(plan.paths[0])


def test_explicit_empty_symbol_filter_loads_no_bulk_rows(tmp_path: Path) -> None:
    archive = tmp_path / "day.csv.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as stream:
        stream.write("ticker,open,close,high,low,volume\n")
        stream.write("AAPL,1,2,3,1,100\n")

    client = object.__new__(PolygonS3Client)
    client.logger = logging.getLogger(__name__)

    assert client.parse_bulk_file(str(archive), symbols=set()) == []
