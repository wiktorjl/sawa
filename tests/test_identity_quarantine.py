"""Scoped issuer-history quarantine cannot delete unreviewed price rows."""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from unittest import mock

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "quarantine_unadjustable_prices.py"
_SPEC = importlib.util.spec_from_file_location("quarantine_identity_test", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
quarantine = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = quarantine
_SPEC.loader.exec_module(quarantine)

DATES = [date(2021, 2, 18), date(2021, 8, 26)]
ARGUMENTS = {
    "ticker": "DFNS", "before": date(2026, 2, 9), "old_cik": "0001777946",
    "current_cik": "0001787518", "reason": "Verified historical Polygon ticker details",
    "expected_rows": 2,
}


def connection(*, archived: int = 2, cik: str = "0001787518"):
    conn = mock.Mock()

    def execute(query: str, params=()):
        response = mock.Mock()
        if "SELECT cik" in query:
            response.fetchone.return_value = (cik,)
        elif "SELECT date" in query:
            response.fetchall.return_value = [(day,) for day in DATES]
        elif "SELECT MIN(date)" in query:
            response.fetchone.return_value = (date(2026, 2, 9),)
        elif "INSERT INTO public.stock_prices_unadjustable_archive" in query:
            response.rowcount = archived
        else:
            response.rowcount = 2
        return response

    conn.execute.side_effect = execute
    return conn


def test_identity_preview_performs_only_reads_and_reports_exact_dates() -> None:
    conn = connection()
    result = quarantine.quarantine_identity_history(conn, **ARGUMENTS)
    assert result["rows"] == 2 and result["applied"] is False
    assert result["first_date"] == "2021-02-18"
    assert result["last_date"] == "2021-08-26"
    assert all(call.args[0].startswith("SELECT") for call in conn.execute.call_args_list)


def test_identity_archive_keeps_evidence_and_deletes_only_locked_dates() -> None:
    conn = connection()
    result = quarantine.quarantine_identity_history(conn, **ARGUMENTS, apply=True)
    calls = conn.execute.call_args_list
    assert result["applied"] is True
    assert any("FOR UPDATE" in call.args[0] for call in calls)
    inserted = next(
        call for call in calls
        if "INSERT INTO public.stock_prices_unadjustable_archive" in call.args[0]
    )
    assert inserted.args[1][1].obj["old_cik"] == ARGUMENTS["old_cik"]
    assert "'identity_mismatch'" in inserted.args[0]
    deleted = next(
        call for call in calls if "DELETE FROM public.stock_prices WHERE" in call.args[0]
    )
    assert deleted.args[1] == ("DFNS", DATES)
    assert calls.index(inserted) < calls.index(deleted)
    assert result["character_rows_invalidated"] == {
        "stock_character_classification": 2, "stock_character_baseline": 2,
        "stock_character_flags": 2, "stock_character_scorecard": 2,
    }
    for table in result["character_rows_invalidated"]:
        assert any(
            call.args == (f"DELETE FROM public.{table} WHERE ticker = %s", ("DFNS",))
            for call in calls
        )
    conn.commit.assert_not_called()  # transaction ownership remains with caller


def test_archive_collision_refuses_any_deletion() -> None:
    conn = connection(archived=1)
    with pytest.raises(ValueError, match="refusing deletion"):
        quarantine.quarantine_identity_history(conn, **ARGUMENTS, apply=True)
    assert not any("DELETE" in call.args[0] for call in conn.execute.call_args_list)


@pytest.mark.parametrize("changed", [{"expected_rows": 3}, {"old_cik": "1787518"}])
def test_unreviewed_identity_or_row_count_prevents_writes(changed: dict) -> None:
    conn = connection()
    with pytest.raises(ValueError):
        quarantine.quarantine_identity_history(conn, **(ARGUMENTS | changed), apply=True)
    assert all(call.args[0].startswith("SELECT") for call in conn.execute.call_args_list)


def test_changed_current_company_identity_prevents_writes() -> None:
    conn = connection(cik="1777946")
    with pytest.raises(ValueError, match="current company CIK"):
        quarantine.quarantine_identity_history(conn, **ARGUMENTS, apply=True)
    assert len(conn.execute.call_args_list) == 1
