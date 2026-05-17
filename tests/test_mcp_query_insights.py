import json
from pathlib import Path

from sawa.mcp_query_insights import (
    analyze_query_log,
    fingerprint_sql,
    get_query_jsonl_path,
    load_cached_query_warning,
    normalize_sql,
)


def _append(path: Path, record: dict) -> None:
    with path.open("a") as fh:
        fh.write(json.dumps(record) + "\n")


def test_query_insights_incrementally_analyzes_structured_log(tmp_path: Path) -> None:
    log_path = get_query_jsonl_path(tmp_path)
    _append(
        log_path,
        {
            "timestamp": "2026-05-17T10:00:00+00:00",
            "sql": "SELECT ticker, close FROM stock_prices WHERE ticker = %(ticker)s",
            "params": {"ticker": "AAPL"},
            "success": True,
            "row_count": 10,
        },
    )
    _append(
        log_path,
        {
            "timestamp": "2026-05-17T10:01:00+00:00",
            "sql": "SELECT ticker, close FROM stock_prices WHERE ticker = %(ticker)s",
            "params": {"ticker": "MSFT"},
            "success": True,
            "row_count": 10,
        },
    )

    cache = analyze_query_log(
        log_dir=tmp_path,
        window_days=30,
        warning_threshold=2,
    )

    summary = cache["summary"]
    assert summary["new_records"] == 2
    assert summary["total_queries"] == 2
    assert summary["warning"] is not None
    assert summary["top_tables"][0] == {"value": "stock_prices", "count": 2}
    assert summary["top_filter_columns"][0] == {"value": "ticker", "count": 2}
    assert summary["top_query_patterns"][0]["count"] == 2

    unchanged = analyze_query_log(log_dir=tmp_path, window_days=30, warning_threshold=2)
    assert unchanged["summary"]["new_records"] == 0
    assert unchanged["summary"]["total_queries"] == 2

    _append(
        log_path,
        {
            "timestamp": "2026-05-17T10:02:00+00:00",
            "sql": "SELECT ticker FROM earnings WHERE report_date >= %(start_date)s",
            "params": {"start_date": "2026-05-01"},
            "success": False,
            "error": "timeout",
        },
    )

    updated = analyze_query_log(log_dir=tmp_path, window_days=30, warning_threshold=2)
    assert updated["summary"]["new_records"] == 1
    assert updated["summary"]["total_queries"] == 3
    assert updated["summary"]["failed_queries"] == 1


def test_cached_query_warning_reads_summary_without_log_scan(tmp_path: Path) -> None:
    log_path = get_query_jsonl_path(tmp_path)
    _append(
        log_path,
        {
            "timestamp": "2026-05-17T10:00:00+00:00",
            "sql": "SELECT * FROM companies",
            "success": True,
        },
    )

    analyze_query_log(log_dir=tmp_path, window_days=30, warning_threshold=1)
    warning = load_cached_query_warning(tmp_path)

    assert warning is not None
    assert "High custom SQL usage detected" in warning


def test_sql_fingerprint_normalizes_literals() -> None:
    first = "SELECT * FROM companies WHERE ticker = 'AAPL' AND market_cap > 100"
    second = "select * from companies where ticker = 'MSFT' and market_cap > 200"

    assert normalize_sql(first) == normalize_sql(second)
    assert fingerprint_sql(first) == fingerprint_sql(second)

