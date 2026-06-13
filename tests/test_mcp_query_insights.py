import json
from pathlib import Path

from sawa.mcp_query_insights import (
    analyze_query_log,
    classify_query,
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


def test_classify_query_separates_forensic_from_analytical() -> None:
    # Genuine agent-style lookups.
    assert classify_query("SELECT ticker, close FROM stock_prices WHERE ticker = 'AAPL'") == "analytical"
    assert classify_query("SELECT * FROM companies WHERE active = true") == "analytical"
    # Introspection / data-quality audit / EXPLAIN / comments -> forensic.
    assert classify_query("SELECT * FROM information_schema.columns") == "forensic"
    assert classify_query("SELECT pg_get_viewdef('stock_prices_live'::regclass)") == "forensic"
    assert classify_query("EXPLAIN ANALYZE SELECT 1") == "forensic"
    assert classify_query("SELECT COUNT(*) FILTER (WHERE vix IS NULL) FROM market_internals") == "forensic"
    assert classify_query("-- audit\nSELECT COUNT(*) FROM stock_prices") == "forensic"
    # Explicit source override forces forensic even for an analytical-looking query.
    assert classify_query("SELECT close FROM stock_prices", source="review") == "forensic"
    assert classify_query("SELECT close FROM stock_prices", source="agent") == "analytical"


def test_forensic_queries_excluded_from_tool_gap_signal(tmp_path: Path) -> None:
    log_path = get_query_jsonl_path(tmp_path)
    # One genuine analytical query...
    _append(log_path, {
        "timestamp": "2026-05-17T10:00:00+00:00",
        "sql": "SELECT ticker, close FROM stock_prices WHERE ticker = 'AAPL'",
        "success": True,
    })
    # ...and two forensic ones (one by SQL shape, one by explicit source).
    _append(log_path, {
        "timestamp": "2026-05-17T10:01:00+00:00",
        "sql": "SELECT COUNT(*) FILTER (WHERE sic_code IS NULL) FROM companies",
        "success": True,
    })
    _append(log_path, {
        "timestamp": "2026-05-17T10:02:00+00:00",
        "sql": "SELECT date, vix FROM market_internals ORDER BY date DESC LIMIT 6",
        "source": "review",
        "success": True,
    })

    summary = analyze_query_log(log_dir=tmp_path, window_days=30, warning_threshold=2)["summary"]

    assert summary["total_queries"] == 3
    assert summary["category_counts"] == {"analytical": 1, "forensic": 2}
    # Recent/warning are analytical-only: 1 analytical < threshold 2 -> no warning.
    assert summary["recent_queries"] == 1
    assert summary["recent_total_queries"] == 3
    assert summary["warning"] is None
    # The signal sees only stock_prices (the forensic companies/market_internals excluded).
    assert [r["value"] for r in summary["top_tables"]] == ["stock_prices"]


def test_sql_fingerprint_normalizes_literals() -> None:
    first = "SELECT * FROM companies WHERE ticker = 'AAPL' AND market_cap > 100"
    second = "select * from companies where ticker = 'MSFT' and market_cap > 200"

    assert normalize_sql(first) == normalize_sql(second)
    assert fingerprint_sql(first) == fingerprint_sql(second)

