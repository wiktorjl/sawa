"""Analyze MCP execute_query usage for missing-tool signals."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "sawa.mcp.query_insights.v1"
DEFAULT_WARNING_THRESHOLD = 25
DEFAULT_WINDOW_DAYS = 7
DEFAULT_TOP_N = 10

_STRING_RE = re.compile(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"")
_NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?\b")
_COMMENT_RE = re.compile(r"(--[^\n]*|/\*.*?\*/)", re.DOTALL)
_SPACE_RE = re.compile(r"\s+")
_TABLE_RE = re.compile(
    r"\b(?:from|join)\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)",
    re.IGNORECASE,
)
_SELECT_RE = re.compile(r"\bselect\b(?P<select>.*?)\bfrom\b", re.IGNORECASE | re.DOTALL)
_WHERE_RE = re.compile(
    r"\bwhere\b(?P<where>.*?)(?:\bgroup\s+by\b|\border\s+by\b|\blimit\b|$)",
    re.IGNORECASE | re.DOTALL,
)
_IDENTIFIER_RE = re.compile(r"\b([a-zA-Z_][\w]*)(?:\.([a-zA-Z_][\w]*))?\b")
_PREDICATE_RE = re.compile(
    r"\b([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)\s*"
    r"(?:=|<>|!=|<=|>=|<|>|\blike\b|\bilike\b|\bin\b)",
    re.IGNORECASE,
)
_SQL_KEYWORDS = {
    "and",
    "as",
    "asc",
    "between",
    "case",
    "cast",
    "coalesce",
    "count",
    "date",
    "desc",
    "distinct",
    "else",
    "end",
    "false",
    "from",
    "group",
    "in",
    "is",
    "join",
    "left",
    "limit",
    "max",
    "min",
    "not",
    "null",
    "on",
    "or",
    "order",
    "over",
    "partition",
    "select",
    "sum",
    "then",
    "true",
    "when",
    "where",
    "with",
}


def get_query_log_dir(log_dir: Path | str | None = None) -> Path:
    """Return the MCP query audit log directory."""
    if log_dir:
        return Path(log_dir)
    return Path(os.environ.get("MCP_QUERY_LOG_DIR") or Path.home() / ".sawa" / "logs")


def get_query_jsonl_path(log_dir: Path | str | None = None) -> Path:
    """Return the structured execute_query audit log path."""
    return get_query_log_dir(log_dir) / "execute_query.jsonl"


def get_query_cache_path(log_dir: Path | str | None = None) -> Path:
    """Return the cached insight summary path."""
    return get_query_log_dir(log_dir) / "execute_query_insights.json"


def normalize_sql(sql: str) -> str:
    """Normalize SQL into a stable, low-cardinality query shape."""
    normalized = _COMMENT_RE.sub(" ", sql)
    normalized = _STRING_RE.sub("?", normalized)
    normalized = _NUMBER_RE.sub("?", normalized)
    normalized = re.sub(r"\bin\s*\([^)]*\)", "in (?)", normalized, flags=re.IGNORECASE)
    return _SPACE_RE.sub(" ", normalized).strip().lower()


def fingerprint_sql(sql: str) -> str:
    """Return a short fingerprint for a normalized SQL shape."""
    normalized = normalize_sql(sql)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]


def extract_tables(sql: str) -> list[str]:
    """Extract table-like identifiers from FROM and JOIN clauses."""
    tables = []
    for match in _TABLE_RE.finditer(sql):
        table = match.group(1).lower()
        if table not in {"select", "with"}:
            tables.append(table)
    return sorted(set(tables))


def extract_selected_columns(sql: str) -> list[str]:
    """Extract likely selected columns from the outer SELECT list."""
    match = _SELECT_RE.search(sql)
    if not match:
        return []
    select_part = match.group("select")
    if "*" in select_part:
        return ["*"]

    columns = []
    for match in _IDENTIFIER_RE.finditer(select_part):
        first = match.group(1).lower()
        second = match.group(2)
        column = (second or first).lower()
        if column not in _SQL_KEYWORDS:
            columns.append(column)
    return sorted(set(columns))


def extract_filter_columns(sql: str) -> list[str]:
    """Extract likely filter columns from WHERE predicates."""
    match = _WHERE_RE.search(sql)
    if not match:
        return []

    columns = []
    for pred in _PREDICATE_RE.finditer(match.group("where")):
        column = pred.group(1).split(".")[-1].lower()
        if column not in _SQL_KEYWORDS:
            columns.append(column)
    return sorted(set(columns))


# A query is "forensic" — DB introspection or data-quality auditing, not an
# agent answering a market question — when its source says so explicitly, or
# when the SQL itself is introspective. Forensic queries still get counted, but
# they are excluded from the missing-tool signal (top tables/patterns + the
# high-usage warning) so a code review or audit can't masquerade as agents
# repeatedly hitting a gap.
FORENSIC_SOURCES = {"review", "forensic", "audit"}

_INTROSPECTION_RE = re.compile(
    r"\binformation_schema\b|\bpg_catalog\b|\bpg_[a-z_]+\b|\bto_regclass\b|\bversion\s*\(\s*\)",
    re.I,
)
_FORENSIC_SQL_RE = re.compile(
    r"\bfilter\s*\(\s*where\b|\bexcept\b|\bintersect\b|\bpg_sleep\b",
    re.I,
)


def classify_query(sql: str, source: str | None = None) -> str:
    """Bucket a logged query as ``analytical`` or ``forensic``.

    ``analytical`` queries are genuine agent usage and drive the missing-tool
    signal; ``forensic`` queries are introspection / data-quality audits and are
    kept out of that signal. An explicit non-agent ``source`` forces forensic;
    otherwise the SQL is inspected (comments, system catalogs, EXPLAIN, set-diff
    gap checks, COUNT(...) FILTER audits).
    """
    if source and source.strip().lower() in FORENSIC_SOURCES:
        return "forensic"
    text = sql or ""
    if "--" in text or "/*" in text:
        return "forensic"
    if text.lstrip().lower().startswith("explain"):
        return "forensic"
    if _INTROSPECTION_RE.search(text) or _FORENSIC_SQL_RE.search(text):
        return "forensic"
    return "analytical"


def _empty_stats() -> dict[str, Any]:
    return {
        "total_queries": 0,
        "successful_queries": 0,
        "failed_queries": 0,
        "unknown_outcome_queries": 0,
        "category_counts": {"analytical": 0, "forensic": 0},
        "daily_counts": {},
        "analytical_daily_counts": {},
        "tables": {},
        "selected_columns": {},
        "filter_columns": {},
        "fingerprints": {},
    }


def _empty_cache() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": None,
        "source_log": str(get_query_jsonl_path()),
        "state": {"jsonl_offset": 0, "jsonl_size": 0},
        "stats": _empty_stats(),
        "summary": {},
    }


def _load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _empty_cache()
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return _empty_cache()
    if data.get("schema_version") != SCHEMA_VERSION:
        return _empty_cache()
    data.setdefault("state", {"jsonl_offset": 0, "jsonl_size": 0})
    data.setdefault("stats", _empty_stats())
    return data


def _bump(mapping: dict[str, int], values: list[str]) -> None:
    for value in values:
        mapping[value] = int(mapping.get(value, 0)) + 1


def _date_key(timestamp: Any) -> str:
    if isinstance(timestamp, str) and len(timestamp) >= 10:
        return timestamp[:10]
    return date.today().isoformat()


def _record_query(stats: dict[str, Any], record: dict[str, Any]) -> None:
    sql = str(record.get("sql") or "").strip()
    if not sql:
        return

    stats["total_queries"] = int(stats.get("total_queries", 0)) + 1
    if record.get("success") is True:
        stats["successful_queries"] = int(stats.get("successful_queries", 0)) + 1
    elif record.get("success") is False:
        stats["failed_queries"] = int(stats.get("failed_queries", 0)) + 1
    else:
        stats["unknown_outcome_queries"] = int(stats.get("unknown_outcome_queries", 0)) + 1

    day = _date_key(record.get("timestamp"))
    daily = stats.setdefault("daily_counts", {})
    daily[day] = int(daily.get(day, 0)) + 1

    category = classify_query(sql, record.get("source"))
    cats = stats.setdefault("category_counts", {"analytical": 0, "forensic": 0})
    cats[category] = int(cats.get(category, 0)) + 1

    # Forensic queries are counted above but kept out of the missing-tool signal
    # (top tables/columns/patterns + the recent-usage warning).
    if category != "analytical":
        return

    a_daily = stats.setdefault("analytical_daily_counts", {})
    a_daily[day] = int(a_daily.get(day, 0)) + 1

    tables = extract_tables(sql)
    selected = extract_selected_columns(sql)
    filters = extract_filter_columns(sql)
    _bump(stats.setdefault("tables", {}), tables)
    _bump(stats.setdefault("selected_columns", {}), selected)
    _bump(stats.setdefault("filter_columns", {}), filters)

    fingerprint = fingerprint_sql(sql)
    fingerprints = stats.setdefault("fingerprints", {})
    info = fingerprints.setdefault(
        fingerprint,
        {
            "count": 0,
            "normalized_sql": normalize_sql(sql),
            "example_sql": sql,
            "tables": {},
            "selected_columns": {},
            "filter_columns": {},
            "first_seen": record.get("timestamp"),
            "last_seen": record.get("timestamp"),
        },
    )
    info["count"] = int(info.get("count", 0)) + 1
    info["last_seen"] = record.get("timestamp")
    if not info.get("first_seen"):
        info["first_seen"] = record.get("timestamp")
    _bump(info.setdefault("tables", {}), tables)
    _bump(info.setdefault("selected_columns", {}), selected)
    _bump(info.setdefault("filter_columns", {}), filters)


def _iter_jsonl_records(path: Path, offset: int) -> tuple[list[dict[str, Any]], int, int]:
    if not path.exists():
        return [], 0, 0

    size = path.stat().st_size
    if offset > size:
        offset = 0

    records = []
    with path.open("rb") as fh:
        fh.seek(offset)
        for raw_line in fh:
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict):
                records.append(record)
        new_offset = fh.tell()
    return records, new_offset, size


def _top(mapping: dict[str, int], limit: int) -> list[dict[str, Any]]:
    counter = Counter({key: int(value) for key, value in mapping.items()})
    return [{"value": key, "count": count} for key, count in counter.most_common(limit)]


def _top_fingerprints(fingerprints: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    rows = []
    for fingerprint, info in fingerprints.items():
        rows.append(
            {
                "fingerprint": fingerprint,
                "count": int(info.get("count", 0)),
                "tables": _top(info.get("tables", {}), 5),
                "filter_columns": _top(info.get("filter_columns", {}), 5),
                "selected_columns": _top(info.get("selected_columns", {}), 5),
                "example_sql": info.get("example_sql"),
                "normalized_sql": info.get("normalized_sql"),
                "first_seen": info.get("first_seen"),
                "last_seen": info.get("last_seen"),
            }
        )
    rows.sort(key=lambda row: row["count"], reverse=True)
    return rows[:limit]


def _recent_query_count(daily_counts: dict[str, int], window_days: int) -> int:
    cutoff = date.today() - timedelta(days=max(window_days - 1, 0))
    total = 0
    for day, count in daily_counts.items():
        try:
            parsed = date.fromisoformat(day)
        except ValueError:
            continue
        if parsed >= cutoff:
            total += int(count)
    return total


def _build_summary(
    cache: dict[str, Any],
    *,
    new_records: int,
    window_days: int,
    warning_threshold: int,
    top_n: int,
) -> dict[str, Any]:
    stats = cache.get("stats", {})
    # The warning and "recent" reflect ANALYTICAL queries only — forensic
    # introspection/audit traffic must not trip the missing-tool signal.
    recent_queries = _recent_query_count(stats.get("analytical_daily_counts", {}), window_days)
    recent_total = _recent_query_count(stats.get("daily_counts", {}), window_days)
    category_counts = stats.get("category_counts", {"analytical": 0, "forensic": 0})
    warning = None
    if recent_queries >= warning_threshold:
        warning = (
            "High custom SQL usage detected: "
            f"{recent_queries} analytical execute_query calls in the last "
            f"{window_days} days. Run `sawa mcp-query-insights` to review "
            "candidate MCP tools."
        )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "new_records": new_records,
        "total_queries": int(stats.get("total_queries", 0)),
        "successful_queries": int(stats.get("successful_queries", 0)),
        "failed_queries": int(stats.get("failed_queries", 0)),
        "unknown_outcome_queries": int(stats.get("unknown_outcome_queries", 0)),
        "category_counts": {
            "analytical": int(category_counts.get("analytical", 0)),
            "forensic": int(category_counts.get("forensic", 0)),
        },
        "window_days": window_days,
        "recent_queries": recent_queries,
        "recent_total_queries": recent_total,
        "warning_threshold": warning_threshold,
        "warning": warning,
        "top_tables": _top(stats.get("tables", {}), top_n),
        "top_filter_columns": _top(stats.get("filter_columns", {}), top_n),
        "top_selected_columns": _top(stats.get("selected_columns", {}), top_n),
        "top_query_patterns": _top_fingerprints(stats.get("fingerprints", {}), top_n),
    }


def analyze_query_log(
    *,
    log_dir: Path | str | None = None,
    reset: bool = False,
    window_days: int = DEFAULT_WINDOW_DAYS,
    warning_threshold: int = DEFAULT_WARNING_THRESHOLD,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    """Analyze new structured execute_query records and update the cache."""
    directory = get_query_log_dir(log_dir)
    directory.mkdir(parents=True, exist_ok=True)
    cache_path = get_query_cache_path(directory)
    jsonl_path = get_query_jsonl_path(directory)

    cache = _empty_cache() if reset else _load_cache(cache_path)
    cache["source_log"] = str(jsonl_path)

    state = cache.setdefault("state", {})
    stats = cache.setdefault("stats", _empty_stats())
    offset = 0 if reset else int(state.get("jsonl_offset", 0) or 0)

    records, new_offset, size = _iter_jsonl_records(jsonl_path, offset)
    if offset > size:
        cache = _empty_cache()
        cache["source_log"] = str(jsonl_path)
        state = cache["state"]
        stats = cache["stats"]
        records, new_offset, size = _iter_jsonl_records(jsonl_path, 0)

    for record in records:
        _record_query(stats, record)

    state["jsonl_offset"] = new_offset
    state["jsonl_size"] = size
    summary = _build_summary(
        cache,
        new_records=len(records),
        window_days=window_days,
        warning_threshold=warning_threshold,
        top_n=top_n,
    )
    cache["generated_at"] = summary["generated_at"]
    cache["summary"] = summary

    cache_path.write_text(json.dumps(cache, indent=2, sort_keys=True, default=str) + "\n")
    return cache


def load_cached_query_warning(log_dir: Path | str | None = None) -> str | None:
    """Read the cached high-usage warning without scanning query logs."""
    cache_path = get_query_cache_path(log_dir)
    try:
        cache = json.loads(cache_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    if cache.get("schema_version") != SCHEMA_VERSION:
        return None
    warning = cache.get("summary", {}).get("warning")
    return str(warning) if warning else None


def format_query_insights(cache: dict[str, Any], *, top_n: int = DEFAULT_TOP_N) -> str:
    """Format cached query insights for CLI output."""
    summary = cache.get("summary", {})
    lines = [
        "MCP execute_query insights",
        "",
        f"Source: {cache.get('source_log')}",
        f"Generated: {summary.get('generated_at') or cache.get('generated_at')}",
        f"New records processed: {summary.get('new_records', 0)}",
        f"Total custom queries: {summary.get('total_queries', 0)}",
        (
            "Breakdown: "
            f"{summary.get('category_counts', {}).get('analytical', 0)} analytical, "
            f"{summary.get('category_counts', {}).get('forensic', 0)} forensic "
            "(introspection/audit, excluded from the tool-gap signal)"
        ),
        (
            f"Recent analytical queries: {summary.get('recent_queries', 0)} "
            f"in the last {summary.get('window_days', DEFAULT_WINDOW_DAYS)} days "
            f"({summary.get('recent_total_queries', 0)} incl. forensic)"
        ),
        (
            "Outcomes: "
            f"{summary.get('successful_queries', 0)} success, "
            f"{summary.get('failed_queries', 0)} failed, "
            f"{summary.get('unknown_outcome_queries', 0)} unknown"
        ),
    ]

    if summary.get("warning"):
        lines.extend(["", f"WARNING: {summary['warning']}"])

    def add_table(title: str, rows: list[dict[str, Any]]) -> None:
        lines.extend(["", title])
        if not rows:
            lines.append("  none")
            return
        for row in rows[:top_n]:
            lines.append(f"  {row['count']:>4}  {row['value']}")

    add_table("Top tables", summary.get("top_tables", []))
    add_table("Top filter columns", summary.get("top_filter_columns", []))
    add_table("Top selected columns", summary.get("top_selected_columns", []))

    patterns = summary.get("top_query_patterns", [])[:top_n]
    lines.extend(["", "Top query patterns"])
    if not patterns:
        lines.append("  none")
    for pattern in patterns:
        tables = ", ".join(row["value"] for row in pattern.get("tables", [])) or "unknown"
        filters = ", ".join(row["value"] for row in pattern.get("filter_columns", [])) or "none"
        lines.append(
            f"  {pattern['count']:>4}  {pattern['fingerprint']}  "
            f"tables=[{tables}] filters=[{filters}]"
        )
        example = str(pattern.get("example_sql") or "").strip().replace("\n", " ")
        if example:
            lines.append(f"        {example[:180]}")

    return "\n".join(lines)

