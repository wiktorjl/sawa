"""Helpers for inspecting log files written by ``setup_logging``.

Backs the ``sawa logs`` CLI subcommand: list runs, tail the most recent
log of a given type, grep across recent logs.

Log filename shape: ``<run_name>_<YYYYMMDD>_<HHMMSS>.log``. Rotation
backups end in ``.log.<N>`` and are excluded from listings.
"""

from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from sawa.utils.logging import get_default_log_dir

_LOG_NAME_RE = re.compile(r"^(?P<type>[A-Za-z_-]+)_(?P<ymd>\d{8})_(?P<hms>\d{6})\.log$")


@dataclass(frozen=True)
class LogEntry:
    """A single log file on disk, parsed from its filename."""

    path: Path
    run_type: str
    when: datetime

    @property
    def size(self) -> int:
        return self.path.stat().st_size

    @property
    def filename(self) -> str:
        return self.path.name


def _parse(path: Path) -> LogEntry | None:
    """Return a ``LogEntry`` if the filename matches the standard shape."""
    m = _LOG_NAME_RE.match(path.name)
    if not m:
        return None
    try:
        when = datetime.strptime(f"{m['ymd']}_{m['hms']}", "%Y%m%d_%H%M%S")
    except ValueError:
        return None
    return LogEntry(path=path, run_type=m["type"], when=when)


def list_runs(
    log_dir: Path | None = None,
    *,
    run_type: str | None = None,
    days: int | None = None,
) -> list[LogEntry]:
    """List parsed log entries, newest first.

    Args:
        log_dir: Override the default ``~/.sawa/logs/``.
        run_type: Filter to a specific run name (``daily``, ``weekly`` …).
        days: Only return entries written in the last ``days`` days.
    """
    directory = log_dir or get_default_log_dir()
    if not directory.exists():
        return []

    cutoff = datetime.now() - timedelta(days=days) if days else None
    entries: list[LogEntry] = []
    for path in directory.iterdir():
        if not path.is_file():
            continue
        entry = _parse(path)
        if entry is None:
            continue
        if run_type and entry.run_type != run_type:
            continue
        if cutoff and entry.when < cutoff:
            continue
        entries.append(entry)
    entries.sort(key=lambda e: e.when, reverse=True)
    return entries


def latest_run(
    log_dir: Path | None = None,
    *,
    run_type: str | None = None,
) -> LogEntry | None:
    """Return the most recent log entry, optionally filtered by type."""
    entries = list_runs(log_dir, run_type=run_type)
    return entries[0] if entries else None


def tail_lines(path: Path, n: int) -> list[str]:
    """Return the last ``n`` lines of a text file without loading all of it."""
    if n <= 0:
        return []
    with path.open(errors="replace") as fh:
        return list(deque(fh, maxlen=n))


def grep_runs(
    pattern: str,
    *,
    log_dir: Path | None = None,
    run_type: str | None = None,
    days: int | None = 7,
    context: int = 0,
    max_matches: int = 200,
) -> list[tuple[LogEntry, int, str]]:
    """Search recent logs for ``pattern`` (regex).

    Returns a list of ``(entry, line_number, line)`` triples, newest run
    first, capped at ``max_matches``.
    """
    regex = re.compile(pattern)
    results: list[tuple[LogEntry, int, str]] = []
    for entry in list_runs(log_dir, run_type=run_type, days=days):
        with entry.path.open(errors="replace") as fh:
            for lineno, line in enumerate(fh, start=1):
                if regex.search(line):
                    results.append((entry, lineno, line.rstrip()))
                    if len(results) >= max_matches:
                        return results
    return results


def format_entry_row(entry: LogEntry) -> str:
    """Single-line listing row for the CLI."""
    size_kb = entry.size / 1024
    if size_kb > 1024:
        size_str = f"{size_kb / 1024:.1f}M"
    else:
        size_str = f"{size_kb:.1f}K"
    return (
        f"{entry.when.strftime('%Y-%m-%d %H:%M:%S')}  "
        f"{entry.run_type:<12} "
        f"{size_str:>8}  "
        f"{entry.filename}"
    )
