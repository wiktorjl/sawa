from datetime import datetime, timedelta
from pathlib import Path

from sawa.logs import format_entry_row, grep_runs, latest_run, list_runs, tail_lines


def _make(dir_: Path, name: str, lines: list[str] | None = None) -> Path:
    path = dir_ / name
    path.write_text("\n".join(lines or []) + ("\n" if lines else ""))
    return path


def test_list_runs_filters_and_sorts(tmp_path: Path) -> None:
    _make(tmp_path, "daily_20260510_120000.log", ["ok"])
    _make(tmp_path, "daily_20260512_120000.log", ["ok"])
    _make(tmp_path, "weekly_20260511_120000.log", ["ok"])
    # Non-conforming names are skipped.
    _make(tmp_path, "random_file.txt")
    _make(tmp_path, "daily_20260512_120000.log.1")  # rotation backup

    entries = list_runs(tmp_path)
    assert [e.filename for e in entries] == [
        "daily_20260512_120000.log",
        "weekly_20260511_120000.log",
        "daily_20260510_120000.log",
    ]

    daily_only = list_runs(tmp_path, run_type="daily")
    assert [e.filename for e in daily_only] == [
        "daily_20260512_120000.log",
        "daily_20260510_120000.log",
    ]


def test_list_runs_days_filter(tmp_path: Path) -> None:
    now = datetime.now()
    fresh = now.strftime("%Y%m%d_%H%M%S")
    old_dt = now - timedelta(days=30)
    old = old_dt.strftime("%Y%m%d_%H%M%S")
    _make(tmp_path, f"daily_{fresh}.log", ["ok"])
    _make(tmp_path, f"daily_{old}.log", ["ok"])

    recent = list_runs(tmp_path, days=7)
    assert len(recent) == 1
    assert fresh in recent[0].filename


def test_latest_run(tmp_path: Path) -> None:
    _make(tmp_path, "daily_20260510_120000.log", ["a"])
    _make(tmp_path, "daily_20260512_120000.log", ["b"])
    latest = latest_run(tmp_path)
    assert latest is not None
    assert latest.filename == "daily_20260512_120000.log"


def test_tail_lines(tmp_path: Path) -> None:
    path = _make(
        tmp_path,
        "daily_20260512_120000.log",
        ["one", "two", "three", "four", "five"],
    )
    assert tail_lines(path, 2) == ["four\n", "five\n"]
    assert tail_lines(path, 0) == []
    assert tail_lines(path, 100) == [
        "one\n",
        "two\n",
        "three\n",
        "four\n",
        "five\n",
    ]


def test_grep_runs(tmp_path: Path) -> None:
    _make(
        tmp_path,
        "daily_20260512_120000.log",
        ["INFO ok", "ERROR boom", "INFO ok"],
    )
    _make(
        tmp_path,
        "weekly_20260511_120000.log",
        ["nothing relevant"],
    )

    matches = grep_runs(r"ERROR", log_dir=tmp_path, days=None)
    assert len(matches) == 1
    entry, lineno, line = matches[0]
    assert entry.filename == "daily_20260512_120000.log"
    assert lineno == 2
    assert "boom" in line


def test_format_entry_row(tmp_path: Path) -> None:
    path = _make(tmp_path, "daily_20260512_120000.log", ["x" * 2048])
    entry = list_runs(tmp_path)[0]
    row = format_entry_row(entry)
    assert "daily" in row
    assert "daily_20260512_120000.log" in row
    assert path.name in row
