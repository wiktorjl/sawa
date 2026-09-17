"""The scheduler must treat dotenv/state paths as untrusted input — and must
never fail silently before it can alert (incident 2026-09-04..09-15).

Every test here stubs the notification paths (`notify`, `notify_via_sawa`,
`notify_last_resort`, a fake `sawa` binary, a fake `curl`): the real
`sawa notify` loads the repository .env and pushes to the operator's phone.
"""

from __future__ import annotations

import fcntl
import os
import re
import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "market_scheduler.sh"

# Records notify() calls instead of running `sawa notify`.
RECORD_NOTIFY = 'notify() { printf "%s|%s|%s\\n" "${3:-info}" "$1" "$2" >> "$HOME/notify-calls"; }; '


def _scheduler_shell(tmp_path: Path, command: str) -> subprocess.CompletedProcess[str]:
    # Let setup_env activate the repository's test environment without copying
    # credentials or installing anything in the temporary project.
    if not (tmp_path / ".venv").exists():
        (tmp_path / ".venv").symlink_to(REPO_ROOT / ".venv")
    env = os.environ.copy()
    env["HOME"] = str(tmp_path / "home")
    env["TMPDIR"] = str(tmp_path)
    return subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; PROJECT_DIR="$2"; initialize_scheduler; ' + command,
            "scheduler-test",
            str(SCRIPT),
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )


def _notify_calls(tmp_path: Path) -> list[str]:
    path = tmp_path / "home" / "notify-calls"
    return path.read_text().splitlines() if path.exists() else []


def test_dotenv_value_is_data_not_shell_code(tmp_path: Path) -> None:
    marker = tmp_path / "executed"
    literal = f"$(touch {marker})"
    (tmp_path / ".env").write_text(f"POLYGON_API_KEY={literal}\n")

    result = _scheduler_shell(tmp_path, 'setup_env; printf "%s" "$POLYGON_API_KEY"')

    assert result.returncode == 0, result.stderr
    assert result.stdout == literal
    assert not marker.exists()
    assert (tmp_path / ".env").stat().st_mode & 0o777 == 0o600


def test_dotenv_rejects_process_control_variables(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("POLYGON_API_KEY=safe\nPYTHONPATH=/tmp/attacker\n")

    result = _scheduler_shell(tmp_path, RECORD_NOTIFY + "setup_env")

    assert result.returncode != 0
    assert "not allowed" in result.stderr
    assert "process-control variable" in result.stderr
    # The reason is now in scheduler.log too, not only in cron's stderr.
    log = (tmp_path / "home" / ".sawa" / "scheduler" / "scheduler.log").read_text()
    assert "could not safely parse .env: process-control variable" in log
    assert "PYTHONPATH" in log
    # setup_env never pushes; main()'s EXIT trap owns that alert.
    assert _notify_calls(tmp_path) == []


def test_dotenv_filters_unknown_keys_without_exporting_or_aborting(tmp_path: Path) -> None:
    # Regression for the 2026-09-04..09-15 outage: one unknown key (UW_KEY)
    # made setup_env fail and main() exit before any job or alert.
    (tmp_path / ".env").write_text(
        "POLYGON_API_KEY=safe\nUW_KEY=not-a-scheduler-key\nMCP_LOG_LEVEL=debug\n"
    )

    result = _scheduler_shell(
        tmp_path,
        RECORD_NOTIFY + 'setup_env; test ! -e "$HOME/notify-calls"; '
        "notify_ignored_env_keys_once; notify_ignored_env_keys_once; "
        'printf "%s|%s|%s" "$POLYGON_API_KEY" "${UW_KEY:-}" "${MCP_LOG_LEVEL:-}"',
    )

    assert result.returncode == 0, result.stderr
    polygon, uw_key, mcp = result.stdout.split("|")
    assert polygon == "safe"  # allowlisted keys still exported
    assert uw_key == "" and mcp == ""  # unknown keys never exported
    assert "not allowed" not in result.stderr
    assert "ignoring .env key that is not in the scheduler allowlist: UW_KEY" in result.stderr
    assert "ignoring .env key that is not in the scheduler allowlist: MCP_LOG_LEVEL" in result.stderr
    assert "not-a-scheduler-key" not in result.stderr  # names only, never values
    state = tmp_path / "home" / ".sawa" / "scheduler"
    assert (state / "env_ignored_UW_KEY").exists()
    assert (state / "env_ignored_MCP_LOG_LEVEL").exists()
    # setup_env itself never pushes (the `test ! -e` above); main()'s hook
    # pushes once and the per-key markers de-duplicate the second call.
    calls = _notify_calls(tmp_path)
    assert len(calls) == 1
    level, title, body = calls[0].split("|", 2)
    assert level == "warning"
    assert title == "Sawa Scheduler: ignoring .env key(s)"
    assert "MCP_LOG_LEVEL" in body and "UW_KEY" in body
    assert "not-a-scheduler-key" not in body


def test_ignored_env_key_marker_is_dropped_when_the_key_disappears(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("POLYGON_API_KEY=safe\nUW_KEY=x\n")
    marker = tmp_path / "home" / ".sawa" / "scheduler" / "env_ignored_UW_KEY"

    first = _scheduler_shell(tmp_path, RECORD_NOTIFY + "setup_env; notify_ignored_env_keys_once")
    assert first.returncode == 0, first.stderr
    assert marker.exists()
    assert len(_notify_calls(tmp_path)) == 1

    (tmp_path / ".env").write_text("POLYGON_API_KEY=safe\n")
    second = _scheduler_shell(tmp_path, RECORD_NOTIFY + "setup_env; notify_ignored_env_keys_once")
    assert second.returncode == 0, second.stderr
    assert not marker.exists()  # re-adding the key later re-alerts
    assert len(_notify_calls(tmp_path)) == 1


def test_dotenv_still_rejects_invalid_variable_names(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("POLYGON_API_KEY=safe\n1BAD=value-must-not-leak\n")

    result = _scheduler_shell(tmp_path, RECORD_NOTIFY + "setup_env")

    assert result.returncode != 0
    assert "invalid environment variable name" in result.stderr
    log = (tmp_path / "home" / ".sawa" / "scheduler" / "scheduler.log").read_text()
    assert "could not safely parse .env: invalid environment variable name" in log
    assert "value-must-not-leak" not in log
    assert "value-must-not-leak" not in result.stderr
    assert _notify_calls(tmp_path) == []


def test_env_example_keys_are_allowlisted() -> None:
    # Every key the repository documents as a .env setting must be accepted by
    # the scheduler, or the next operator to uncomment one reproduces the
    # 2026-09-04 outage class (now a warning, but still a mistake).
    example = (REPO_ROOT / ".env.example").read_text()
    documented = set(re.findall(r"^#?\s*([A-Z][A-Z0-9_]*)=", example, flags=re.MULTILINE))
    script = SCRIPT.read_text()
    allowed_block = script.split("allowed = {", 1)[1].split("}", 1)[0]
    allowed = set(re.findall(r'"([A-Z][A-Z0-9_]*)"', allowed_block))

    assert documented, "no keys parsed from .env.example"
    assert documented <= allowed, sorted(documented - allowed)


# ── EXIT trap (fail loud) ────────────────────────────────────────────────────


def _fail_loud_shell(tmp_path: Path, command: str) -> subprocess.CompletedProcess[str]:
    # Exercise the EXIT trap the way main() installs it, with both delivery
    # paths stubbed so no real notification can be sent from the test suite.
    stubs = (
        'notify_via_sawa() { printf "%s\\n%s\\n" "$1" "$2" >> "$HOME/sawa-alert"; }; '
        'notify_last_resort() { printf "%s\\n" "$2" >> "$HOME/curl-alert"; return 1; }; '
        "set -e; trap 'scheduler_exit_handler $?' EXIT; "
    )
    return _scheduler_shell(tmp_path, stubs + command)


def test_exit_handler_alerts_once_per_day_on_pre_job_failure(tmp_path: Path) -> None:
    (tmp_path / "elsewhere").write_text("POLYGON_API_KEY=safe\n")
    (tmp_path / ".env").symlink_to(tmp_path / "elsewhere")  # setup_env refuses symlinks
    home = tmp_path / "home"

    first = _fail_loud_shell(tmp_path, "SCHEDULER_STAGE=setup_env; setup_env")
    assert first.returncode == 1, first.stderr
    alert = (home / "sawa-alert").read_text()
    assert alert.startswith("Sawa Scheduler FAILED\n")
    assert "during 'setup_env'" in alert
    assert "refusing symlinked .env" in alert
    assert not (home / "curl-alert").exists()
    assert "scheduler exited with status 1 during stage 'setup_env'" in first.stderr
    markers = list((home / ".sawa" / "scheduler").glob("pretick_failure_notified_*"))
    assert len(markers) == 1

    second = _fail_loud_shell(tmp_path, "SCHEDULER_STAGE=setup_env; setup_env")
    assert second.returncode == 1
    assert (home / "sawa-alert").read_text() == alert  # same day: no second push
    assert "already sent today" in second.stderr


def test_exit_handler_retries_delivery_when_no_path_worked(tmp_path: Path) -> None:
    # A failed delivery must not consume the day's one alert.
    (tmp_path / "elsewhere").write_text("POLYGON_API_KEY=safe\n")
    (tmp_path / ".env").symlink_to(tmp_path / "elsewhere")
    home = tmp_path / "home"

    result = _scheduler_shell(
        tmp_path,
        "notify_via_sawa() { return 1; }; notify_last_resort() { return 1; }; "
        "set -e; trap 'scheduler_exit_handler $?' EXIT; setup_env",
    )

    assert result.returncode == 1
    assert "could not deliver the failure notification" in result.stderr
    assert not list((home / ".sawa" / "scheduler").glob("pretick_failure_notified_*"))


def test_exit_handler_is_silent_on_success_and_after_reported_failures(tmp_path: Path) -> None:
    home = tmp_path / "home"

    clean = _fail_loud_shell(tmp_path, "SCHEDULER_STAGE=tick; exit 0")
    assert clean.returncode == 0, clean.stderr
    assert not (home / "sawa-alert").exists()

    reported = _fail_loud_shell(
        tmp_path,
        "notify() { :; }; SCHEDULER_STAGE=tick; "
        'notify_unreported_failure 1 "Sawa Daily FAILED" "body"; exit 1',
    )
    assert reported.returncode == 1
    assert not (home / "sawa-alert").exists()
    assert "already reported" in reported.stderr


def test_exit_handler_alerts_on_unreported_failure_after_the_tick_line(tmp_path: Path) -> None:
    # e.g. start_intraday's "could not verify newly started intraday process"
    # -> return 1 -> set -e exits main after the tick line was logged.
    home = tmp_path / "home"

    result = _fail_loud_shell(
        tmp_path,
        "SCHEDULER_STAGE=tick; log 'ERROR: could not verify newly started intraday process'; exit 1",
    )

    assert result.returncode == 1
    alert = (home / "sawa-alert").read_text()
    assert "during 'tick'" in alert
    assert "could not verify newly started intraday process" in alert


def test_exit_handler_falls_back_to_curl_with_topic_off_argv(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_curl = fake_bin / "curl"
    fake_curl.write_text(
        """#!/bin/bash
printf '%s\\0' "$@" > "$HOME/curl-argv"
cat > "$HOME/curl-config"
"""
    )
    fake_curl.chmod(0o700)
    secret_topic = "ntfy.example.invalid/capability-topic-secret"
    (tmp_path / ".env").write_text(f'NTFY_TOPIC="{secret_topic}"\nPOLYGON_API_KEY=safe\n')

    result = _scheduler_shell(
        tmp_path,
        'PATH="$PROJECT_DIR/bin:$PATH"; export PATH; '
        "notify_via_sawa() { return 1; }; "
        "set -e; trap 'scheduler_exit_handler $?' EXIT; "
        "SCHEDULER_STAGE=initialize_scheduler; log 'ERROR: flock failed (rc=127)'; exit 3",
    )

    assert result.returncode == 3, result.stderr
    home = tmp_path / "home"
    argv = (home / "curl-argv").read_bytes()
    assert b"capability-topic-secret" not in argv
    assert argv.split(b"\0")[:-1] == [b"-fsS", b"-m", b"15", b"-o", b"/dev/null", b"-K", b"-"]
    config = (home / "curl-config").read_text()
    assert f'url = "https://{secret_topic}"' in config
    assert 'header = "Priority: 5"' in config
    assert "flock failed (rc=127)" in config
    assert "direct ntfy POST" in result.stderr


def test_exit_handler_does_not_write_through_a_refused_state_symlink(tmp_path: Path) -> None:
    # initialize_scheduler refuses a symlink inside the state directory; the
    # trap must then alert without touching the directory (the planted
    # scheduler.log symlink points at a file we must not append to).
    home = tmp_path / "home"
    state = home / ".sawa" / "scheduler"
    state.mkdir(parents=True)
    target = tmp_path / "sentinel"
    target.write_text("keep-me")
    (state / "scheduler.log").symlink_to(target)
    env = os.environ.copy()
    env["HOME"] = str(home)
    env["TMPDIR"] = str(tmp_path)

    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; '
            'notify_via_sawa() { printf "%s\\n" "$2" >> "$HOME/sawa-alert"; }; '
            "notify_last_resort() { return 1; }; "
            "set -e; trap 'scheduler_exit_handler $?' EXIT; "
            "SCHEDULER_STAGE=initialize_scheduler; initialize_scheduler",
            "scheduler-test",
            str(SCRIPT),
        ],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode != 0
    assert "containing symlinks" in result.stderr
    assert target.read_text() == "keep-me"
    assert (home / "sawa-alert").read_text().count("Sawa Scheduler FAILED") == 0
    assert "during 'initialize_scheduler'" in (home / "sawa-alert").read_text()
    assert sorted(p.name for p in state.iterdir()) == ["scheduler.log"]
    # The once-per-day marker went to the private TMPDIR fallback instead.
    fallback = tmp_path / f"sawa-scheduler-alerts-{os.getuid()}"
    assert list(fallback.glob("pretick_failure_notified_*"))


def test_flock_error_is_not_reported_as_contention(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["HOME"] = str(tmp_path / "home")
    broken = subprocess.run(
        ["bash", "-c", 'source "$1"; flock() { return 127; }; initialize_scheduler',
         "scheduler-test", str(SCRIPT)],
        check=False, capture_output=True, env=env, text=True,
    )
    assert broken.returncode != 0
    assert "flock failed (rc=127)" in broken.stderr

    busy = subprocess.run(
        ["bash", "-c", 'source "$1"; flock() { return 1; }; initialize_scheduler; echo REACHED',
         "scheduler-test", str(SCRIPT)],
        check=False, capture_output=True, env=env, text=True,
    )
    assert busy.returncode == 0
    assert "REACHED" not in busy.stdout
    assert "already running" in busy.stderr
    log = (tmp_path / "home" / ".sawa" / "scheduler" / "scheduler.log").read_text()
    assert "already running" in log


def test_cron_log_is_made_private_but_not_truncated(tmp_path: Path) -> None:
    state = tmp_path / "home" / ".sawa" / "scheduler"
    state.mkdir(parents=True)
    cron_log = state / "cron.log"
    cron_log.write_text("".join(f"line {i}\n" for i in range(12000)))
    cron_log.chmod(0o664)

    result = _scheduler_shell(tmp_path, "true")

    assert result.returncode == 0, result.stderr
    assert cron_log.stat().st_mode & 0o777 == 0o600
    assert len(cron_log.read_text().splitlines()) == 12000


# ── Whole-script runs (cron's view) ──────────────────────────────────────────


def _fake_project(tmp_path: Path) -> Path:
    """A copy of the scheduler in a throwaway project whose venv holds a fake `sawa`."""
    (tmp_path / "scripts").mkdir()
    shutil.copy(SCRIPT, tmp_path / "scripts" / "market_scheduler.sh")
    venv_bin = tmp_path / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    (venv_bin / "activate").write_text(
        'PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd):$PATH"; export PATH\n'
    )
    fake_sawa = venv_bin / "sawa"
    fake_sawa.write_text('#!/bin/bash\nprintf "%s\\0" "$@" >> "$HOME/sawa-argv"\n')
    fake_sawa.chmod(0o700)
    # Real interpreter (python-dotenv) for the .env parse; the market-status
    # probe is answered offline with a closed market so the run never talks to
    # the provider and does not depend on the wall clock.
    fake_python = venv_bin / "python"
    fake_python.write_text(
        "#!/bin/bash\n"
        'if [ -n "${SAWA_MARKET_STATUS_API_KEY:-}" ]; then '
        "printf '%s' '{\"exchanges\":{\"nyse\":\"closed\"}}'; exit 0; fi\n"
        f'exec "{REPO_ROOT / ".venv" / "bin" / "python"}" "$@"\n'
    )
    fake_python.chmod(0o700)
    (tmp_path / "home").mkdir()
    return tmp_path / "scripts" / "market_scheduler.sh"


def _run_main(tmp_path: Path, script: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["HOME"] = str(tmp_path / "home")
    env["TMPDIR"] = str(tmp_path)
    return subprocess.run(
        ["bash", str(script)], check=False, capture_output=True, env=env, text=True
    )


def _sawa_argv(tmp_path: Path) -> list[bytes]:
    path = tmp_path / "home" / "sawa-argv"
    return path.read_bytes().split(b"\0") if path.exists() else []


def test_main_completes_a_tick_with_an_unknown_env_key(tmp_path: Path) -> None:
    # The exact 2026-09-04 trigger: a harmless extra key in .env.
    script = _fake_project(tmp_path)
    (tmp_path / ".env").write_text("POLYGON_API_KEY=safe\nUW_KEY=extra\n")

    first = _run_main(tmp_path, script)
    second = _run_main(tmp_path, script)

    assert first.returncode == 0, first.stderr
    assert "could not safely parse .env" not in first.stderr
    assert "ignoring .env key that is not in the scheduler allowlist: UW_KEY" in first.stderr
    assert "Scheduler tick — market: closed" in first.stderr
    assert "Sawa Scheduler FAILED" not in first.stderr
    assert second.returncode == 0, second.stderr
    argv = _sawa_argv(tmp_path)
    assert argv.count(b"Sawa Scheduler: ignoring .env key(s)") == 1  # once, not per tick
    assert b"extra" not in argv  # the ignored value never reaches a job


def test_main_alerts_once_per_day_when_it_aborts_before_the_tick(tmp_path: Path) -> None:
    script = _fake_project(tmp_path)
    (tmp_path / "real.env").write_text("POLYGON_API_KEY=safe\n")
    (tmp_path / ".env").symlink_to(tmp_path / "real.env")  # setup_env refuses symlinks

    first = _run_main(tmp_path, script)
    second = _run_main(tmp_path, script)

    assert first.returncode == 1, first.stderr
    assert "refusing symlinked .env" in first.stderr
    assert "scheduler exited with status 1 during stage 'setup_env'" in first.stderr
    argv = _sawa_argv(tmp_path)
    assert argv[:2] == [b"notify", b"--title"]
    assert b"Sawa Scheduler FAILED" in argv
    assert b"error" in argv
    assert argv.count(b"notify") == 1
    assert second.returncode == 1
    assert "already sent today" in second.stderr
    assert _sawa_argv(tmp_path).count(b"notify") == 1
    assert list((tmp_path / "home" / ".sawa" / "scheduler").glob("pretick_failure_notified_*"))


def test_main_lock_contention_still_exits_quietly(tmp_path: Path) -> None:
    script = _fake_project(tmp_path)
    (tmp_path / ".env").write_text("POLYGON_API_KEY=safe\n")
    state = tmp_path / "home" / ".sawa" / "scheduler"
    state.mkdir(parents=True)
    with open(state / "scheduler.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _run_main(tmp_path, script)

    assert result.returncode == 0, result.stderr
    assert "Another scheduler is already running" in result.stderr
    assert not (tmp_path / "home" / "sawa-argv").exists()


# ── Job retry cap and daily summary ──────────────────────────────────────────


def test_daily_retries_are_capped_per_date(tmp_path: Path) -> None:
    stubs = (
        RECORD_NOTIFY
        + "heartbeat() { :; }; run_doctor() { return 1; }; "
        'sawa() { printf "%s\\n" "$1" >> "$HOME/sawa-runs"; return 0; }; '
    )
    (tmp_path / ".env").write_text("POLYGON_API_KEY=safe\n")

    result = _scheduler_shell(
        tmp_path,
        stubs + "for i in 1 2 3 4 5; do run_daily || true; done; "
        'cat "$STATE_DIR"/daily_attempts_*',
    )

    assert result.returncode == 0, result.stderr
    runs = (tmp_path / "home" / "sawa-runs").read_text().split()
    assert runs == ["daily"] * 3  # attempts 4 and 5 never start the job
    assert result.stdout.strip() == "3"
    assert result.stderr.count("giving up for") == 2
    calls = _notify_calls(tmp_path)
    assert [c.split("|")[1] for c in calls] == ["Sawa Daily: last retry"]


def test_daily_summary_reads_the_inserted_count_sawa_actually_logs(tmp_path: Path) -> None:
    stubs = (
        "notify() { :; }; heartbeat() { :; }; run_doctor() { :; }; "
        'build_daily_summary() { printf "inserted=%s" "$1"; }; '
        "sawa() { printf '%s\\n' '  Inserted 5000/120581 records' '  Inserted 120581 records'; }; "
    )
    (tmp_path / ".env").write_text("POLYGON_API_KEY=safe\n")

    result = _scheduler_shell(tmp_path, stubs + "run_daily")

    assert result.returncode == 0, result.stderr
    assert "Daily completed: inserted=120581" in result.stderr


def test_market_status_treats_polygon_after_hours_states_as_closed(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python"
    fake_python.write_text(
        '#!/bin/bash\nprintf \'%s\' \'{"exchanges":{"nyse":"extended-hours"}}\'\n'
    )
    fake_python.chmod(0o700)
    (tmp_path / ".env").write_text("POLYGON_API_KEY=safe\n")

    result = _scheduler_shell(
        tmp_path,
        'setup_env; PATH="$PROJECT_DIR/bin:$PATH"; export PATH; check_market_status',
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "closed"
    assert "unreachable" not in result.stderr
    assert "extended-hours (treated as closed)" in result.stderr


# ── Existing hardening tests (unchanged) ─────────────────────────────────────


def test_scheduler_rejects_preexisting_state_symlink(tmp_path: Path) -> None:
    # This test invokes initialization directly because _scheduler_shell would
    # create the state directory before we can plant the hostile entry.
    home = tmp_path / "home"
    state = home / ".sawa" / "scheduler"
    state.mkdir(parents=True)
    target = tmp_path / "sentinel"
    target.write_text("keep-me")
    (state / "scheduler.log").symlink_to(target)
    env = os.environ.copy()
    env["HOME"] = str(home)

    result = subprocess.run(
        ["bash", "-c", 'source "$1"; initialize_scheduler', "scheduler-test", str(SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode != 0
    assert "containing symlinks" in result.stderr
    assert target.read_text() == "keep-me"


def test_stop_refuses_live_unrelated_pid(tmp_path: Path) -> None:
    command = r'''
        log() { :; }
        notify() { :; }
        sleep 30 & victim=$!
        token=$(process_start_token "$victim")
        printf '%s %s\n' "$victim" "$token" > "$STATE_DIR/intraday.pid"
        stop_intraday
        kill -0 "$victim"
        result=$?
        kill "$victim" 2>/dev/null || true
        wait "$victim" 2>/dev/null || true
        exit "$result"
    '''

    result = _scheduler_shell(tmp_path, command)

    assert result.returncode == 0, result.stderr


def test_stop_rejects_non_positive_pid_without_signaling(tmp_path: Path) -> None:
    command = r'''
        log() { :; }
        notify() { :; }
        printf '%s\n' '-1 123' > "$STATE_DIR/intraday.pid"
        stop_intraday
        test ! -e "$STATE_DIR/intraday.pid"
    '''

    result = _scheduler_shell(tmp_path, command)

    assert result.returncode == 0, result.stderr


def test_database_url_is_passed_to_python_via_environment_not_argv(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python"
    fake_python.write_text(
        """#!/bin/bash
printf '%s\\0' "$@" > "$HOME/python-argv"
printf '%s' "$DATABASE_URL" > "$HOME/python-database-url"
printf '%s\\n' '2026-08-28'
"""
    )
    fake_python.chmod(0o700)
    secret_url = "postgresql://reader:argv-secret@db.invalid/sawa"
    (tmp_path / ".env").write_text(f"DATABASE_URL={secret_url}\n")

    result = _scheduler_shell(
        tmp_path,
        'setup_env; PATH="$PROJECT_DIR/bin:$PATH"; export PATH; '
        'build_daily_summary "1,234"',
    )

    assert result.returncode == 0, result.stderr
    assert "Latest prices: 2026-08-28" in result.stdout
    assert "Prices inserted: 1,234" in result.stdout
    home = tmp_path / "home"
    argv = (home / "python-argv").read_bytes().split(b"\0")
    rendered_argv = b"\0".join(argv)
    assert secret_url.encode() not in rendered_argv
    assert b"argv-secret" not in rendered_argv
    assert (home / "python-database-url").read_text() == secret_url
    assert argv[:-1] == [b"-"]


def test_scheduler_never_places_database_url_in_command_arguments() -> None:
    script = SCRIPT.read_text()

    assert 'psql "$DATABASE_URL"' not in script
    assert 'python - "$DATABASE_URL"' not in script


def test_scheduler_http_secrets_use_child_environment_not_argv(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python"
    fake_python.write_text(
        """#!/bin/bash
if [ -n "${SAWA_HEARTBEAT_REQUEST_URL:-}" ]; then
    printf '%s\\0' "$@" > "$HOME/heartbeat-argv"
    printf '%s' "$SAWA_HEARTBEAT_REQUEST_URL" > "$HOME/heartbeat-url"
    printf '%s' "$SAWA_HEARTBEAT_REQUEST_SUFFIX" > "$HOME/heartbeat-suffix"
elif [ -n "${SAWA_MARKET_STATUS_API_KEY:-}" ]; then
    printf '%s\\0' "$@" > "$HOME/market-status-argv"
    printf '%s' "$SAWA_MARKET_STATUS_API_KEY" > "$HOME/market-status-key"
    printf '%s' '{"exchanges":{"nyse":"open"}}'
fi
"""
    )
    fake_python.chmod(0o700)
    heartbeat_secret = "https://hc.example.invalid/capability-uuid-secret"
    polygon_secret = "polygon-argv-secret"
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                f"SAWA_HEARTBEAT_URL={heartbeat_secret}",
                f"POLYGON_API_KEY={polygon_secret}",
            ]
        )
        + "\n"
    )

    result = _scheduler_shell(
        tmp_path,
        'setup_env; PATH="$PROJECT_DIR/bin:$PATH"; export PATH; '
        'heartbeat "$SAWA_HEARTBEAT_URL" /fail; check_market_status',
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "open"
    home = tmp_path / "home"
    heartbeat_argv = (home / "heartbeat-argv").read_bytes()
    market_argv = (home / "market-status-argv").read_bytes()
    assert heartbeat_secret.encode() not in heartbeat_argv
    assert polygon_secret.encode() not in market_argv
    assert heartbeat_argv.split(b"\0")[:-1] == [b"-"]
    assert market_argv.split(b"\0")[:-1] == [b"-"]
    assert (home / "heartbeat-url").read_text() == heartbeat_secret
    assert (home / "heartbeat-suffix").read_text() == "/fail"
    assert (home / "market-status-key").read_text() == polygon_secret


def test_scheduler_http_helpers_are_bounded_and_have_no_secret_curl_argv() -> None:
    script = SCRIPT.read_text()

    assert '"${url}${suffix}"' not in script
    assert 'Authorization: Bearer $POLYGON_API_KEY' not in script
    assert "MAX_RESPONSE_BYTES = 64 * 1024" in script
    assert "response.read(MAX_RESPONSE_BYTES + 1)" in script
    assert "signal.setitimer(signal.ITIMER_REAL, 10)" in script
    assert "signal.setitimer(signal.ITIMER_REAL, 5)" in script
    assert script.count("build_opener(NoRedirect).open") == 2


def test_heartbeat_does_not_follow_redirects() -> None:
    script = SCRIPT.read_text()
    heartbeat_block = script.split("heartbeat() {", 1)[1].split(
        "# ── Environment setup", 1
    )[0]

    assert "class NoRedirect(HTTPRedirectHandler):" in heartbeat_block
    assert "build_opener(NoRedirect).open" in heartbeat_block
    assert "urlopen(" not in heartbeat_block


def test_heartbeat_rejects_plain_http_without_request(tmp_path: Path) -> None:
    result = _scheduler_shell(
        tmp_path,
        'heartbeat "http://127.0.0.1:9/capability-secret"',
    )

    assert result.returncode == 0
    assert "heartbeat ping failed" in result.stderr
