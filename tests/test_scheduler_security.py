"""The scheduler must treat dotenv/state paths as untrusted input."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "market_scheduler.sh"


def _scheduler_shell(tmp_path: Path, command: str) -> subprocess.CompletedProcess[str]:
    # Let setup_env activate the repository's test environment without copying
    # credentials or installing anything in the temporary project.
    (tmp_path / ".venv").symlink_to(Path(__file__).resolve().parents[1] / ".venv")
    env = os.environ.copy()
    env["HOME"] = str(tmp_path / "home")
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

    result = _scheduler_shell(tmp_path, "setup_env")

    assert result.returncode != 0
    assert "not allowed" in result.stderr


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
