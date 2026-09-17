#!/bin/bash
# Market Hours Scheduler
# Runs via cron every 15 minutes. Manages sawa intraday streaming during market
# hours, runs sawa daily after market close, and runs sawa weekly once per ISO
# week on the first eligible closed-market evening.
#
# Crontab entry (install manually). Run every day (0-6, includes Sunday) so a
# missed Saturday weekly tick can recover on Sunday; the per-week/per-day done
# flags keep it idempotent:
#   */15 * * * * /home/seed/code/sawa/scripts/market_scheduler.sh >> ~/.sawa/scheduler/cron.log 2>&1
#
# State directory: ~/.sawa/scheduler/

set -euo pipefail
umask 077

# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STATE_DIR="$HOME/.sawa/scheduler"
LOG_FILE="$STATE_DIR/scheduler.log"
# NTFY_TOPIC is read by `sawa notify` (Python notifier abstraction). Source
# .env in setup_env to make it available to the child process.
DAILY_WAIT_HOURS=1  # hours after close before running daily
INTRADAY_STOP_TIMEOUT=60  # seconds to wait for graceful shutdown
# A daily/weekly that fails (job or doctor) is retried on every closed-market
# evening tick; without a cap an unfixable doctor FAIL re-ran the 75-minute
# daily back-to-back until midnight (2026-08-31: 7 attempts). Per-date cap.
MAX_JOB_ATTEMPTS=3

# Fail-loud bookkeeping (see "Fail loud" section). STATE_DIR_OK is set once
# initialize_scheduler has verified the state directory, so nothing writes
# through a refused symlink; SCHEDULER_STAGE names the phase a non-zero exit
# happened in; SCHEDULER_ALERTED records that a failure alert (or a deliberate
# "the job reported itself" skip) already went out this tick.
STATE_DIR_OK=false
SCHEDULER_STAGE="startup"
SCHEDULER_ALERTED=false
SCHEDULER_LAST_ERROR=""
SCHEDULER_IGNORED_ENV_KEYS=""

# ── Lock (prevent overlapping runs) ──────────────────────────────────────────

LOCK_FILE="$STATE_DIR/scheduler.lock"

acquire_lock() {
    exec 9>"$LOCK_FILE"
    chmod 600 "$LOCK_FILE"
    # `flock -n` exits 1 on contention; anything else (127 not installed,
    # EBADF/ENOLCK) is an error that must not masquerade as "already running".
    local rc=0
    flock -n 9 || rc=$?
    case "$rc" in
        0) ;;
        1)
            log "Another scheduler is already running, skipping"
            exit 0
            ;;
        *)
            log "ERROR: flock failed (rc=$rc) on $LOCK_FILE"
            return 1
            ;;
    esac
    # Write PID for debugging
    echo $$ >&9
}

initialize_scheduler() {
    if [ -L "$STATE_DIR" ]; then
        echo "Refusing symlinked scheduler state directory: $STATE_DIR" >&2
        return 1
    fi
    mkdir -p "$STATE_DIR"
    chmod 700 "$STATE_DIR"
    if find "$STATE_DIR" -mindepth 1 -maxdepth 1 -type l -print -quit | grep -q .; then
        echo "Refusing scheduler state directory containing symlinks: $STATE_DIR" >&2
        return 1
    fi
    touch "$LOG_FILE"
    chmod 600 "$LOG_FILE"
    # From here on log() may append to scheduler.log and marker files may be
    # written: the directory is a real, private, symlink-free directory.
    STATE_DIR_OK=true
    # cron appends this script's stdout/stderr to cron.log (crontab entry at
    # the top of this file); the shell creates it with the default umask, so
    # keep it as private as scheduler.log. It is not rotated here on purpose:
    # it is the only long-term record of early aborts (see "Fail loud").
    [ -f "$STATE_DIR/cron.log" ] && chmod 600 "$STATE_DIR/cron.log"
    acquire_lock

    # Trim log to last 5000 lines periodically.
    if [ -f "$LOG_FILE" ] && [ "$(wc -l < "$LOG_FILE")" -gt 10000 ]; then
        tail -n 5000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
    fi
}

# ── Logging ──────────────────────────────────────────────────────────────────

log() {
    local ts
    ts=$(TZ=America/New_York date '+%Y-%m-%d %H:%M:%S ET')
    local msg="[$ts] $*"
    case "$*" in
        ERROR*) SCHEDULER_LAST_ERROR="$*" ;;
    esac
    # Before initialize_scheduler has verified the state directory the log
    # path may be a planted symlink; stderr (cron.log) is the only safe sink.
    if [ "$STATE_DIR_OK" = true ]; then
        echo "$msg" >> "$LOG_FILE"
    fi
    echo "$msg" >&2
}

# ── Notifications ────────────────────────────────────────────────────────────
#
# Delegates to `sawa notify`, which uses the same Notifier abstraction as
# the Python run wrappers. Backend (ntfy, etc.) is selected by the
# SAWA_NOTIFIER / NTFY_TOPIC env vars sourced from .env in setup_env.

notify() {
    local title="$1"
    local body="$2"
    local level="${3:-info}"
    log "Sending notification ($level): $title"
    if [ "$level" = error ]; then
        SCHEDULER_ALERTED=true
    fi
    if ! sawa notify \
            --title "$title" \
            --body "$body" \
            --level "$level" \
            --tag chart_with_upwards_trend \
            --tag scheduler \
            >> "$LOG_FILE" 2>&1; then
        log "WARN: sawa notify failed"
    fi
}

# Every sawa job already sends its own detailed failure notification through
# monitored_run (check counts, elapsed time, the degraded reasons). Alerting
# again here on the same event doubled every failure on the operator's phone.
# Only alert for terminations the job could NOT have reported itself: killed by
# a signal (128+N), or the shell failing to execute it at all (126/127).
notify_unreported_failure() {
    local exit_code="$1"
    local title="$2"
    local body="$3"
    SCHEDULER_ALERTED=true
    if [ "$exit_code" -ge 126 ]; then
        notify "$title" "$body" error
    else
        log "Skipping duplicate alert; the job reported exit $exit_code itself"
    fi
}

run_doctor() {
    local job="$1"
    local exit_code=0

    log "Starting sawa doctor --job $job..."
    sawa doctor --job "$job" --log-dir "$PROJECT_DIR/logs" \
        >> "$LOG_FILE" 2>&1 || exit_code=$?

    if [ "$exit_code" -ne 0 ]; then
        log "ERROR: sawa doctor --job $job failed (exit $exit_code)"
        notify_unreported_failure "$exit_code" "Sawa Doctor FAILED" \
            "doctor --job $job exited with code $exit_code"
        return 1
    fi

    log "Doctor passed for $job"
}

# ── Heartbeat (dead-man's-switch) ─────────────────────────────────────────────
#
# Pings an external monitor (e.g. healthchecks.io) so that if the host, cron,
# or notifier is down — meaning no Sawa notification can be delivered at all —
# the *absence* of a ping raises an alert there. Configured via
# SAWA_HEARTBEAT_URL (daily) and SAWA_WEEKLY_HEARTBEAT_URL (weekly), sourced
# from .env; a no-op when the relevant URL is unset.
heartbeat() {
    local url="$1" suffix="${2:-}"  # suffix: "" on success, "/fail" on failure
    [ -z "$url" ] && return 0
    # The capability URL is passed only in the child's environment. Putting it
    # in curl argv exposes the token to `ps` for the duration of the request.
    if ! SAWA_HEARTBEAT_REQUEST_URL="$url" \
        SAWA_HEARTBEAT_REQUEST_SUFFIX="$suffix" \
        python - >/dev/null 2>&1 <<'PY'
import os
import signal
from urllib.parse import urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class NoRedirect(HTTPRedirectHandler):
    """Keep a validated HTTPS capability URL from redirecting elsewhere."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def deadline_expired(_signum, _frame):
    raise TimeoutError("heartbeat request exceeded overall deadline")


raw_url = os.environ["SAWA_HEARTBEAT_REQUEST_URL"]
suffix = os.environ.get("SAWA_HEARTBEAT_REQUEST_SUFFIX", "")
if suffix not in {"", "/fail"}:
    raise SystemExit("invalid heartbeat suffix")
parts = urlsplit(raw_url)
if parts.scheme != "https" or not parts.netloc:
    raise SystemExit("heartbeat URL must use HTTPS")
if parts.username is not None or parts.password is not None:
    raise SystemExit("heartbeat URL must not contain userinfo")
if suffix:
    path = parts.path.rstrip("/") + suffix
    request_url = urlunsplit((parts.scheme, parts.netloc, path, parts.query, ""))
else:
    request_url = urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
request = Request(request_url, method="GET")
signal.signal(signal.SIGALRM, deadline_expired)
signal.setitimer(signal.ITIMER_REAL, 10)
try:
    with build_opener(NoRedirect).open(request, timeout=10) as response:
        response.read(1)
finally:
    signal.setitimer(signal.ITIMER_REAL, 0)
PY
    then
        # Heartbeat URLs commonly contain an embedded secret UUID/token.
        log "WARN: heartbeat ping failed"
    fi
}

# ── Fail loud ────────────────────────────────────────────────────────────────
#
# Everything that runs before the "Scheduler tick" line (state directory,
# lock, .env parsing, venv/python availability) used to fail with one stderr
# line and no push: under `set -e` main() exited before any notify() or
# heartbeat call was reachable. Incident 2026-09-04..09-15: one unknown .env
# key (UW_KEY) silenced intraday stop, daily, weekly and doctor for 11 days.
#
# main() installs scheduler_exit_handler as its EXIT trap. Any non-zero exit
# that nothing has reported (SCHEDULER_ALERTED=false) produces one push per
# ET day: through `sawa notify` when the venv is usable (it loads .env itself,
# so it works even when setup_env did not get that far), else through a
# direct ntfy POST that needs only bash, curl and the NTFY_TOPIC line of .env.
# The topic is a capability and never enters argv.

# Where once-per-day alert markers live. STATE_DIR when it has been verified,
# else a private per-user directory under TMPDIR so a broken state directory
# still produces one alert per day rather than one per tick.
alert_marker_dir() {
    if [ "$STATE_DIR_OK" = true ]; then
        printf '%s\n' "$STATE_DIR"
        return 0
    fi
    local dir="${TMPDIR:-/tmp}/sawa-scheduler-alerts-$(id -u)"
    mkdir -p -m 700 "$dir" 2>/dev/null || return 1
    [ ! -L "$dir" ] && [ -O "$dir" ] || return 1
    printf '%s\n' "$dir"
}

# Append helper output to scheduler.log when it is safe to write there,
# otherwise to stderr (cron.log). Used by the paths the EXIT trap reaches.
run_logged() {
    if [ "$STATE_DIR_OK" = true ]; then
        "$@" >> "$LOG_FILE" 2>&1
    else
        "$@" >&2
    fi
}

notify_via_sawa() {
    local title="$1" body="$2" sawa_bin=""
    if command -v sawa >/dev/null 2>&1; then
        sawa_bin=sawa
    elif [ -x "$PROJECT_DIR/.venv/bin/sawa" ]; then
        sawa_bin="$PROJECT_DIR/.venv/bin/sawa"
    else
        return 1
    fi
    # `sawa notify` loads .env itself (python-dotenv) and exits 1 when no
    # backend is configured or the send failed, so the caller can fall through.
    run_logged "$sawa_bin" notify \
        --title "$title" \
        --body "$body" \
        --level error \
        --tag rotating_light \
        --tag scheduler
}

notify_last_resort() {
    local title="$1" body="$2" env_file="$PROJECT_DIR/.env" topic url
    [ -f "$env_file" ] && [ ! -L "$env_file" ] || return 1
    command -v curl >/dev/null 2>&1 || return 1
    # Read ONLY the NTFY_TOPIC line, as data (no source, no eval). Accept the
    # same spellings NtfyNotifier does: bare topic, host/topic, or https URL.
    topic=$(sed -n 's/^[[:space:]]*\(export[[:space:]]\{1,\}\)\{0,1\}NTFY_TOPIC[[:space:]]*=[[:space:]]*//p' "$env_file" \
        | head -n 1 \
        | sed -e 's/[[:space:]]\{1,\}#.*$//' -e 's/[[:space:]]*$//' \
              -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'\$/\1/")
    [ -n "$topic" ] || return 1
    case "$topic" in
        https://*) url="$topic" ;;
        http://*)  return 1 ;;
        */*)       url="https://$topic" ;;
        *)         url="https://ntfy.sh/$topic" ;;
    esac
    # Title/body are our own text; strip the two characters curl's config
    # syntax treats specially and fold to one line.
    title=$(printf '%s' "$title" | tr -d '"\\' | tr '\n' ' ')
    body=$(printf '%s' "$body" | tr -d '"\\' | tr '\n' ' ')
    # Feed the capability URL to curl through a config on stdin (-K -), so it
    # never appears in argv / `ps`.
    printf 'url = "%s"\nheader = "Title: %s"\nheader = "Priority: 5"\nheader = "Tags: rotating_light,scheduler"\ndata = "%s"\n' \
        "$url" "$title" "$body" \
        | curl -fsS -m 15 -o /dev/null -K -
}

scheduler_exit_handler() {
    local status="$1"
    set +e
    trap - EXIT
    [ "$status" -eq 0 ] && return 0
    if [ "$SCHEDULER_ALERTED" = true ]; then
        log "Tick ended with exit $status (stage: $SCHEDULER_STAGE); failure was already reported"
        return 0
    fi
    # Only the first line of the last ERROR, bounded, goes into the push.
    local last_error="${SCHEDULER_LAST_ERROR%%$'\n'*}"
    last_error="${last_error:0:240}"
    log "ERROR: scheduler exited with status $status during stage '$SCHEDULER_STAGE' before any alert was sent"

    local marker_dir marker today
    today=$(TZ=America/New_York date '+%Y-%m-%d')
    marker_dir=$(alert_marker_dir) || marker_dir=""
    marker="${marker_dir:+$marker_dir/}pretick_failure_notified_$today"
    if [ -n "$marker_dir" ] && [ -e "$marker" ]; then
        log "A scheduler-failure alert was already sent today; suppressing this one"
        return 0
    fi

    local title="Sawa Scheduler FAILED" body
    body="market_scheduler.sh on $(hostname) exited $status during '$SCHEDULER_STAGE' before running any job."
    if [ -n "$last_error" ]; then
        body="$body Last error: $last_error."
    fi
    body="$body Every 15-min tick will keep failing until this is fixed (no intraday stop, daily, weekly or doctor). Further alerts suppressed until tomorrow. See ~/.sawa/scheduler/cron.log"

    if notify_via_sawa "$title" "$body"; then
        log "Failure notification sent via sawa notify"
    elif notify_last_resort "$title" "$body"; then
        log "Failure notification sent via direct ntfy POST"
    else
        log "ERROR: could not deliver the failure notification by any path"
        return 0
    fi
    # Only a delivered alert is rate-limited; a failed delivery is retried
    # on the next tick.
    if [ -n "$marker_dir" ]; then
        touch "$marker" 2>/dev/null || true
        find "$marker_dir" -maxdepth 1 -name 'pretick_failure_notified_*' -mtime +7 -delete 2>/dev/null || true
    fi
}

# Push ONE warning per .env key that setup_env ignored (marker file per key),
# dropping markers for keys that disappeared so re-adding a key re-alerts.
# Called from main(), never from setup_env, so a test or an interactive
# `source market_scheduler.sh; setup_env` can never reach the real
# `sawa notify` (which loads the repository .env and pushes to the operator).
notify_ignored_env_keys_once() {
    local key marker new_keys="" current=" $SCHEDULER_IGNORED_ENV_KEYS "
    # Key names passed setup_env's identifier check, so word splitting is safe.
    for key in $SCHEDULER_IGNORED_ENV_KEYS; do
        marker="$STATE_DIR/env_ignored_$key"
        [ -e "$marker" ] && continue
        touch "$marker"
        new_keys="$new_keys $key"
    done
    for marker in "$STATE_DIR"/env_ignored_*; do
        [ -e "$marker" ] || continue
        key=${marker##*/env_ignored_}
        case "$current" in
            *" $key "*) ;;
            *) rm -f "$marker" ;;
        esac
    done
    [ -z "$new_keys" ] && return 0
    notify "Sawa Scheduler: ignoring .env key(s)" \
        "Not in the scheduler allowlist, so never exported to jobs:$new_keys. Remove from .env or add to the allowlist in scripts/market_scheduler.sh (setup_env)." \
        warning
}

# ── Environment setup ────────────────────────────────────────────────────────

setup_env() {
    cd "$PROJECT_DIR"

    # Activate the virtualenv before parsing .env so python-dotenv is available.
    if [ -f .venv/bin/activate ]; then
        # shellcheck disable=SC1091
        source .venv/bin/activate
    fi

    # Parse .env as data. Never `source` it: dotenv values are not trusted shell
    # syntax, and sourcing turns a writable configuration file into code.
    if [ -f .env ]; then
        if [ -L .env ]; then
            log "ERROR: refusing symlinked .env"
            return 1
        fi
        # The allowlist is a FILTER: keys outside it are never exported into
        # this shell or any child, but they no longer abort the tick — a
        # benign extra key in .env is configuration, not an attack. A short
        # denylist of process-control names still aborts (loudly, via the EXIT
        # trap) because their presence means someone is trying to inject code.
        local dotenv_exports parse_err reason
        parse_err=$(mktemp 2>/dev/null || echo /dev/null)
        if ! dotenv_exports=$(python - "$PROJECT_DIR/.env" 2>"$parse_err" <<'PY'
import os
import re
import shlex
import sys

from dotenv import dotenv_values

path = sys.argv[1]
# Names that let a writable .env inject code into this shell or its children.
denied = {
    "BASH_ENV",
    "ENV",
    "HOME",
    "IFS",
    "LD_LIBRARY_PATH",
    "LD_PRELOAD",
    "PATH",
    "PROMPT_COMMAND",
    "PS4",
    "PYTHONHOME",
    "PYTHONPATH",
    "PYTHONSTARTUP",
    "SHELLOPTS",
}
allowed = {
    "CACHE_ENABLED",
    "CACHE_TTL_SECONDS",
    "DATABASE_URL",
    "DEFAULT_COMPANY_PROVIDER",
    "DEFAULT_ECONOMY_PROVIDER",
    "DEFAULT_FUNDAMENTAL_PROVIDER",
    "DEFAULT_PRICE_PROVIDER",
    "DEFAULT_RATIOS_PROVIDER",
    "FRED_API_KEY",
    "INTRADAY_RETENTION_DAYS",
    "MASSIVE_API_KEY",
    "NTFY_TOPIC",
    "PGDATABASE",
    "PGHOST",
    "PGPASSWORD",
    "PGPORT",
    "PGUSER",
    "POLYGON_API_KEY",
    "POLYGON_S3_ACCESS_KEY",
    "POLYGON_S3_SECRET_KEY",
    "SAWA_HEARTBEAT_URL",
    "SAWA_NOTIFIER",
    "SAWA_TICK_HEARTBEAT_URL",
    "SAWA_WATCHDOG_HEARTBEAT_URL",
    "SAWA_WEEKLY_HEARTBEAT_URL",
}
flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
fd = os.open(path, flags)
os.fchmod(fd, 0o600)
with os.fdopen(fd, encoding="utf-8") as stream:
    values = dotenv_values(stream=stream)
ignored = []
for key, value in values.items():
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        raise SystemExit(f"invalid environment variable name: {key!r}")
    if key in denied:
        raise SystemExit(f"process-control variable is not allowed in scheduler .env: {key}")
    if key not in allowed:
        # Never exported, never aborts; reported by name (never value).
        ignored.append(key)
        continue
    if value is not None:
        print(f"export {key}={shlex.quote(value)}")
# Names passed the identifier check above, so this line is safe to eval.
print(f"SCHEDULER_IGNORED_ENV_KEYS={shlex.quote(' '.join(sorted(ignored)))}")
PY
        ); then
            # The parser's stderr carries key names, line numbers and the
            # exception text (never values); its last line is the reason.
            # bash prefixes its own errors ("python: command not found") with
            # this script's absolute path; drop it, the path is not the news.
            reason=$(tail -n 1 "$parse_err" 2>/dev/null | sed 's|^[^ ]*market_scheduler\.sh: ||' | head -c 300)
            rm -f "$parse_err"
            log "ERROR: could not safely parse .env${reason:+: $reason}"
            return 1
        fi
        rm -f "$parse_err"
        SCHEDULER_IGNORED_ENV_KEYS=""
        eval "$dotenv_exports"
        unset dotenv_exports
        local key
        for key in $SCHEDULER_IGNORED_ENV_KEYS; do
            log "WARN: ignoring .env key that is not in the scheduler allowlist: $key"
        done
    fi

    # The scheduler emits its own success summaries (richer than Python's
    # stats dict — it includes intraday start/stop times). Suppress the
    # Python notifier's success notifications to avoid duplicates. Failure
    # notifications from monitored_run still fire (with stack traces) — bash
    # also notifies on non-zero exit, which is intentional belt+suspenders.
    export SAWA_NOTIFY_SUCCESS=0
}

# ── Market status detection ──────────────────────────────────────────────────

check_market_status() {
    # Try Polygon.io market status API (handles holidays, early closes)
    log "Checking market status via Polygon.io API..."
    local response
    if [ -z "${POLYGON_API_KEY:-}" ]; then
        log "WARN: POLYGON_API_KEY is unavailable for market-status check"
        response=""
    else
        # Read the key from the child environment, not argv. Cap the body before
        # it enters a shell variable so a broken endpoint cannot exhaust memory.
        response=$(SAWA_MARKET_STATUS_API_KEY="$POLYGON_API_KEY" \
            python - 2>/dev/null <<'PY'
import os
import signal
import sys
from urllib.request import HTTPRedirectHandler, Request, build_opener

MAX_RESPONSE_BYTES = 64 * 1024


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def deadline_expired(_signum, _frame):
    raise TimeoutError("market-status request exceeded overall deadline")


request = Request(
    "https://api.polygon.io/v1/marketstatus/now",
    headers={"Authorization": f"Bearer {os.environ['SAWA_MARKET_STATUS_API_KEY']}"},
    method="GET",
)
signal.signal(signal.SIGALRM, deadline_expired)
signal.setitimer(signal.ITIMER_REAL, 5)
try:
    with build_opener(NoRedirect).open(request, timeout=5) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
finally:
    signal.setitimer(signal.ITIMER_REAL, 0)
if len(body) > MAX_RESPONSE_BYTES:
    raise SystemExit("market-status response exceeded 64 KiB")
sys.stdout.buffer.write(body)
PY
        ) || true
    fi

    if [ -n "$response" ]; then
        local nyse_status
        nyse_status=$(printf '%s' "$response" | python3 -c "import sys,json; print(json.load(sys.stdin)['exchanges']['nyse'])" 2>/dev/null) || true

        if [ "$nyse_status" = "open" ]; then
            log "Polygon API says NYSE: open"
            echo "open"
            return
        elif [ "$nyse_status" = "closed" ]; then
            log "Polygon API says NYSE: closed"
            echo "closed"
            return
        elif [ "$nyse_status" = "extended-hours" ] || [ "$nyse_status" = "early-hours" ]; then
            # Documented Polygon states for 04:00-09:30 and 16:00-20:00 ET.
            # Treating them as an API failure logged "unreachable" twice a tick
            # for four hours a day and hid genuine outages in the noise.
            log "Polygon API says NYSE: $nyse_status (treated as closed)"
            echo "closed"
            return
        fi
        log "WARN: Polygon API returned unexpected status: $nyse_status"
    fi

    # Fallback: simple time-based check (ET timezone)
    log "WARN: Polygon API unreachable, using time-based fallback"
    local hour minute dow
    hour=$(TZ=America/New_York date '+%-H')
    minute=$(TZ=America/New_York date '+%-M')
    dow=$(TZ=America/New_York date '+%u')  # 1=Mon, 7=Sun

    # Weekends
    if [ "$dow" -ge 6 ]; then
        echo "closed"
        return
    fi

    # Market hours: 9:30 AM - 4:00 PM ET
    local time_mins=$((hour * 60 + minute))
    if [ "$time_mins" -ge 570 ] && [ "$time_mins" -lt 960 ]; then
        echo "open"
    else
        echo "closed"
    fi
}

# ── Intraday process management ─────────────────────────────────────────────

process_start_token() {
    local pid="$1" stat_line stat_tail
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
    [ -r "/proc/$pid/stat" ] || return 1
    IFS= read -r stat_line < "/proc/$pid/stat" || return 1
    # /proc/PID/stat field 2 is parenthesized and may contain spaces. Strip
    # through its final ") "; field 22 (starttime) is then positional field 20.
    stat_tail=${stat_line##*) }
    # Intentional field splitting of the kernel-owned stat record.
    # shellcheck disable=SC2086
    set -- $stat_tail
    [ "$#" -ge 20 ] || return 1
    [[ "${20}" =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "${20}"
}

intraday_command_matches() {
    local pid="$1" previous="" argument
    [ -r "/proc/$pid/cmdline" ] || return 1
    while IFS= read -r -d '' argument; do
        if [ "${previous##*/}" = "sawa" ] && [ "$argument" = "intraday" ]; then
            return 0
        fi
        previous="$argument"
    done < "/proc/$pid/cmdline"
    return 1
}

intraday_identity_matches() {
    local pid="$1" expected_token="$2" actual_token
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
    [[ "$expected_token" =~ ^[0-9]+$ ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    actual_token=$(process_start_token "$pid") || return 1
    [ "$actual_token" = "$expected_token" ] || return 1
    intraday_command_matches "$pid"
}

read_intraday_identity() {
    local pid_file="$1" extra=""
    INTRADAY_PID=""
    INTRADAY_START_TOKEN=""
    IFS=' ' read -r INTRADAY_PID INTRADAY_START_TOKEN extra < "$pid_file" || return 1
    [ -z "$extra" ] || return 1
    [[ "$INTRADAY_PID" =~ ^[1-9][0-9]*$ ]] || return 1
    [[ "$INTRADAY_START_TOKEN" =~ ^[0-9]+$ ]] || return 1
}

is_intraday_running() {
    local pid_file="$STATE_DIR/intraday.pid"
    if [ -f "$pid_file" ]; then
        if read_intraday_identity "$pid_file" \
                && intraday_identity_matches "$INTRADAY_PID" "$INTRADAY_START_TOKEN"; then
            return 0
        else
            log "WARN: discarding invalid/stale intraday process identity"
            rm -f "$pid_file"
        fi
    fi
    return 1
}

start_intraday() {
    log "Starting sawa intraday..."
    mkdir -p "$STATE_DIR"

    # sawa intraday already writes rotating logs to $PROJECT_DIR/logs (see
    # --log-dir). Capturing stdout/stderr separately here just produces an
    # unrotated duplicate that fills the disk (incident 2026-06-04).
    sawa intraday --log-dir "$PROJECT_DIR/logs" >/dev/null 2>&1 9>&- &
    local pid=$!
    local start_token="" identity_ready=false
    for _attempt in {1..20}; do
        if start_token=$(process_start_token "$pid") \
                && intraday_command_matches "$pid"; then
            identity_ready=true
            break
        fi
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.05
    done
    if [ "$identity_ready" != true ]; then
        log "ERROR: could not verify newly started intraday process"
        kill -INT "$pid" 2>/dev/null || true
        return 1
    fi
    printf '%s %s\n' "$pid" "$start_token" > "$STATE_DIR/intraday.pid.tmp"
    chmod 600 "$STATE_DIR/intraday.pid.tmp"
    mv "$STATE_DIR/intraday.pid.tmp" "$STATE_DIR/intraday.pid"
    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/intraday_start_time"

    local start_time
    start_time=$(cat "$STATE_DIR/intraday_start_time")
    log "Intraday started (PID $pid) at $start_time"
    notify "Sawa Intraday Started" "Intraday streaming started at $start_time"
}

stop_intraday() {
    local pid_file="$STATE_DIR/intraday.pid"
    if [ ! -f "$pid_file" ]; then
        return
    fi

    if ! read_intraday_identity "$pid_file" \
            || ! intraday_identity_matches "$INTRADAY_PID" "$INTRADAY_START_TOKEN"; then
        log "WARN: refusing to signal invalid/stale intraday process identity"
        rm -f "$pid_file"
        return
    fi

    local pid="$INTRADAY_PID" start_token="$INTRADAY_START_TOKEN"
    log "Stopping intraday (PID $pid)..."

    # Graceful shutdown via SIGINT
    kill -INT "$pid" 2>/dev/null || true

    # Wait for process to exit
    local waited=0
    while intraday_identity_matches "$pid" "$start_token" \
            && [ "$waited" -lt "$INTRADAY_STOP_TIMEOUT" ]; do
        if [ "$((waited % 10))" -eq 0 ] && [ "$waited" -gt 0 ]; then
            log "Waiting for intraday to exit... (${waited}s/${INTRADAY_STOP_TIMEOUT}s)"
        fi
        sleep 1
        waited=$((waited + 1))
    done

    # Force kill if still running
    if intraday_identity_matches "$pid" "$start_token"; then
        log "WARN: Intraday did not exit gracefully, sending SIGKILL"
        kill -9 "$pid" 2>/dev/null || true
    fi

    rm -f "$pid_file"
    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/intraday_stop_time"

    local start_time stop_time
    start_time=$(cat "$STATE_DIR/intraday_start_time" 2>/dev/null || echo "unknown")
    stop_time=$(cat "$STATE_DIR/intraday_stop_time")
    log "Intraday stopped at $stop_time (started $start_time)"
    notify "Sawa Intraday Stopped" "Intraday stopped at $stop_time (ran $start_time — $stop_time)"
}

# ── Retry cap ────────────────────────────────────────────────────────────────
#
# main() re-enters run_daily/run_weekly on every closed-evening tick while the
# done flag is missing. Count attempts per job and period so an unfixable
# failure (a doctor FAIL the job cannot repair, a provider outage) stops after
# MAX_JOB_ATTEMPTS instead of hammering the provider until midnight.

# Usage: job_attempts_exhausted <job> <period>. Returns 0 (true) when the cap
# has been reached, after alerting once; otherwise records this attempt.
job_attempts_exhausted() {
    local job="$1" period="$2" attempts=0
    local file="$STATE_DIR/${job}_attempts_$period"
    if [ -f "$file" ]; then
        IFS= read -r attempts < "$file" || attempts=0
        [[ "$attempts" =~ ^[0-9]+$ ]] || attempts=0
    fi
    if [ "$attempts" -ge "$MAX_JOB_ATTEMPTS" ]; then
        log "${job^}: giving up for $period after $attempts failed attempt(s)"
        return 0
    fi
    attempts=$((attempts + 1))
    printf '%s\n' "$attempts" > "$file"
    if [ "$attempts" -eq "$MAX_JOB_ATTEMPTS" ]; then
        # Final allowed attempt: say so once, before it runs.
        notify "Sawa ${job^}: last retry" \
            "sawa $job for $period has failed $((attempts - 1)) time(s); this is attempt $attempts of $MAX_JOB_ATTEMPTS. If it fails again the scheduler stops retrying for $period (touch ~/.sawa/scheduler/${job}_done_$period to skip, or delete ${job}_attempts_$period to re-arm)." \
            warning
    fi
    return 1
}

# ── Weekly job ───────────────────────────────────────────────────────────────

is_weekly_done_this_week() {
    # Use ISO week number to track weekly completion
    local week
    week=$(TZ=America/New_York date '+%G-W%V')
    [ -f "$STATE_DIR/weekly_done_$week" ]
}

run_weekly() {
    local week
    week=$(TZ=America/New_York date '+%G-W%V')

    if job_attempts_exhausted weekly "$week"; then
        return 0
    fi

    log "Starting sawa weekly..."
    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/weekly_start_time"

    local exit_code=0
    sawa weekly --log-dir "$PROJECT_DIR/logs" >/dev/null 2>&1 || exit_code=$?

    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/weekly_end_time"

    if [ "$exit_code" -ne 0 ]; then
        log "ERROR: sawa weekly failed (exit $exit_code)"
        notify_unreported_failure "$exit_code" "Sawa Weekly FAILED" \
            "sawa weekly exited with code $exit_code at $(cat "$STATE_DIR/weekly_end_time")"
        heartbeat "${SAWA_WEEKLY_HEARTBEAT_URL:-}" /fail
        return 1
    fi

    if ! run_doctor weekly; then
        heartbeat "${SAWA_WEEKLY_HEARTBEAT_URL:-}" /fail
        return 1
    fi

    # Mark weekly as done
    touch "$STATE_DIR/weekly_done_$week"

    local start_time end_time
    start_time=$(cat "$STATE_DIR/weekly_start_time")
    end_time=$(cat "$STATE_DIR/weekly_end_time")
    log "Weekly completed: $start_time — $end_time"
    notify "Sawa Weekly Complete" "Weekly update finished at $end_time (economy, overviews, news, corporate actions)"
    heartbeat "${SAWA_WEEKLY_HEARTBEAT_URL:-}"

    # Clean up old flag files (keep last 8 weeks)
    find "$STATE_DIR" -name "weekly_done_*" -mtime +60 -delete 2>/dev/null || true
    find "$STATE_DIR" -name "weekly_attempts_*" -mtime +60 -delete 2>/dev/null || true
}

# ── Daily job ────────────────────────────────────────────────────────────────

is_daily_done_today() {
    local today
    today=$(TZ=America/New_York date '+%Y-%m-%d')
    [ -f "$STATE_DIR/daily_done_$today" ]
}

run_daily() {
    local today
    today=$(TZ=America/New_York date '+%Y-%m-%d')

    if job_attempts_exhausted daily "$today"; then
        return 0
    fi

    log "Starting sawa daily..."
    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/daily_start_time"

    local inserted exit_code=0
    # Consume the complete command stream while retaining only the first
    # inserted-price count, so a verbose run cannot grow shell memory without
    # bound. pipefail preserves sawa's exit status through awk.
    # sawa/daily.py logs the committed price count as "Inserted N records"
    # (progress lines read "Inserted N/M ..." and never match).
    inserted=$(sawa daily --log-dir "$PROJECT_DIR/logs" 2>&1 | awk '
        !found && match($0, /Inserted [0-9,]+ records/) {
            value = substr($0, RSTART, RLENGTH)
            sub(/^Inserted /, "", value)
            sub(/ records$/, "", value)
            found = 1
        }
        END { if (found) print value }
    ') || exit_code=$?

    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/daily_end_time"

    if [ "$exit_code" -ne 0 ]; then
        log "ERROR: sawa daily failed (exit $exit_code)"
        notify_unreported_failure "$exit_code" "Sawa Daily FAILED" \
            "sawa daily exited with code $exit_code at $(cat "$STATE_DIR/daily_end_time")"
        heartbeat "${SAWA_HEARTBEAT_URL:-}" /fail
        return 1
    fi

    if ! run_doctor daily; then
        heartbeat "${SAWA_HEARTBEAT_URL:-}" /fail
        return 1
    fi

    # Mark daily as done
    touch "$STATE_DIR/daily_done_$today"

    # Build summary
    local summary
    summary=$(build_daily_summary "$inserted")

    log "Daily completed: $summary"
    notify "Sawa Daily Summary" "$summary"
    heartbeat "${SAWA_HEARTBEAT_URL:-}"

    # Clean up old flag files (keep last 7 days)
    find "$STATE_DIR" -name "daily_done_*" -mtime +7 -delete 2>/dev/null || true
    find "$STATE_DIR" -name "daily_attempts_*" -mtime +7 -delete 2>/dev/null || true
}

build_daily_summary() {
    local inserted="${1:-}"
    local summary=""

    # Query DB for latest price date
    local last_date
    # psycopg reads DATABASE_URL (or discrete PG* variables) from the child
    # environment. The credential-bearing connection string never appears in
    # process arguments, unlike passing the URL as a positional CLI argument.
    if ! last_date=$(command python - 2>/dev/null <<'PY'
import os

import psycopg

conninfo = os.environ.get("DATABASE_URL")
connect_args = (conninfo,) if conninfo else ()
with psycopg.connect(
    *connect_args,
    options="-c default_transaction_read_only=on -c search_path=pg_catalog,public",
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT MAX(date) FROM public.stock_prices")
        value = cursor.fetchone()[0]
        if value is not None:
            print(value)
PY
    ); then
        last_date="unknown"
    fi
    summary="Latest prices: $last_date"

    # Intraday session times
    local intraday_start intraday_stop
    intraday_start=$(cat "$STATE_DIR/intraday_start_time" 2>/dev/null || echo "N/A")
    intraday_stop=$(cat "$STATE_DIR/intraday_stop_time" 2>/dev/null || echo "N/A")
    summary="$summary
Intraday ran: $intraday_start — $intraday_stop"

    # Daily job timing
    local daily_start daily_end
    daily_start=$(cat "$STATE_DIR/daily_start_time" 2>/dev/null || echo "N/A")
    daily_end=$(cat "$STATE_DIR/daily_end_time" 2>/dev/null || echo "N/A")
    summary="$summary
Daily: $daily_start — $daily_end"

    if [ -n "$inserted" ]; then
        summary="$summary
Prices inserted: $inserted"
    fi

    echo "$summary"
}

# ── Main logic ───────────────────────────────────────────────────────────────

main() {
    # Command substitutions run in subshells that do not inherit this trap,
    # so `$(check_market_status)` and friends cannot trigger it.
    trap 'scheduler_exit_handler $?' EXIT
    SCHEDULER_STAGE="initialize_scheduler"
    initialize_scheduler
    SCHEDULER_STAGE="setup_env"
    setup_env
    notify_ignored_env_keys_once

    SCHEDULER_STAGE="market_status"
    local status
    status=$(check_market_status)
    local et_time
    et_time=$(TZ=America/New_York date '+%H:%M ET')
    local action_taken=false

    log "Scheduler tick — market: $status, time: $et_time"
    SCHEDULER_STAGE="tick"
    # Optional per-tick dead-man's switch (SAWA_TICK_HEARTBEAT_URL, e.g. a
    # healthchecks.io check with period 15 min / grace 60 min): the monitor
    # alerts on the ABSENCE of ticks, covering the cases where even this
    # script cannot run (cron stopped, host down, script unreadable).
    heartbeat "${SAWA_TICK_HEARTBEAT_URL:-}"

    if [ "$status" = "open" ]; then
        # Market is open: ensure intraday is running
        if is_intraday_running; then
            log "Intraday: already running (PID $(cat "$STATE_DIR/intraday.pid"))"
        else
            start_intraday
            action_taken=true
        fi
    else
        # Market is closed
        # Stop intraday if still running
        if is_intraday_running; then
            stop_intraday
            action_taken=true
        fi

        # Run daily after market close + wait period
        local hour
        hour=$(TZ=America/New_York date '+%-H')
        local close_hour=$((16 + DAILY_WAIT_HOURS))  # 17 by default

        if [ "$hour" -lt "$close_hour" ]; then
            log "Daily: too early (waiting until ${close_hour}:00 ET)"
        elif is_daily_done_today; then
            log "Daily: already completed today"
        else
            run_daily
            action_taken=true
        fi

        # Run weekly once per ISO week, on the first eligible closed-market
        # evening. Decoupled from Saturday (dow=6) so a missed Saturday tick
        # (host down, cron paused, reboot, lock held) self-heals on the next
        # closed evening — Sunday, or a weekday after close. Idempotent via the
        # per-week weekly_done flag; the in-job get_last_date backfill makes a
        # later catch-up correct. Gated on $close_hour so it doesn't fire
        # mid-session on a closed-but-early tick (e.g. a holiday morning).
        if [ "$hour" -lt "$close_hour" ]; then
            : # too early for the weekly job too; wait for the evening tick
        elif is_weekly_done_this_week; then
            log "Weekly: already completed this week"
        else
            run_weekly
            action_taken=true
        fi
    fi

    if [ "$action_taken" = false ]; then
        log "No action needed"
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
