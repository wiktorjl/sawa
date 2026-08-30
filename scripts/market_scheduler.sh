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

# ── Lock (prevent overlapping runs) ──────────────────────────────────────────

LOCK_FILE="$STATE_DIR/scheduler.lock"

acquire_lock() {
    exec 9>"$LOCK_FILE"
    chmod 600 "$LOCK_FILE"
    if ! flock -n 9; then
        echo "[$(TZ=America/New_York date '+%Y-%m-%d %H:%M:%S ET')] Another scheduler is already running, skipping" >&2
        exit 0
    fi
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
    echo "$msg" >> "$LOG_FILE"
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

run_doctor() {
    local job="$1"
    local exit_code=0

    log "Starting sawa doctor --job $job..."
    sawa doctor --job "$job" --log-dir "$PROJECT_DIR/logs" \
        >> "$LOG_FILE" 2>&1 || exit_code=$?

    if [ "$exit_code" -ne 0 ]; then
        log "ERROR: sawa doctor --job $job failed (exit $exit_code)"
        notify "Sawa Doctor FAILED" "doctor --job $job exited with code $exit_code" error
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
        local dotenv_exports
        if ! dotenv_exports=$(python - "$PROJECT_DIR/.env" <<'PY'
import os
import re
import shlex
import sys

from dotenv import dotenv_values

path = sys.argv[1]
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
    "SAWA_WEEKLY_HEARTBEAT_URL",
}
flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
fd = os.open(path, flags)
os.fchmod(fd, 0o600)
with os.fdopen(fd, encoding="utf-8") as stream:
    values = dotenv_values(stream=stream)
for key, value in values.items():
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        raise SystemExit(f"invalid environment variable name: {key!r}")
    if key not in allowed:
        raise SystemExit(f"environment variable is not allowed in scheduler .env: {key}")
    if value is not None:
        print(f"export {key}={shlex.quote(value)}")
PY
        ); then
            log "ERROR: could not safely parse .env"
            return 1
        fi
        eval "$dotenv_exports"
        unset dotenv_exports
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

    log "Starting sawa weekly..."
    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/weekly_start_time"

    local exit_code=0
    sawa weekly --log-dir "$PROJECT_DIR/logs" >/dev/null 2>&1 || exit_code=$?

    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/weekly_end_time"

    if [ "$exit_code" -ne 0 ]; then
        log "ERROR: sawa weekly failed (exit $exit_code)"
        notify "Sawa Weekly FAILED" "sawa weekly exited with code $exit_code at $(cat "$STATE_DIR/weekly_end_time")" error
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

    log "Starting sawa daily..."
    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/daily_start_time"

    local inserted exit_code=0
    # Consume the complete command stream while retaining only the first
    # inserted-price count, so a verbose run cannot grow shell memory without
    # bound. pipefail preserves sawa's exit status through awk.
    inserted=$(sawa daily --log-dir "$PROJECT_DIR/logs" 2>&1 | awk '
        !found && match($0, /Inserted [0-9,]+ price/) {
            value = substr($0, RSTART, RLENGTH)
            sub(/^Inserted /, "", value)
            sub(/ price$/, "", value)
            found = 1
        }
        END { if (found) print value }
    ') || exit_code=$?

    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/daily_end_time"

    if [ "$exit_code" -ne 0 ]; then
        log "ERROR: sawa daily failed (exit $exit_code)"
        notify "Sawa Daily FAILED" "sawa daily exited with code $exit_code at $(cat "$STATE_DIR/daily_end_time")" error
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
    initialize_scheduler
    setup_env

    local status
    status=$(check_market_status)
    local et_time
    et_time=$(TZ=America/New_York date '+%H:%M ET')
    local action_taken=false

    log "Scheduler tick — market: $status, time: $et_time"

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
