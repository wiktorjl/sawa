#!/bin/bash
# Market Hours Scheduler
# Runs via cron every 15 minutes on weekdays. Manages sawa intraday streaming
# during market hours and runs sawa daily after market close.
#
# Crontab entry (install manually):
#   */15 * * * 1-5 /home/seed/code/sawa/scripts/market_scheduler.sh >> ~/.sawa/scheduler/cron.log 2>&1
#
# State directory: ~/.sawa/scheduler/

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STATE_DIR="$HOME/.sawa/scheduler"
LOG_FILE="$STATE_DIR/scheduler.log"
NTFY_TOPIC="ntfy.sh/WiktorAI"
DAILY_WAIT_HOURS=1  # hours after close before running daily
INTRADAY_STOP_TIMEOUT=60  # seconds to wait for graceful shutdown

# ── State directory setup ────────────────────────────────────────────────────

mkdir -p "$STATE_DIR"

# ── Lock (prevent overlapping runs) ──────────────────────────────────────────

LOCK_FILE="$STATE_DIR/scheduler.lock"

acquire_lock() {
    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
        echo "[$(TZ=America/New_York date '+%Y-%m-%d %H:%M:%S ET')] Another scheduler is already running, skipping" >&2
        exit 0
    fi
    # Write PID for debugging
    echo $$ >&9
}

acquire_lock

# ── Logging ──────────────────────────────────────────────────────────────────

log() {
    local ts
    ts=$(TZ=America/New_York date '+%Y-%m-%d %H:%M:%S ET')
    local msg="[$ts] $*"
    echo "$msg" >> "$LOG_FILE"
    echo "$msg" >&2
}

# Trim log to last 5000 lines periodically
if [ -f "$LOG_FILE" ] && [ "$(wc -l < "$LOG_FILE")" -gt 10000 ]; then
    tail -n 5000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi

# ── Notifications ────────────────────────────────────────────────────────────

notify() {
    local title="$1"
    local body="$2"
    log "Sending notification: $title"
    curl -s \
        -H "Title: $title" \
        -H "Tags: chart_with_upwards_trend" \
        -d "$body" \
        "$NTFY_TOPIC" > /dev/null 2>&1 || log "WARN: ntfy notification failed"
}

# ── Environment setup ────────────────────────────────────────────────────────

setup_env() {
    cd "$PROJECT_DIR"

    # Load .env
    if [ -f .env ]; then
        set -a
        # shellcheck disable=SC1091
        source .env
        set +a
    fi

    # Activate virtualenv
    if [ -f .venv/bin/activate ]; then
        # shellcheck disable=SC1091
        source .venv/bin/activate
    fi
}

# ── Market status detection ──────────────────────────────────────────────────

check_market_status() {
    # Try Polygon.io market status API (handles holidays, early closes)
    log "Checking market status via Polygon.io API..."
    local response
    response=$(curl -s --max-time 5 \
        "https://api.polygon.io/v1/marketstatus/now?apiKey=$POLYGON_API_KEY" 2>/dev/null) || true

    if [ -n "$response" ]; then
        local nyse_status
        nyse_status=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin)['exchanges']['nyse'])" 2>/dev/null) || true

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

is_intraday_running() {
    local pid_file="$STATE_DIR/intraday.pid"
    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        else
            # Stale PID file
            rm -f "$pid_file"
        fi
    fi
    return 1
}

start_intraday() {
    log "Starting sawa intraday..."
    mkdir -p "$STATE_DIR"

    sawa intraday --log-dir "$PROJECT_DIR/logs" >> "$STATE_DIR/intraday.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$STATE_DIR/intraday.pid"
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

    local pid
    pid=$(cat "$pid_file")
    log "Stopping intraday (PID $pid)..."

    # Graceful shutdown via SIGINT
    kill -INT "$pid" 2>/dev/null || true

    # Wait for process to exit
    local waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$INTRADAY_STOP_TIMEOUT" ]; do
        if [ "$((waited % 10))" -eq 0 ] && [ "$waited" -gt 0 ]; then
            log "Waiting for intraday to exit... (${waited}s/${INTRADAY_STOP_TIMEOUT}s)"
        fi
        sleep 1
        waited=$((waited + 1))
    done

    # Force kill if still running
    if kill -0 "$pid" 2>/dev/null; then
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

    local output exit_code=0
    output=$(sawa weekly --log-dir "$PROJECT_DIR/logs" 2>&1) || exit_code=$?

    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/weekly_end_time"

    if [ "$exit_code" -ne 0 ]; then
        log "ERROR: sawa weekly failed (exit $exit_code)"
        notify "Sawa Weekly FAILED" "sawa weekly exited with code $exit_code at $(cat "$STATE_DIR/weekly_end_time")"
        return 1
    fi

    # Mark weekly as done
    touch "$STATE_DIR/weekly_done_$week"

    local start_time end_time
    start_time=$(cat "$STATE_DIR/weekly_start_time")
    end_time=$(cat "$STATE_DIR/weekly_end_time")
    log "Weekly completed: $start_time — $end_time"
    notify "Sawa Weekly Complete" "Weekly update finished at $end_time (economy, overviews, news, corporate actions)"

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

    local output exit_code=0
    output=$(sawa daily --log-dir "$PROJECT_DIR/logs" 2>&1) || exit_code=$?

    TZ=America/New_York date '+%Y-%m-%d %H:%M ET' > "$STATE_DIR/daily_end_time"

    if [ "$exit_code" -ne 0 ]; then
        log "ERROR: sawa daily failed (exit $exit_code)"
        notify "Sawa Daily FAILED" "sawa daily exited with code $exit_code at $(cat "$STATE_DIR/daily_end_time")"
        return 1
    fi

    # Mark daily as done
    touch "$STATE_DIR/daily_done_$today"

    # Build summary
    local summary
    summary=$(build_daily_summary "$output")

    log "Daily completed: $summary"
    notify "Sawa Daily Summary" "$summary"

    # Clean up old flag files (keep last 7 days)
    find "$STATE_DIR" -name "daily_done_*" -mtime +7 -delete 2>/dev/null || true
}

build_daily_summary() {
    local output="$1"
    local summary=""

    # Query DB for latest price date
    local last_date
    last_date=$(psql "$DATABASE_URL" -t -A -c "SELECT MAX(date) FROM stock_prices" 2>/dev/null || echo "unknown")
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

    # Parse prices inserted from output
    local inserted
    inserted=$(echo "$output" | grep -oP 'Inserted \K[0-9,]+(?= price)' 2>/dev/null | head -1 || true)
    if [ -n "$inserted" ]; then
        summary="$summary
Prices inserted: $inserted"
    fi

    echo "$summary"
}

# ── Main logic ───────────────────────────────────────────────────────────────

main() {
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
        local hour dow
        hour=$(TZ=America/New_York date '+%-H')
        dow=$(TZ=America/New_York date '+%u')  # 1=Mon, 7=Sun
        local close_hour=$((16 + DAILY_WAIT_HOURS))  # 17 by default

        if [ "$hour" -lt "$close_hour" ]; then
            log "Daily: too early (waiting until ${close_hour}:00 ET)"
        elif is_daily_done_today; then
            log "Daily: already completed today"
        else
            run_daily
            action_taken=true
        fi

        # Run weekly on Saturdays (dow=6), after daily would have run
        if [ "$dow" -eq 6 ]; then
            if is_weekly_done_this_week; then
                log "Weekly: already completed this week"
            else
                run_weekly
                action_taken=true
            fi
        fi
    fi

    if [ "$action_taken" = false ]; then
        log "No action needed"
    fi
}

main "$@"
