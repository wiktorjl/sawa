#!/bin/bash
# Daily stock price update
# Usage: ./scripts/daily.sh [additional args]
# Cron example: 0 6 * * 1-5 /path/to/scripts/daily.sh

set -e

cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d .venv ]; then
    source .venv/bin/activate
fi

# Create logs directory
mkdir -p logs

# Run daily update with file logging
exec sawa daily --log-dir logs "$@"
