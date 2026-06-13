#!/bin/bash
# Weekly fundamentals and economy data update
# Usage: ./scripts/weekly.sh [additional args]
# Cron example: 0 2 * * 6 /path/to/scripts/weekly.sh  # Saturday, matching market_scheduler.sh

set -e

cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d .venv ]; then
    source .venv/bin/activate
fi

# Create logs directory
mkdir -p logs

# Run weekly update, then inspect the database before reporting scheduler success.
sawa weekly --log-dir logs "$@"
sawa doctor --job weekly --log-dir logs
