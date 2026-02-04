#!/bin/bash
# Weekly fundamentals and economy data update
# Usage: ./scripts/weekly.sh [additional args]
# Cron example: 0 2 * * 0 /path/to/scripts/weekly.sh

set -e

cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d .venv ]; then
    source .venv/bin/activate
fi

# Create logs directory
mkdir -p logs

# Run weekly update with file logging
exec sawa weekly --log-dir logs "$@"
