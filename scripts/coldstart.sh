#!/bin/bash
# Full database bootstrap from scratch
# Usage: ./scripts/coldstart.sh [additional args]

set -e

cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d .venv ]; then
    source .venv/bin/activate
fi

# Create logs directory
mkdir -p logs

# Run coldstart with file logging
exec sawa coldstart --log-dir logs "$@"
