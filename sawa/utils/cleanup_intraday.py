"""
Utility to cleanup old intraday data.

Intended to be run as a cron job:
    0 0 * * * cd /path/to/sawa && /path/to/.venv/bin/python -m sawa.utils.cleanup_intraday
"""

import logging
import os
import sys

import psycopg
from dotenv import load_dotenv

from sawa.database.intraday_load import cleanup_old_intraday_data
from sawa.utils import setup_logging

DEFAULT_RETENTION_DAYS = 7

load_dotenv()


def main() -> int:
    """Run cleanup and return exit code."""
    logger = setup_logging()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL environment variable not set")
        return 1

    days = int(os.environ.get("INTRADAY_RETENTION_DAYS", DEFAULT_RETENTION_DAYS))

    try:
        with psycopg.connect(db_url) as conn:
            deleted = cleanup_old_intraday_data(conn, days, logger)

        logger.info(f"Cleanup complete: removed {deleted} records")
        return 0

    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
