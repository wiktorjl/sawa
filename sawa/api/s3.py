"""
Polygon S3 bulk data client for historical prices.

Downloads daily OHLC data from Polygon's flat files S3 bucket.
"""

import csv
import gzip
import logging
import os
import tempfile
import time
from datetime import date
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, EndpointConnectionError

from sawa.utils.dates import DATE_FORMAT

S3_ENDPOINT = "https://files.polygon.io"
S3_BUCKET = "flatfiles"
S3_KEY_TEMPLATE = "us_stocks_sip/day_aggs_v1/{year}/{month}/{date}.csv.gz"

# Retry configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0  # seconds
DEFAULT_MAX_DELAY = 30.0  # seconds


class PolygonS3Client:
    """Client for Polygon S3 bulk data files."""

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        logger: logging.Logger | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        self.client = session.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            config=BotoConfig(signature_version="s3v4"),
        )

    def download_day(self, target_date: date) -> str | None:
        """
        Download bulk file for a specific date with retry logic.

        Uses exponential backoff for transient failures.
        Does not retry on 404 (no data for that day).

        Args:
            target_date: Date to download

        Returns:
            Path to temp file, or None if no data (weekend/holiday)
        """
        date_str = target_date.strftime(DATE_FORMAT)
        key = S3_KEY_TEMPLATE.format(
            year=f"{target_date.year:04d}",
            month=f"{target_date.month:02d}",
            date=date_str,
        )

        self.logger.debug(f"Downloading s3://{S3_BUCKET}/{key}")

        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                with tempfile.NamedTemporaryFile(delete=False) as tmp:
                    self.client.download_fileobj(S3_BUCKET, key, tmp)
                    return tmp.name
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "404":
                    # No data for this date (weekend/holiday) - don't retry
                    return None
                if error_code in ("500", "502", "503", "504", "SlowDown"):
                    # Transient errors - retry
                    last_error = e
                else:
                    # Other client errors - don't retry
                    raise
            except EndpointConnectionError as e:
                # Network errors - retry
                last_error = e
            except OSError as e:
                # Connection reset, timeout, etc. - retry
                last_error = e

            # Calculate delay with exponential backoff
            delay = min(self.base_delay * (2**attempt), self.max_delay)
            self.logger.warning(
                f"S3 download failed (attempt {attempt + 1}/{self.max_retries}): {last_error}. "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)

        # All retries exhausted
        if last_error:
            raise last_error
        return None

    def parse_bulk_file(
        self,
        filepath: str,
        symbols: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Parse downloaded bulk file into records.

        Args:
            filepath: Path to gzipped CSV file
            symbols: Optional set of symbols to filter

        Returns:
            List of price records
        """
        records: list[dict[str, Any]] = []

        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("CSV missing header row")

            header_map = {name.lower(): name for name in reader.fieldnames}

            def col(names: list[str]) -> str:
                for n in names:
                    if n.lower() in header_map:
                        return header_map[n.lower()]
                raise RuntimeError(f"Column not found: {names}")

            sym_col = col(["symbol", "ticker", "sym"])
            open_col = col(["open", "o"])
            close_col = col(["close", "c"])
            high_col = col(["high", "h"])
            low_col = col(["low", "l"])
            vol_col = col(["volume", "v"])

            for row in reader:
                symbol = (row.get(sym_col) or "").strip()
                if not symbol:
                    continue
                if symbols and symbol not in symbols:
                    continue

                records.append(
                    {
                        "symbol": symbol,
                        "open": row.get(open_col, ""),
                        "close": row.get(close_col, ""),
                        "high": row.get(high_col, ""),
                        "low": row.get(low_col, ""),
                        "volume": row.get(vol_col, ""),
                    }
                )

        return records

    def download_and_parse(
        self,
        target_date: date,
        symbols: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Download and parse bulk file for a date.

        Args:
            target_date: Date to download
            symbols: Optional set of symbols to filter

        Returns:
            List of price records with date added
        """
        filepath = self.download_day(target_date)
        if filepath is None:
            return []

        try:
            records = self.parse_bulk_file(filepath, symbols)
            date_str = target_date.strftime(DATE_FORMAT)
            for r in records:
                r["date"] = date_str
            return records
        finally:
            try:
                os.unlink(filepath)
            except OSError:
                pass
