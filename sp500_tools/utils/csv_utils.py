"""Standardized CSV handling utilities."""

import csv
import logging
from pathlib import Path
from typing import Any


def get_existing_keys(
    filepath: Path,
    key_field: str,
    logger: logging.Logger | None = None,
) -> set[str]:
    """
    Get set of existing record keys from CSV file.

    Args:
        filepath: Path to CSV file
        key_field: Column name to use as key
        logger: Optional logger for warnings

    Returns:
        Set of existing key values
    """
    if not filepath.exists():
        return set()

    existing: set[str] = set()
    try:
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if key_field in row and row[key_field]:
                    existing.add(row[key_field])
    except (OSError, csv.Error) as e:
        if logger:
            logger.warning(f"Could not read {filepath}: {e}")
    return existing


def append_csv(
    filepath: Path,
    data: list[dict[str, Any]],
    fieldnames: list[str],
    logger: logging.Logger | None = None,
) -> int:
    """
    Append rows to CSV file, creating if needed.

    Args:
        filepath: Path to CSV file
        data: List of row dictionaries
        fieldnames: Column names in order
        logger: Optional logger for status messages

    Returns:
        Number of rows written
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    file_exists = filepath.exists()
    mode = "a" if file_exists else "w"

    with open(filepath, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)

    if logger:
        action = "Appended to" if file_exists else "Created"
        logger.debug(f"{action} {filepath}: {len(data)} rows")
    return len(data)
