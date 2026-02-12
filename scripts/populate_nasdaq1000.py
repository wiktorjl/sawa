#!/usr/bin/env python3
"""
Populate nasdaq1000 index constituents from a file.

Usage:
    python scripts/populate_nasdaq1000.py nasdaq1000_symbols.txt
"""

import os
import sys
from pathlib import Path

import psycopg


def populate_index_constituents(
    database_url: str,
    symbols_file: Path,
    index_code: str = "nasdaq1000"
) -> tuple[int, int]:
    """
    Populate index constituents from a file.

    Args:
        database_url: PostgreSQL connection URL
        symbols_file: Path to file with symbols (one per line)
        index_code: Index code (default: nasdaq1000)

    Returns:
        Tuple of (symbols_added, symbols_skipped)
    """
    # Read symbols from file
    with open(symbols_file) as f:
        symbols = [line.strip().upper() for line in f if line.strip()]

    print(f"Read {len(symbols)} symbols from {symbols_file}")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            # Get index ID
            cur.execute("SELECT id FROM indices WHERE code = %s", (index_code,))
            row = cur.fetchone()
            if not row:
                print(f"ERROR: Index '{index_code}' not found in database")
                print("Run this first:")
                print(f"  psql $DATABASE_URL -f scripts/add_nasdaq1000_index.sql")
                return (0, 0)

            index_id = row[0]
            print(f"Found index: {index_code} (id={index_id})")

            # Clear existing constituents for this index
            cur.execute("DELETE FROM index_constituents WHERE index_id = %s", (index_id,))
            deleted = cur.rowcount
            if deleted > 0:
                print(f"Cleared {deleted} existing constituents")

            # Insert constituents (only those already in companies table)
            added = 0
            skipped = 0

            for symbol in symbols:
                cur.execute(
                    """
                    INSERT INTO index_constituents (index_id, ticker)
                    SELECT %s, %s
                    WHERE EXISTS (SELECT 1 FROM companies WHERE ticker = %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (index_id, symbol, symbol),
                )
                if cur.rowcount > 0:
                    added += 1
                else:
                    skipped += 1
                    print(f"  Skipped {symbol} (not in companies table)")

            # Update last_updated timestamp
            cur.execute(
                "UPDATE indices SET last_updated = CURRENT_TIMESTAMP WHERE id = %s",
                (index_id,)
            )

            conn.commit()

            print(f"\nResults:")
            print(f"  Added: {added} constituents")
            print(f"  Skipped: {skipped} (not in companies table)")

            if skipped > 0:
                print(f"\nTo add the {skipped} missing companies, run:")
                print(f"  sawa add-symbol --file {symbols_file}")

            return (added, skipped)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/populate_nasdaq1000.py <symbols_file>")
        print("Example: python scripts/populate_nasdaq1000.py nasdaq1000_symbols.txt")
        sys.exit(1)

    symbols_file = Path(sys.argv[1])
    if not symbols_file.exists():
        print(f"ERROR: File not found: {symbols_file}")
        sys.exit(1)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        sys.exit(1)

    added, skipped = populate_index_constituents(database_url, symbols_file)
    sys.exit(0 if added > 0 else 1)
