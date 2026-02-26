#!/usr/bin/env python3
"""
Create nasdaq5000 index, populate from symbols file, and remove nasdaq100.

Usage:
    python scripts/populate_nasdaq5000.py nasdaq1000_symbols.txt
"""

import os
import sys
from pathlib import Path

import psycopg


def setup_nasdaq5000(database_url: str, symbols_file: Path) -> None:
    """Create nasdaq5000 index, assign tickers, remove nasdaq100."""
    # Read symbols from file
    with open(symbols_file) as f:
        symbols = [line.strip().upper() for line in f if line.strip()]

    print(f"Read {len(symbols)} symbols from {symbols_file}")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            # 1. Create nasdaq5000 index
            cur.execute("""
                INSERT INTO indices (code, name, description)
                VALUES ('nasdaq5000', 'NASDAQ-5000', 'NASDAQ listed stocks')
                ON CONFLICT (code) DO NOTHING
            """)
            print("Created nasdaq5000 index")

            # Get nasdaq5000 index id
            cur.execute("SELECT id FROM indices WHERE code = 'nasdaq5000'")
            nasdaq5000_id = cur.fetchone()[0]

            # 2. Migrate existing nasdaq100 constituents to nasdaq5000
            cur.execute("""
                INSERT INTO index_constituents (index_id, ticker)
                SELECT %s, ic.ticker
                FROM index_constituents ic
                JOIN indices i ON ic.index_id = i.id
                WHERE i.code = 'nasdaq100'
                ON CONFLICT DO NOTHING
            """, (nasdaq5000_id,))
            migrated = cur.rowcount
            print(f"Migrated {migrated} nasdaq100 constituents to nasdaq5000")

            # 3. Insert all tickers from file (only those in companies table)
            added = 0
            skipped = 0
            for symbol in symbols:
                cur.execute("""
                    INSERT INTO index_constituents (index_id, ticker)
                    SELECT %s, %s
                    WHERE EXISTS (SELECT 1 FROM companies WHERE ticker = %s)
                    ON CONFLICT DO NOTHING
                """, (nasdaq5000_id, symbol, symbol))
                if cur.rowcount > 0:
                    added += 1
                else:
                    skipped += 1

            print(f"Added {added} new constituents from file")
            print(f"Skipped {skipped} (not in companies or already assigned)")

            # 4. Delete nasdaq100 index (CASCADE removes its constituents)
            cur.execute("DELETE FROM indices WHERE code = 'nasdaq100'")
            if cur.rowcount > 0:
                print("Deleted nasdaq100 index")

            # 5. Update timestamp
            cur.execute(
                "UPDATE indices SET last_updated = CURRENT_TIMESTAMP WHERE id = %s",
                (nasdaq5000_id,),
            )

            conn.commit()

            # Show final count
            cur.execute(
                "SELECT COUNT(*) FROM index_constituents WHERE index_id = %s",
                (nasdaq5000_id,),
            )
            total = cur.fetchone()[0]
            print(f"\nTotal nasdaq5000 constituents: {total}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/populate_nasdaq5000.py <symbols_file>")
        sys.exit(1)

    symbols_file = Path(sys.argv[1])
    if not symbols_file.exists():
        print(f"ERROR: File not found: {symbols_file}")
        sys.exit(1)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        sys.exit(1)

    setup_nasdaq5000(database_url, symbols_file)
