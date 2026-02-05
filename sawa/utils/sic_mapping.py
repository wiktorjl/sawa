"""SIC code to GICS sector mapping.

Maps SEC SIC codes (4-digit) to GICS classification (Sector and Industry).
Based on Yahoo Finance sector classifications and SEC SIC code descriptions.

Storage: Primary storage is the `sic_gics_mapping` database table.
Fallback: Dictionary-based mapping if database is unavailable.

GICS Sectors (11 total):
- Energy
- Materials
- Industrials
- Consumer Discretionary
- Consumer Staples
- Health Care
- Financials
- Information Technology
- Communication Services
- Utilities
- Real Estate
"""

import logging
import os
from functools import lru_cache
from typing import TypedDict

logger = logging.getLogger(__name__)


class GICSMapping(TypedDict):
    """GICS sector/industry mapping with confidence and notes."""

    gics_sector: str
    gics_industry: str
    confidence: str  # "high", "medium", "low"
    notes: str


# In-memory cache for database mappings
_db_mappings_cache: dict[str, GICSMapping] | None = None


def _get_database_url() -> str | None:
    """Get database URL from environment."""
    # Try DATABASE_URL first (MCP server pattern)
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    # Try to construct from PG* variables (sawa pattern)
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    password = os.environ.get("PGPASSWORD")

    if all([dbname, user, password]):
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    return None


def load_mappings_from_db(database_url: str | None = None) -> dict[str, GICSMapping]:
    """Load all SIC-to-GICS mappings from the database.

    Args:
        database_url: Database connection URL (optional, uses env vars if not provided)

    Returns:
        Dictionary of SIC code -> GICSMapping

    Raises:
        RuntimeError: If database connection fails and no URL provided
    """
    global _db_mappings_cache

    # Return cached if available
    if _db_mappings_cache is not None:
        return _db_mappings_cache

    url = database_url or _get_database_url()
    if not url:
        logger.warning("No database URL available, using fallback dictionary")
        return {}

    try:
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(url, row_factory=dict_row) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sic_code, gics_sector, gics_industry, confidence, notes
                    FROM sic_gics_mapping
                    """
                )
                rows = cur.fetchall()

        mappings: dict[str, GICSMapping] = {}
        for row in rows:
            mappings[row["sic_code"]] = GICSMapping(
                gics_sector=row["gics_sector"],
                gics_industry=row["gics_industry"],
                confidence=row["confidence"],
                notes=row["notes"] or "",
            )

        # Cache the result
        _db_mappings_cache = mappings
        logger.info(f"Loaded {len(mappings)} SIC-to-GICS mappings from database")
        return mappings

    except Exception as e:
        logger.warning(f"Failed to load mappings from database: {e}")
        return {}


def clear_cache() -> None:
    """Clear the in-memory mappings cache."""
    global _db_mappings_cache
    _db_mappings_cache = None


@lru_cache(maxsize=512)
def _get_mapping_from_db(sic_code: str) -> GICSMapping | None:
    """Get a single mapping from database (with LRU cache).

    This is used when the full cache isn't loaded.
    """
    url = _get_database_url()
    if not url:
        return None

    try:
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(url, row_factory=dict_row) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT gics_sector, gics_industry, confidence, notes
                    FROM sic_gics_mapping
                    WHERE sic_code = %s
                    """,
                    (sic_code,),
                )
                row = cur.fetchone()

        if row:
            return GICSMapping(
                gics_sector=row["gics_sector"],
                gics_industry=row["gics_industry"],
                confidence=row["confidence"],
                notes=row["notes"] or "",
            )

    except Exception as e:
        logger.debug(f"Database lookup failed for SIC {sic_code}: {e}")

    return None


# =============================================================================
# FALLBACK DICTIONARY
# =============================================================================
# Used when database is unavailable or for testing

SIC_TO_GICS_FALLBACK: dict[str, GICSMapping] = {
    # Information Technology
    "7372": {
        "gics_sector": "Information Technology",
        "gics_industry": "Software",
        "confidence": "high",
        "notes": "",
    },
    "7371": {
        "gics_sector": "Information Technology",
        "gics_industry": "IT Services",
        "confidence": "high",
        "notes": "",
    },
    "7373": {
        "gics_sector": "Information Technology",
        "gics_industry": "IT Services",
        "confidence": "high",
        "notes": "",
    },
    "7374": {
        "gics_sector": "Information Technology",
        "gics_industry": "IT Services",
        "confidence": "high",
        "notes": "",
    },
    "3674": {
        "gics_sector": "Information Technology",
        "gics_industry": "Semiconductors",
        "confidence": "high",
        "notes": "",
    },
    "3672": {
        "gics_sector": "Information Technology",
        "gics_industry": "Electronic Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3670": {
        "gics_sector": "Information Technology",
        "gics_industry": "Electronic Equipment",
        "confidence": "medium",
        "notes": "",
    },
    "3679": {
        "gics_sector": "Information Technology",
        "gics_industry": "Electronic Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3678": {
        "gics_sector": "Information Technology",
        "gics_industry": "Electronic Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3663": {
        "gics_sector": "Information Technology",
        "gics_industry": "Communications Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3669": {
        "gics_sector": "Information Technology",
        "gics_industry": "Communications Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3571": {
        "gics_sector": "Information Technology",
        "gics_industry": "Technology Hardware",
        "confidence": "high",
        "notes": "",
    },
    "3572": {
        "gics_sector": "Information Technology",
        "gics_industry": "Technology Hardware",
        "confidence": "high",
        "notes": "",
    },
    "3570": {
        "gics_sector": "Information Technology",
        "gics_industry": "Technology Hardware",
        "confidence": "high",
        "notes": "",
    },
    "3576": {
        "gics_sector": "Information Technology",
        "gics_industry": "Communications Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3577": {
        "gics_sector": "Information Technology",
        "gics_industry": "Technology Hardware",
        "confidence": "medium",
        "notes": "",
    },
    "3559": {
        "gics_sector": "Information Technology",
        "gics_industry": "Semiconductor Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3827": {
        "gics_sector": "Information Technology",
        "gics_industry": "Semiconductor Equipment",
        "confidence": "high",
        "notes": "",
    },
    "8741": {
        "gics_sector": "Information Technology",
        "gics_industry": "IT Services",
        "confidence": "high",
        "notes": "",
    },
    "6794": {
        "gics_sector": "Information Technology",
        "gics_industry": "Technology Hardware",
        "confidence": "medium",
        "notes": "",
    },
    "3357": {
        "gics_sector": "Information Technology",
        "gics_industry": "Electronic Equipment",
        "confidence": "high",
        "notes": "",
    },
    # Communication Services
    "7370": {
        "gics_sector": "Communication Services",
        "gics_industry": "Interactive Media",
        "confidence": "high",
        "notes": "",
    },
    "4813": {
        "gics_sector": "Communication Services",
        "gics_industry": "Integrated Telecom",
        "confidence": "high",
        "notes": "",
    },
    "4812": {
        "gics_sector": "Communication Services",
        "gics_industry": "Wireless Telecom",
        "confidence": "high",
        "notes": "",
    },
    "4899": {
        "gics_sector": "Communication Services",
        "gics_industry": "Wireless Telecom",
        "confidence": "medium",
        "notes": "",
    },
    "4833": {
        "gics_sector": "Communication Services",
        "gics_industry": "Broadcasting",
        "confidence": "high",
        "notes": "",
    },
    "4832": {
        "gics_sector": "Communication Services",
        "gics_industry": "Broadcasting",
        "confidence": "high",
        "notes": "",
    },
    "4841": {
        "gics_sector": "Communication Services",
        "gics_industry": "Cable & Satellite",
        "confidence": "high",
        "notes": "",
    },
    "2711": {
        "gics_sector": "Communication Services",
        "gics_industry": "Publishing",
        "confidence": "high",
        "notes": "",
    },
    "7841": {
        "gics_sector": "Communication Services",
        "gics_industry": "Movies & Entertainment",
        "confidence": "high",
        "notes": "",
    },
    "7900": {
        "gics_sector": "Communication Services",
        "gics_industry": "Entertainment",
        "confidence": "high",
        "notes": "",
    },
    "7311": {
        "gics_sector": "Communication Services",
        "gics_industry": "Advertising",
        "confidence": "high",
        "notes": "",
    },
    # Health Care
    "2834": {
        "gics_sector": "Health Care",
        "gics_industry": "Pharmaceuticals",
        "confidence": "high",
        "notes": "",
    },
    "2836": {
        "gics_sector": "Health Care",
        "gics_industry": "Biotechnology",
        "confidence": "high",
        "notes": "",
    },
    "2835": {
        "gics_sector": "Health Care",
        "gics_industry": "Life Sciences Tools",
        "confidence": "high",
        "notes": "",
    },
    "3841": {
        "gics_sector": "Health Care",
        "gics_industry": "Medical Devices",
        "confidence": "high",
        "notes": "",
    },
    "3842": {
        "gics_sector": "Health Care",
        "gics_industry": "Medical Devices",
        "confidence": "high",
        "notes": "",
    },
    "3844": {
        "gics_sector": "Health Care",
        "gics_industry": "Medical Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3845": {
        "gics_sector": "Health Care",
        "gics_industry": "Medical Devices",
        "confidence": "high",
        "notes": "",
    },
    "3851": {
        "gics_sector": "Health Care",
        "gics_industry": "Medical Devices",
        "confidence": "high",
        "notes": "",
    },
    "3826": {
        "gics_sector": "Health Care",
        "gics_industry": "Life Sciences Tools",
        "confidence": "high",
        "notes": "",
    },
    "8071": {
        "gics_sector": "Health Care",
        "gics_industry": "Life Sciences Tools",
        "confidence": "high",
        "notes": "",
    },
    "8731": {
        "gics_sector": "Health Care",
        "gics_industry": "Life Sciences Tools",
        "confidence": "high",
        "notes": "",
    },
    "6324": {
        "gics_sector": "Health Care",
        "gics_industry": "Managed Health Care",
        "confidence": "high",
        "notes": "",
    },
    "8062": {
        "gics_sector": "Health Care",
        "gics_industry": "Health Care Facilities",
        "confidence": "high",
        "notes": "",
    },
    "8082": {
        "gics_sector": "Health Care",
        "gics_industry": "Health Care Services",
        "confidence": "high",
        "notes": "",
    },
    "8090": {
        "gics_sector": "Health Care",
        "gics_industry": "Health Care Services",
        "confidence": "high",
        "notes": "",
    },
    "8011": {
        "gics_sector": "Health Care",
        "gics_industry": "Health Care Services",
        "confidence": "high",
        "notes": "",
    },
    "8051": {
        "gics_sector": "Health Care",
        "gics_industry": "Health Care Facilities",
        "confidence": "high",
        "notes": "",
    },
    "5122": {
        "gics_sector": "Health Care",
        "gics_industry": "Health Care Distributors",
        "confidence": "high",
        "notes": "",
    },
    "5047": {
        "gics_sector": "Health Care",
        "gics_industry": "Health Care Distributors",
        "confidence": "high",
        "notes": "",
    },
    # Financials
    "6022": {
        "gics_sector": "Financials",
        "gics_industry": "Banks",
        "confidence": "high",
        "notes": "",
    },
    "6021": {
        "gics_sector": "Financials",
        "gics_industry": "Banks",
        "confidence": "high",
        "notes": "",
    },
    "6035": {
        "gics_sector": "Financials",
        "gics_industry": "Thrifts & Mortgage",
        "confidence": "high",
        "notes": "",
    },
    "6331": {
        "gics_sector": "Financials",
        "gics_industry": "Property & Casualty Insurance",
        "confidence": "high",
        "notes": "",
    },
    "6311": {
        "gics_sector": "Financials",
        "gics_industry": "Life Insurance",
        "confidence": "high",
        "notes": "",
    },
    "6321": {
        "gics_sector": "Financials",
        "gics_industry": "Life & Health Insurance",
        "confidence": "high",
        "notes": "",
    },
    "6411": {
        "gics_sector": "Financials",
        "gics_industry": "Insurance Brokers",
        "confidence": "high",
        "notes": "",
    },
    "6399": {
        "gics_sector": "Financials",
        "gics_industry": "Insurance",
        "confidence": "high",
        "notes": "",
    },
    "6211": {
        "gics_sector": "Financials",
        "gics_industry": "Capital Markets",
        "confidence": "high",
        "notes": "",
    },
    "6200": {
        "gics_sector": "Financials",
        "gics_industry": "Capital Markets",
        "confidence": "high",
        "notes": "",
    },
    "6282": {
        "gics_sector": "Financials",
        "gics_industry": "Asset Management",
        "confidence": "high",
        "notes": "",
    },
    "6141": {
        "gics_sector": "Financials",
        "gics_industry": "Consumer Finance",
        "confidence": "high",
        "notes": "",
    },
    "6163": {
        "gics_sector": "Financials",
        "gics_industry": "Mortgage Finance",
        "confidence": "high",
        "notes": "",
    },
    "6199": {
        "gics_sector": "Financials",
        "gics_industry": "Financial Services",
        "confidence": "medium",
        "notes": "",
    },
    "7389": {
        "gics_sector": "Financials",
        "gics_industry": "Transaction Processing",
        "confidence": "medium",
        "notes": "Ambiguous",
    },
    # Real Estate
    "6798": {
        "gics_sector": "Real Estate",
        "gics_industry": "REITs",
        "confidence": "high",
        "notes": "",
    },
    "6531": {
        "gics_sector": "Real Estate",
        "gics_industry": "Real Estate Services",
        "confidence": "high",
        "notes": "",
    },
    "6500": {
        "gics_sector": "Real Estate",
        "gics_industry": "Real Estate Services",
        "confidence": "high",
        "notes": "",
    },
    "6510": {
        "gics_sector": "Real Estate",
        "gics_industry": "Real Estate Services",
        "confidence": "high",
        "notes": "",
    },
    # Utilities
    "4911": {
        "gics_sector": "Utilities",
        "gics_industry": "Electric Utilities",
        "confidence": "high",
        "notes": "",
    },
    "4931": {
        "gics_sector": "Utilities",
        "gics_industry": "Multi-Utilities",
        "confidence": "high",
        "notes": "",
    },
    "4991": {
        "gics_sector": "Utilities",
        "gics_industry": "Independent Power",
        "confidence": "high",
        "notes": "",
    },
    "4922": {
        "gics_sector": "Utilities",
        "gics_industry": "Gas Utilities",
        "confidence": "medium",
        "notes": "",
    },
    "4923": {
        "gics_sector": "Utilities",
        "gics_industry": "Gas Utilities",
        "confidence": "high",
        "notes": "",
    },
    "4924": {
        "gics_sector": "Utilities",
        "gics_industry": "Gas Utilities",
        "confidence": "high",
        "notes": "",
    },
    "4932": {
        "gics_sector": "Utilities",
        "gics_industry": "Multi-Utilities",
        "confidence": "high",
        "notes": "",
    },
    "4941": {
        "gics_sector": "Utilities",
        "gics_industry": "Water Utilities",
        "confidence": "high",
        "notes": "",
    },
    # Energy
    "1311": {
        "gics_sector": "Energy",
        "gics_industry": "Oil & Gas E&P",
        "confidence": "high",
        "notes": "",
    },
    "6792": {
        "gics_sector": "Energy",
        "gics_industry": "Oil & Gas E&P",
        "confidence": "high",
        "notes": "",
    },
    "2911": {
        "gics_sector": "Energy",
        "gics_industry": "Oil & Gas Refining",
        "confidence": "high",
        "notes": "",
    },
    "1389": {
        "gics_sector": "Energy",
        "gics_industry": "Oil & Gas Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3533": {
        "gics_sector": "Energy",
        "gics_industry": "Oil & Gas Equipment",
        "confidence": "high",
        "notes": "",
    },
    "4610": {
        "gics_sector": "Energy",
        "gics_industry": "Oil & Gas Midstream",
        "confidence": "high",
        "notes": "",
    },
    # Materials
    "2821": {
        "gics_sector": "Materials",
        "gics_industry": "Chemicals",
        "confidence": "high",
        "notes": "",
    },
    "2810": {
        "gics_sector": "Materials",
        "gics_industry": "Industrial Gases",
        "confidence": "high",
        "notes": "",
    },
    "2860": {
        "gics_sector": "Materials",
        "gics_industry": "Specialty Chemicals",
        "confidence": "high",
        "notes": "",
    },
    "2870": {
        "gics_sector": "Materials",
        "gics_industry": "Fertilizers",
        "confidence": "high",
        "notes": "",
    },
    "2800": {
        "gics_sector": "Materials",
        "gics_industry": "Chemicals",
        "confidence": "high",
        "notes": "",
    },
    "2851": {
        "gics_sector": "Materials",
        "gics_industry": "Specialty Chemicals",
        "confidence": "high",
        "notes": "",
    },
    "1000": {
        "gics_sector": "Materials",
        "gics_industry": "Metals & Mining",
        "confidence": "high",
        "notes": "",
    },
    "1040": {
        "gics_sector": "Materials",
        "gics_industry": "Gold",
        "confidence": "high",
        "notes": "",
    },
    "1400": {
        "gics_sector": "Materials",
        "gics_industry": "Construction Materials",
        "confidence": "high",
        "notes": "",
    },
    "3312": {
        "gics_sector": "Materials",
        "gics_industry": "Steel",
        "confidence": "high",
        "notes": "",
    },
    "3334": {
        "gics_sector": "Materials",
        "gics_industry": "Aluminum",
        "confidence": "high",
        "notes": "",
    },
    "3350": {
        "gics_sector": "Materials",
        "gics_industry": "Metals & Mining",
        "confidence": "high",
        "notes": "",
    },
    "3241": {
        "gics_sector": "Materials",
        "gics_industry": "Construction Materials",
        "confidence": "high",
        "notes": "",
    },
    "6795": {
        "gics_sector": "Materials",
        "gics_industry": "Gold",
        "confidence": "high",
        "notes": "",
    },
    "2650": {
        "gics_sector": "Materials",
        "gics_industry": "Containers & Packaging",
        "confidence": "high",
        "notes": "",
    },
    "2670": {
        "gics_sector": "Materials",
        "gics_industry": "Containers & Packaging",
        "confidence": "high",
        "notes": "",
    },
    "2673": {
        "gics_sector": "Materials",
        "gics_industry": "Containers & Packaging",
        "confidence": "high",
        "notes": "",
    },
    "3411": {
        "gics_sector": "Materials",
        "gics_industry": "Containers & Packaging",
        "confidence": "high",
        "notes": "",
    },
    "3089": {
        "gics_sector": "Materials",
        "gics_industry": "Containers & Packaging",
        "confidence": "high",
        "notes": "",
    },
    "3990": {
        "gics_sector": "Materials",
        "gics_industry": "Containers & Packaging",
        "confidence": "high",
        "notes": "",
    },
    "2621": {
        "gics_sector": "Materials",
        "gics_industry": "Paper Products",
        "confidence": "high",
        "notes": "",
    },
    "2421": {
        "gics_sector": "Materials",
        "gics_industry": "Forest Products",
        "confidence": "high",
        "notes": "",
    },
    "0100": {
        "gics_sector": "Materials",
        "gics_industry": "Agricultural Inputs",
        "confidence": "high",
        "notes": "",
    },
    # Industrials
    "3760": {
        "gics_sector": "Industrials",
        "gics_industry": "Aerospace & Defense",
        "confidence": "high",
        "notes": "",
    },
    "3812": {
        "gics_sector": "Industrials",
        "gics_industry": "Aerospace & Defense",
        "confidence": "high",
        "notes": "",
    },
    "3721": {
        "gics_sector": "Industrials",
        "gics_industry": "Aerospace & Defense",
        "confidence": "high",
        "notes": "",
    },
    "3720": {
        "gics_sector": "Industrials",
        "gics_industry": "Aerospace & Defense",
        "confidence": "high",
        "notes": "",
    },
    "3724": {
        "gics_sector": "Industrials",
        "gics_industry": "Aerospace & Defense",
        "confidence": "high",
        "notes": "",
    },
    "3728": {
        "gics_sector": "Industrials",
        "gics_industry": "Aerospace & Defense",
        "confidence": "high",
        "notes": "",
    },
    "3730": {
        "gics_sector": "Industrials",
        "gics_industry": "Aerospace & Defense",
        "confidence": "high",
        "notes": "",
    },
    "3480": {
        "gics_sector": "Industrials",
        "gics_industry": "Aerospace & Defense",
        "confidence": "high",
        "notes": "",
    },
    "3560": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3561": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3569": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3510": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3523": {
        "gics_sector": "Industrials",
        "gics_industry": "Agricultural Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3530": {
        "gics_sector": "Industrials",
        "gics_industry": "Construction Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3531": {
        "gics_sector": "Industrials",
        "gics_industry": "Construction Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3540": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3550": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3580": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3590": {
        "gics_sector": "Industrials",
        "gics_industry": "Electrical Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3743": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3490": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3420": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Machinery",
        "confidence": "high",
        "notes": "",
    },
    "3600": {
        "gics_sector": "Industrials",
        "gics_industry": "Electrical Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3620": {
        "gics_sector": "Industrials",
        "gics_industry": "Electrical Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3621": {
        "gics_sector": "Industrials",
        "gics_industry": "Electrical Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3613": {
        "gics_sector": "Industrials",
        "gics_industry": "Electrical Equipment",
        "confidence": "high",
        "notes": "",
    },
    "3690": {
        "gics_sector": "Industrials",
        "gics_industry": "Electrical Equipment",
        "confidence": "medium",
        "notes": "",
    },
    "3585": {
        "gics_sector": "Industrials",
        "gics_industry": "Building Products",
        "confidence": "high",
        "notes": "",
    },
    "3430": {
        "gics_sector": "Industrials",
        "gics_industry": "Building Products",
        "confidence": "high",
        "notes": "",
    },
    "3822": {
        "gics_sector": "Industrials",
        "gics_industry": "Building Products",
        "confidence": "high",
        "notes": "",
    },
    "3823": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Instruments",
        "confidence": "medium",
        "notes": "",
    },
    "3825": {
        "gics_sector": "Industrials",
        "gics_industry": "Electronic Equipment",
        "confidence": "medium",
        "notes": "",
    },
    "3829": {
        "gics_sector": "Industrials",
        "gics_industry": "Industrial Instruments",
        "confidence": "medium",
        "notes": "",
    },
    "4011": {
        "gics_sector": "Industrials",
        "gics_industry": "Railroads",
        "confidence": "high",
        "notes": "",
    },
    "4210": {
        "gics_sector": "Industrials",
        "gics_industry": "Air Freight & Logistics",
        "confidence": "high",
        "notes": "",
    },
    "4213": {
        "gics_sector": "Industrials",
        "gics_industry": "Ground Transportation",
        "confidence": "high",
        "notes": "",
    },
    "4513": {
        "gics_sector": "Industrials",
        "gics_industry": "Air Freight & Logistics",
        "confidence": "high",
        "notes": "",
    },
    "4731": {
        "gics_sector": "Industrials",
        "gics_industry": "Air Freight & Logistics",
        "confidence": "high",
        "notes": "",
    },
    "4512": {
        "gics_sector": "Industrials",
        "gics_industry": "Airlines",
        "confidence": "high",
        "notes": "",
    },
    "1731": {
        "gics_sector": "Industrials",
        "gics_industry": "Construction & Engineering",
        "confidence": "high",
        "notes": "",
    },
    "1600": {
        "gics_sector": "Industrials",
        "gics_industry": "Construction & Engineering",
        "confidence": "high",
        "notes": "",
    },
    "7320": {
        "gics_sector": "Industrials",
        "gics_industry": "Research & Consulting",
        "confidence": "high",
        "notes": "",
    },
    "8700": {
        "gics_sector": "Industrials",
        "gics_industry": "Professional Services",
        "confidence": "high",
        "notes": "",
    },
    "8711": {
        "gics_sector": "Industrials",
        "gics_industry": "Construction & Engineering",
        "confidence": "high",
        "notes": "",
    },
    "5000": {
        "gics_sector": "Industrials",
        "gics_industry": "Trading Companies",
        "confidence": "high",
        "notes": "",
    },
    "5065": {
        "gics_sector": "Industrials",
        "gics_industry": "Trading Companies",
        "confidence": "medium",
        "notes": "",
    },
    "7350": {
        "gics_sector": "Industrials",
        "gics_industry": "Trading Companies",
        "confidence": "high",
        "notes": "",
    },
    "7359": {
        "gics_sector": "Industrials",
        "gics_industry": "Trading Companies",
        "confidence": "high",
        "notes": "",
    },
    "7381": {
        "gics_sector": "Industrials",
        "gics_industry": "Commercial Services",
        "confidence": "high",
        "notes": "",
    },
    "4953": {
        "gics_sector": "Industrials",
        "gics_industry": "Waste Management",
        "confidence": "high",
        "notes": "",
    },
    # Consumer Discretionary
    "5331": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Broadline Retail",
        "confidence": "high",
        "notes": "",
    },
    "5961": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Internet Retail",
        "confidence": "high",
        "notes": "",
    },
    "5211": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Home Improvement",
        "confidence": "high",
        "notes": "",
    },
    "5200": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Home Improvement",
        "confidence": "high",
        "notes": "",
    },
    "5651": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Apparel Retail",
        "confidence": "high",
        "notes": "",
    },
    "5700": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Home Furnishing Retail",
        "confidence": "high",
        "notes": "",
    },
    "5731": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Computer & Electronics Retail",
        "confidence": "high",
        "notes": "",
    },
    "5900": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Specialty Retail",
        "confidence": "high",
        "notes": "",
    },
    "5990": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Specialty Retail",
        "confidence": "high",
        "notes": "",
    },
    "5500": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Automotive Retail",
        "confidence": "high",
        "notes": "",
    },
    "5531": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Automotive Retail",
        "confidence": "high",
        "notes": "",
    },
    "5090": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Specialty Retail",
        "confidence": "high",
        "notes": "",
    },
    "3711": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Automobiles",
        "confidence": "high",
        "notes": "",
    },
    "3714": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Auto Parts",
        "confidence": "high",
        "notes": "",
    },
    "7011": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Hotels & Resorts",
        "confidence": "high",
        "notes": "",
    },
    "5812": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Restaurants",
        "confidence": "high",
        "notes": "",
    },
    "5810": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Restaurants",
        "confidence": "high",
        "notes": "",
    },
    "7948": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Casinos & Gaming",
        "confidence": "high",
        "notes": "",
    },
    "7990": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Casinos & Gaming",
        "confidence": "medium",
        "notes": "",
    },
    "4400": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Hotels & Resorts",
        "confidence": "high",
        "notes": "",
    },
    "4700": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Hotels & Resorts",
        "confidence": "high",
        "notes": "",
    },
    "7510": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Rental & Leasing",
        "confidence": "high",
        "notes": "",
    },
    "7340": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Hotels & Resorts",
        "confidence": "medium",
        "notes": "",
    },
    "1531": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Homebuilding",
        "confidence": "high",
        "notes": "",
    },
    "1520": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Homebuilding",
        "confidence": "high",
        "notes": "",
    },
    "2300": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Apparel",
        "confidence": "high",
        "notes": "",
    },
    "2320": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Apparel",
        "confidence": "medium",
        "notes": "",
    },
    "3021": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Footwear",
        "confidence": "high",
        "notes": "",
    },
    "3100": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Apparel & Luxury",
        "confidence": "high",
        "notes": "",
    },
    "3630": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Household Durables",
        "confidence": "high",
        "notes": "",
    },
    "3942": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Leisure Products",
        "confidence": "high",
        "notes": "",
    },
    "3944": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Leisure Products",
        "confidence": "high",
        "notes": "",
    },
    "8200": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Education Services",
        "confidence": "high",
        "notes": "",
    },
    "5013": {
        "gics_sector": "Consumer Discretionary",
        "gics_industry": "Auto Parts",
        "confidence": "high",
        "notes": "",
    },
    # Consumer Staples
    "2080": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Beverages",
        "confidence": "high",
        "notes": "",
    },
    "2086": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Soft Drinks",
        "confidence": "high",
        "notes": "",
    },
    "2082": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Brewers",
        "confidence": "high",
        "notes": "",
    },
    "2000": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Packaged Foods",
        "confidence": "high",
        "notes": "",
    },
    "2011": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Packaged Foods",
        "confidence": "high",
        "notes": "",
    },
    "2015": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Packaged Foods",
        "confidence": "high",
        "notes": "",
    },
    "2030": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Packaged Foods",
        "confidence": "high",
        "notes": "",
    },
    "2033": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Packaged Foods",
        "confidence": "high",
        "notes": "",
    },
    "2040": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Packaged Foods",
        "confidence": "high",
        "notes": "",
    },
    "2060": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Packaged Foods",
        "confidence": "high",
        "notes": "",
    },
    "2070": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Agricultural Products",
        "confidence": "high",
        "notes": "",
    },
    "2090": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Packaged Foods",
        "confidence": "high",
        "notes": "",
    },
    "2111": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Tobacco",
        "confidence": "high",
        "notes": "",
    },
    "2840": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Household Products",
        "confidence": "high",
        "notes": "",
    },
    "2842": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Household Products",
        "confidence": "high",
        "notes": "",
    },
    "2844": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Personal Products",
        "confidence": "high",
        "notes": "",
    },
    "5411": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Food Retail",
        "confidence": "high",
        "notes": "",
    },
    "5140": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Food Distributors",
        "confidence": "high",
        "notes": "",
    },
    "5912": {
        "gics_sector": "Consumer Staples",
        "gics_industry": "Drug Retail",
        "confidence": "high",
        "notes": "",
    },
}


# Text patterns for fallback matching when SIC code not in dictionary
SIC_DESCRIPTION_PATTERNS: dict[str, str] = {
    # Information Technology
    "computer": "Information Technology",
    "software": "Information Technology",
    "semiconductor": "Information Technology",
    "data processing": "Information Technology",
    "programming": "Information Technology",
    # Health Care
    "pharmaceutical": "Health Care",
    "medical": "Health Care",
    "hospital": "Health Care",
    "health": "Health Care",
    "biotechnology": "Health Care",
    "surgical": "Health Care",
    "diagnostic": "Health Care",
    # Financials
    "bank": "Financials",
    "insurance": "Financials",
    "investment": "Financials",
    "financial": "Financials",
    "securities": "Financials",
    "credit": "Financials",
    # Consumer Discretionary
    "retail": "Consumer Discretionary",
    "restaurant": "Consumer Discretionary",
    "hotel": "Consumer Discretionary",
    "automobile": "Consumer Discretionary",
    "apparel": "Consumer Discretionary",
    "motor vehicle": "Consumer Discretionary",
    # Consumer Staples
    "food": "Consumer Staples",
    "beverage": "Consumer Staples",
    "tobacco": "Consumer Staples",
    "grocery": "Consumer Staples",
    # Industrials
    "machinery": "Industrials",
    "aerospace": "Industrials",
    "defense": "Industrials",
    "construction": "Industrials",
    "transportation": "Industrials",
    "engineering": "Industrials",
    "aircraft": "Industrials",
    "railroad": "Industrials",
    "trucking": "Industrials",
    # Energy
    "oil": "Energy",
    "gas": "Energy",
    "petroleum": "Energy",
    "crude": "Energy",
    # Materials
    "chemical": "Materials",
    "mining": "Materials",
    "metal": "Materials",
    "paper": "Materials",
    "steel": "Materials",
    # Utilities
    "electric": "Utilities",
    "utility": "Utilities",
    "water supply": "Utilities",
    # Communication Services
    "telecommunication": "Communication Services",
    "broadcasting": "Communication Services",
    "television": "Communication Services",
    "cable": "Communication Services",
    "telephone": "Communication Services",
    # Real Estate
    "real estate": "Real Estate",
    "reit": "Real Estate",
}


def _get_mapping(sic_code: str) -> GICSMapping | None:
    """Get mapping for an SIC code from DB or fallback dict.

    Order of lookup:
    1. In-memory cache (if loaded from DB)
    2. Database query (with LRU cache)
    3. Fallback dictionary
    """
    # Check in-memory cache first
    if _db_mappings_cache is not None and sic_code in _db_mappings_cache:
        return _db_mappings_cache[sic_code]

    # Try database lookup
    mapping = _get_mapping_from_db(sic_code)
    if mapping:
        return mapping

    # Fall back to dictionary
    return SIC_TO_GICS_FALLBACK.get(sic_code)


def map_sic_to_gics(sic_code: str | None, sic_description: str | None = None) -> str:
    """Map SIC code to GICS sector.

    Args:
        sic_code: SIC code (e.g., "7372")
        sic_description: SIC description for fallback matching (optional)

    Returns:
        GICS sector name, or "Other" if no mapping found
    """
    # Try exact SIC code match
    if sic_code:
        mapping = _get_mapping(sic_code)
        if mapping:
            return mapping["gics_sector"]

    # Try description pattern match as fallback
    if sic_description:
        desc_lower = sic_description.lower()
        for pattern, sector in SIC_DESCRIPTION_PATTERNS.items():
            if pattern in desc_lower:
                return sector

    return "Other"


def get_sic_mapping(sic_code: str) -> GICSMapping | None:
    """Get full GICS mapping details for an SIC code.

    Args:
        sic_code: SIC code (e.g., "7372")

    Returns:
        GICSMapping dict with sector, industry, confidence, notes, or None
    """
    return _get_mapping(sic_code)


def get_sic_industry(sic_code: str | None, sic_description: str | None = None) -> str:
    """Get GICS industry for an SIC code.

    Args:
        sic_code: SIC code (e.g., "7372")
        sic_description: SIC description for fallback (optional)

    Returns:
        GICS industry name, or "Unclassified" if no mapping found
    """
    if sic_code:
        mapping = _get_mapping(sic_code)
        if mapping:
            return mapping["gics_industry"]

    return "Unclassified"
