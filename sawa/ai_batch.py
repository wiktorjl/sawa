"""Batch AI generation for company overviews."""

import logging
import os
import time
from typing import Any

from sawa.utils import setup_logging


def run_overview_batch(
    zai_api_key: str,
    database_url: str,
    limit: int = 50,
    delay: float = 2.0,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Generate company overviews for top tickers by market cap.

    Processes tickers that don't already have cached (shared) overviews.

    Args:
        zai_api_key: Z.AI API key for LLM generation
        database_url: PostgreSQL connection URL
        limit: Maximum tickers to process
        delay: Seconds between API calls (rate limiting)
        logger: Logger instance

    Returns:
        Statistics dictionary with:
        - success: True if no errors
        - generated: Number of overviews generated
        - skipped: Number of tickers skipped (no company data)
        - errors: Number of API/generation errors
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False, "generated": 0, "skipped": 0, "errors": 0}

    # Set environment variables for ZAI client and database
    os.environ["ZAI_API_KEY"] = zai_api_key
    os.environ["DATABASE_URL"] = database_url

    # Import TUI modules (they read from environment)
    from sawa_tui.ai.client import ZAIClient, ZAIError
    from sawa_tui.models.overview import OverviewManager
    from sawa_tui.models.queries import StockQueries

    client = ZAIClient()

    if not client.is_configured():
        logger.error("ZAI client not configured - check API key")
        return stats

    # Get tickers needing overviews
    tickers = OverviewManager.get_top_tickers_without_overview(limit)
    logger.info(f"Found {len(tickers)} tickers without cached overviews")

    if not tickers:
        logger.info("No tickers to process")
        stats["success"] = True
        return stats

    for i, ticker in enumerate(tickers, 1):
        logger.info(f"[{i}/{len(tickers)}] Processing {ticker}...")

        try:
            # Get company info
            company = StockQueries.get_company(ticker)
            if not company:
                logger.warning(f"  No company data for {ticker}, skipping")
                stats["skipped"] += 1
                continue

            # Generate overview (no streaming for batch)
            overview = client.generate_company_overview(
                ticker=ticker,
                company_name=company.name,
                sector=company.sector,
            )

            # Save to database as shared (no user_id)
            if OverviewManager.save(overview, user_id=None):
                stats["generated"] += 1
                logger.info(f"  Generated and saved overview for {ticker}")
            else:
                stats["errors"] += 1
                logger.error(f"  Failed to save overview for {ticker}")

            # Rate limit between API calls
            if i < len(tickers):
                time.sleep(delay)

        except ZAIError as e:
            stats["errors"] += 1
            logger.error(f"  API error for {ticker}: {e.message}")
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"  Unexpected error for {ticker}: {e}")

    stats["success"] = stats["errors"] == 0
    logger.info(
        f"\nBatch complete: {stats['generated']} generated, "
        f"{stats['skipped']} skipped, {stats['errors']} errors"
    )

    return stats
