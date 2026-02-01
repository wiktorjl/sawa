"""Standardized CLI patterns."""

import argparse

from .dates import parse_date


def create_parser(description: str, epilog: str = "") -> argparse.ArgumentParser:
    """
    Create parser with consistent formatting.

    Args:
        description: Help text description
        epilog: Additional help text after options

    Returns:
        Configured ArgumentParser
    """
    return argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """
    Add common arguments (verbose, continue).

    Args:
        parser: ArgumentParser to modify
    """
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging"
    )
    parser.add_argument(
        "--continue",
        dest="continue_mode",
        action="store_true",
        help="Resume interrupted operation",
    )


def add_date_args(parser: argparse.ArgumentParser, default_years: int = 5) -> None:
    """
    Add date range arguments.

    Args:
        parser: ArgumentParser to modify
        default_years: Default years for --years option
    """
    parser.add_argument(
        "--start-date", type=parse_date, metavar="YYYY-MM-DD", help="Start date"
    )
    parser.add_argument(
        "--end-date", type=parse_date, metavar="YYYY-MM-DD", help="End date"
    )
    parser.add_argument(
        "--years",
        type=int,
        metavar="N",
        default=default_years,
        help=f"Years back from today (default: {default_years})",
    )


def add_api_key_arg(parser: argparse.ArgumentParser, env_var: str) -> None:
    """
    Add API key argument.

    Args:
        parser: ArgumentParser to modify
        env_var: Environment variable name for default
    """
    parser.add_argument("--api-key", help=f"API key (overrides {env_var})")
