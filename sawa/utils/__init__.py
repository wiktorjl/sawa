"""Shared utilities for S&P 500 tools."""

from .config import (
    get_database_url,
    get_env,
    get_massive_api_key,
    get_polygon_api_key,
    get_polygon_s3_credentials,
)
from .csv_utils import append_csv, get_existing_keys
from .dates import (
    DATE_FORMAT,
    DEFAULT_YEARS,
    calculate_date_range,
    parse_date,
    timestamp_to_date,
)
from .logging import setup_logging
from .sic_mapping import (
    clear_cache as clear_sic_cache,
)
from .sic_mapping import (
    get_sic_industry,
    get_sic_mapping,
    map_sic_to_gics,
)
from .sic_mapping import (
    load_mappings_from_db as load_sic_mappings,
)
from .symbols import (
    fetch_index_symbols,
    fetch_nasdaq100_symbols,
    fetch_sp500_symbols,
    load_symbols,
    validate_ticker,
)
from .xdg import (
    ensure_dirs,
    get_cache_dir,
    get_config_dir,
    get_config_file,
    get_data_dir,
    get_log_file,
    get_state_dir,
    load_config,
    load_toml,
    save_config,
    save_toml,
)

__all__ = [
    # config
    "get_env",
    "get_polygon_api_key",
    "get_polygon_s3_credentials",
    "get_massive_api_key",
    "get_database_url",
    # csv_utils
    "get_existing_keys",
    "append_csv",
    # dates
    "DATE_FORMAT",
    "DEFAULT_YEARS",
    "parse_date",
    "calculate_date_range",
    "timestamp_to_date",
    # logging
    "setup_logging",
    # sic_mapping
    "map_sic_to_gics",
    "get_sic_mapping",
    "get_sic_industry",
    "load_sic_mappings",
    "clear_sic_cache",
    # symbols
    "validate_ticker",
    "load_symbols",
    "fetch_sp500_symbols",
    "fetch_nasdaq100_symbols",
    "fetch_index_symbols",
    # xdg
    "get_config_dir",
    "get_data_dir",
    "get_cache_dir",
    "get_state_dir",
    "get_config_file",
    "get_log_file",
    "ensure_dirs",
    "load_toml",
    "save_toml",
    "load_config",
    "save_config",
]
