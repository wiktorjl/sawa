"""XDG Base Directory Specification utilities.

Provides standardized paths for configuration, data, cache, and state files
following the XDG Base Directory Specification.

Typical paths:
    - Config: ~/.config/sp500-tools/config.toml
    - Data: ~/.local/share/sp500-tools/
    - Cache: ~/.cache/sp500-tools/
    - State: ~/.local/state/sp500-tools/
"""

import os
import sys
from pathlib import Path
from typing import Any

# Try to import tomllib (Python 3.11+) or tomli
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None  # type: ignore

try:
    import tomli_w
except ImportError:
    tomli_w = None  # type: ignore


# Default application name
DEFAULT_APP_NAME = "sp500-tools"


def get_config_dir(app_name: str = DEFAULT_APP_NAME) -> Path:
    """
    Get XDG config directory.

    Uses $XDG_CONFIG_HOME if set, otherwise ~/.config.

    Args:
        app_name: Application name for subdirectory

    Returns:
        Path to config directory (e.g., ~/.config/sp500-tools)
    """
    base = os.environ.get("XDG_CONFIG_HOME")
    if base:
        return Path(base) / app_name
    return Path.home() / ".config" / app_name


def get_data_dir(app_name: str = DEFAULT_APP_NAME) -> Path:
    """
    Get XDG data directory.

    Uses $XDG_DATA_HOME if set, otherwise ~/.local/share.

    Args:
        app_name: Application name for subdirectory

    Returns:
        Path to data directory (e.g., ~/.local/share/sp500-tools)
    """
    base = os.environ.get("XDG_DATA_HOME")
    if base:
        return Path(base) / app_name
    return Path.home() / ".local" / "share" / app_name


def get_cache_dir(app_name: str = DEFAULT_APP_NAME) -> Path:
    """
    Get XDG cache directory.

    Uses $XDG_CACHE_HOME if set, otherwise ~/.cache.

    Args:
        app_name: Application name for subdirectory

    Returns:
        Path to cache directory (e.g., ~/.cache/sp500-tools)
    """
    base = os.environ.get("XDG_CACHE_HOME")
    if base:
        return Path(base) / app_name
    return Path.home() / ".cache" / app_name


def get_state_dir(app_name: str = DEFAULT_APP_NAME) -> Path:
    """
    Get XDG state directory.

    Uses $XDG_STATE_HOME if set, otherwise ~/.local/state.

    Args:
        app_name: Application name for subdirectory

    Returns:
        Path to state directory (e.g., ~/.local/state/sp500-tools)
    """
    base = os.environ.get("XDG_STATE_HOME")
    if base:
        return Path(base) / app_name
    return Path.home() / ".local" / "state" / app_name


def get_config_file(app_name: str = DEFAULT_APP_NAME, filename: str = "config.toml") -> Path:
    """
    Get path to main config file.

    Args:
        app_name: Application name for subdirectory
        filename: Config filename (default: config.toml)

    Returns:
        Path to config file (e.g., ~/.config/sp500-tools/config.toml)
    """
    return get_config_dir(app_name) / filename


def get_log_file(app_name: str = DEFAULT_APP_NAME, filename: str = "app.log") -> Path:
    """
    Get path to log file in state directory.

    Args:
        app_name: Application name for subdirectory
        filename: Log filename (default: app.log)

    Returns:
        Path to log file (e.g., ~/.local/state/sp500-tools/app.log)
    """
    return get_state_dir(app_name) / filename


def ensure_dirs(app_name: str = DEFAULT_APP_NAME) -> dict[str, Path]:
    """
    Ensure all XDG directories exist for the application.

    Creates config, data, cache, and state directories if they don't exist.

    Args:
        app_name: Application name for subdirectories

    Returns:
        Dictionary with paths to all created directories
    """
    dirs = {
        "config": get_config_dir(app_name),
        "data": get_data_dir(app_name),
        "cache": get_cache_dir(app_name),
        "state": get_state_dir(app_name),
    }

    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)

    return dirs


def load_toml(path: Path) -> dict[str, Any]:
    """
    Load a TOML configuration file.

    Args:
        path: Path to TOML file

    Returns:
        Parsed configuration dictionary

    Raises:
        FileNotFoundError: If file doesn't exist
        ImportError: If tomllib/tomli is not available
    """
    if tomllib is None:
        raise ImportError(
            "TOML parsing requires Python 3.11+ or 'tomli' package. Install with: pip install tomli"
        )

    with open(path, "rb") as f:
        return tomllib.load(f)


def save_toml(path: Path, data: dict[str, Any]) -> None:
    """
    Save a dictionary to a TOML configuration file.

    Args:
        path: Path to TOML file
        data: Configuration dictionary to save

    Raises:
        ImportError: If tomli_w is not available
    """
    if tomli_w is None:
        raise ImportError(
            "TOML writing requires 'tomli-w' package. Install with: pip install tomli-w"
        )

    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "wb") as f:
        tomli_w.dump(data, f)


def load_config(
    app_name: str = DEFAULT_APP_NAME,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Load application configuration from XDG config file.

    If the config file doesn't exist, returns defaults.
    Merges loaded config with defaults (loaded values take precedence).

    Args:
        app_name: Application name for config directory
        defaults: Default configuration values

    Returns:
        Configuration dictionary
    """
    config_path = get_config_file(app_name)
    result = dict(defaults) if defaults else {}

    if config_path.exists():
        try:
            loaded = load_toml(config_path)
            # Deep merge loaded config into defaults
            _deep_merge(result, loaded)
        except (ImportError, OSError):
            pass  # Fall back to defaults

    return result


def save_config(
    config: dict[str, Any],
    app_name: str = DEFAULT_APP_NAME,
) -> None:
    """
    Save application configuration to XDG config file.

    Args:
        config: Configuration dictionary to save
        app_name: Application name for config directory
    """
    config_path = get_config_file(app_name)
    ensure_dirs(app_name)
    save_toml(config_path, config)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> None:
    """
    Deep merge override dictionary into base dictionary (in-place).

    Args:
        base: Base dictionary to merge into
        override: Dictionary with values to override
    """
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
