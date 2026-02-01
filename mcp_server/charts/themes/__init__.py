"""Theme system for charts."""

from .base import Theme
from .mono import MonoTheme
from .osaka_jade import OsakaJadeTheme

# Theme registry
_THEMES: dict[str, type[Theme]] = {
    "osaka-jade": OsakaJadeTheme,
    "mono": MonoTheme,
}

# Default theme
DEFAULT_THEME = "osaka-jade"


def get_theme(name: str | None = None) -> Theme:
    """
    Get a theme by name.

    Args:
        name: Theme name. If None, returns default theme.

    Returns:
        Theme instance

    Raises:
        ValueError: If theme name is not found
    """
    if name is None:
        name = DEFAULT_THEME

    name = name.lower()

    if name not in _THEMES:
        available = ", ".join(_THEMES.keys())
        raise ValueError(f"Unknown theme: {name}. Available themes: {available}")

    return _THEMES[name]()


def list_themes() -> list[str]:
    """
    Get list of available theme names.

    Returns:
        List of theme names
    """
    return list(_THEMES.keys())


def register_theme(name: str, theme_class: type[Theme]) -> None:
    """
    Register a custom theme.

    Args:
        name: Theme name
        theme_class: Theme class
    """
    _THEMES[name.lower()] = theme_class


__all__ = [
    "Theme",
    "get_theme",
    "list_themes",
    "register_theme",
    "OsakaJadeTheme",
    "MonoTheme",
]
