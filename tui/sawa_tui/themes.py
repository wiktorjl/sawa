"""Theme definitions for the TUI."""

from dataclasses import dataclass

from sawa_tui.config import get_tui_config


@dataclass
class Theme:
    """Color theme for the TUI."""

    name: str

    # Primary colors
    primary: str  # Main accent color (headers, selected items)
    secondary: str  # Secondary accent
    accent: str  # Highlights

    # Text colors
    text: str  # Normal text
    text_muted: str  # Dimmed/secondary text
    text_bright: str  # Emphasized text

    # Status colors
    positive: str  # Green - gains, success
    negative: str  # Red - losses, errors
    warning: str  # Yellow - warnings
    info: str  # Cyan - info

    # UI elements
    border: str  # Panel borders
    border_focus: str  # Focused panel borders
    header: str  # Header backgrounds
    selected: str  # Selected item background
    selected_text: str  # Selected item text


# Theme definitions
THEMES = {
    "default": Theme(
        name="default",
        primary="green",
        secondary="blue",
        accent="cyan",
        text="white",
        text_muted="dim",
        text_bright="bold white",
        positive="green",
        negative="red",
        warning="yellow",
        info="cyan",
        border="green",
        border_focus="bright_green",
        header="bold yellow",
        selected="on dark_green",
        selected_text="bold white",
    ),
    "osaka-jade": Theme(
        name="osaka-jade",
        primary="bright_green",
        secondary="dark_green",
        accent="cyan",
        text="white",
        text_muted="bright_black",
        text_bright="bold bright_white",
        positive="bright_green",
        negative="red",
        warning="yellow",
        info="bright_cyan",
        border="green",
        border_focus="bright_green",
        header="bold bright_green",
        selected="on dark_green",
        selected_text="bold bright_white",
    ),
    "mono": Theme(
        name="mono",
        primary="white",
        secondary="bright_black",
        accent="white",
        text="white",
        text_muted="bright_black",
        text_bright="bold white",
        positive="white",
        negative="white",
        warning="white",
        info="white",
        border="white",
        border_focus="bold white",
        header="bold white",
        selected="on bright_black",
        selected_text="bold white",
    ),
    "high-contrast": Theme(
        name="high-contrast",
        primary="bright_yellow",
        secondary="bright_cyan",
        accent="bright_magenta",
        text="bright_white",
        text_muted="white",
        text_bright="bold bright_white",
        positive="bright_green",
        negative="bright_red",
        warning="bright_yellow",
        info="bright_cyan",
        border="bright_yellow",
        border_focus="bold bright_yellow",
        header="bold bright_yellow",
        selected="on blue",
        selected_text="bold bright_white",
    ),
    # Popular terminal themes
    "dracula": Theme(
        name="dracula",
        primary="#bd93f9",  # Purple
        secondary="#ff79c6",  # Pink
        accent="#8be9fd",  # Cyan
        text="#f8f8f2",  # Foreground
        text_muted="#6272a4",  # Comment
        text_bright="bold #f8f8f2",
        positive="#50fa7b",  # Green
        negative="#ff5555",  # Red
        warning="#f1fa8c",  # Yellow
        info="#8be9fd",  # Cyan
        border="#bd93f9",  # Purple
        border_focus="#ff79c6",  # Pink
        header="bold #bd93f9",
        selected="on #44475a",  # Selection
        selected_text="bold #f8f8f2",
    ),
    "catppuccin": Theme(
        name="catppuccin",
        primary="#cba6f7",  # Mauve
        secondary="#89b4fa",  # Blue
        accent="#f5c2e7",  # Pink
        text="#cdd6f4",  # Text
        text_muted="#6c7086",  # Overlay0
        text_bright="bold #cdd6f4",
        positive="#a6e3a1",  # Green
        negative="#f38ba8",  # Red
        warning="#f9e2af",  # Yellow
        info="#89dceb",  # Sky
        border="#cba6f7",  # Mauve
        border_focus="#f5c2e7",  # Pink
        header="bold #cba6f7",
        selected="on #45475a",  # Surface1
        selected_text="bold #cdd6f4",
    ),
    "gruvbox": Theme(
        name="gruvbox",
        primary="#fabd2f",  # Yellow
        secondary="#83a598",  # Aqua
        accent="#d3869b",  # Purple
        text="#ebdbb2",  # Light0
        text_muted="#928374",  # Gray
        text_bright="bold #fbf1c7",
        positive="#b8bb26",  # Green
        negative="#fb4934",  # Red
        warning="#fabd2f",  # Yellow
        info="#83a598",  # Aqua
        border="#fabd2f",  # Yellow
        border_focus="#fe8019",  # Orange
        header="bold #fabd2f",
        selected="on #3c3836",  # Bg1
        selected_text="bold #fbf1c7",
    ),
    "nord": Theme(
        name="nord",
        primary="#88c0d0",  # Frost
        secondary="#81a1c1",  # Frost blue
        accent="#b48ead",  # Aurora purple
        text="#eceff4",  # Snow Storm
        text_muted="#4c566a",  # Polar Night
        text_bright="bold #eceff4",
        positive="#a3be8c",  # Aurora green
        negative="#bf616a",  # Aurora red
        warning="#ebcb8b",  # Aurora yellow
        info="#88c0d0",  # Frost
        border="#88c0d0",
        border_focus="#81a1c1",
        header="bold #88c0d0",
        selected="on #3b4252",  # Polar Night
        selected_text="bold #eceff4",
    ),
    "tokyo-night": Theme(
        name="tokyo-night",
        primary="#7aa2f7",  # Blue
        secondary="#bb9af7",  # Purple
        accent="#7dcfff",  # Cyan
        text="#c0caf5",  # Foreground
        text_muted="#565f89",  # Comment
        text_bright="bold #c0caf5",
        positive="#9ece6a",  # Green
        negative="#f7768e",  # Red
        warning="#e0af68",  # Yellow
        info="#7dcfff",  # Cyan
        border="#7aa2f7",
        border_focus="#bb9af7",
        header="bold #7aa2f7",
        selected="on #24283b",  # Bg highlight
        selected_text="bold #c0caf5",
    ),
    "solarized": Theme(
        name="solarized",
        primary="#268bd2",  # Blue
        secondary="#2aa198",  # Cyan
        accent="#6c71c4",  # Violet
        text="#839496",  # Base0
        text_muted="#586e75",  # Base01
        text_bright="bold #93a1a1",  # Base1
        positive="#859900",  # Green
        negative="#dc322f",  # Red
        warning="#b58900",  # Yellow
        info="#2aa198",  # Cyan
        border="#268bd2",
        border_focus="#2aa198",
        header="bold #268bd2",
        selected="on #073642",  # Base02
        selected_text="bold #fdf6e3",
    ),
    "one-dark": Theme(
        name="one-dark",
        primary="#61afef",  # Blue
        secondary="#c678dd",  # Purple
        accent="#56b6c2",  # Cyan
        text="#abb2bf",  # Foreground
        text_muted="#5c6370",  # Comment
        text_bright="bold #abb2bf",
        positive="#98c379",  # Green
        negative="#e06c75",  # Red
        warning="#e5c07b",  # Yellow
        info="#56b6c2",  # Cyan
        border="#61afef",
        border_focus="#c678dd",
        header="bold #61afef",
        selected="on #3e4451",  # Visual
        selected_text="bold #abb2bf",
    ),
}


def get_theme() -> Theme:
    """Get the current theme based on config."""
    config = get_tui_config()
    theme_name = config.get("theme", "name", "default")
    return THEMES.get(theme_name, THEMES["default"])
