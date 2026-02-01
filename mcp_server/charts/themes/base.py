"""Base theme class for chart styling."""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class ColorPalette:
    """Color palette for a theme.

    Colors are stored as RGB tuples (r, g, b) for 24-bit color support.
    """

    # Primary colors
    primary: tuple[int, int, int] = (255, 255, 255)
    secondary: tuple[int, int, int] = (200, 200, 200)

    # Semantic colors
    positive: tuple[int, int, int] = (0, 255, 0)
    negative: tuple[int, int, int] = (255, 0, 0)
    warning: tuple[int, int, int] = (255, 255, 0)
    info: tuple[int, int, int] = (0, 255, 255)

    # Text colors
    text: tuple[int, int, int] = (255, 255, 255)
    muted: tuple[int, int, int] = (128, 128, 128)

    # UI elements
    border: tuple[int, int, int] = (200, 200, 200)
    highlight: tuple[int, int, int] = (255, 255, 0)

    # Chart-specific
    sparkline: tuple[int, int, int] = (0, 200, 200)
    bar_fill: tuple[int, int, int] = (100, 200, 100)
    bar_empty: tuple[int, int, int] = (64, 64, 64)


@dataclass
class Symbols:
    """Unicode symbols used in charts."""

    # Trend indicators
    positive: str = "+"
    negative: str = "-"
    neutral: str = "="

    # Bullets and markers
    bullet: str = "*"
    empty_bullet: str = "o"

    # Status icons
    check: str = "+"
    cross: str = "x"
    warning: str = "!"
    star: str = "*"

    # Arrows
    arrow_up: str = "^"
    arrow_down: str = "v"
    arrow_right: str = ">"
    arrow_left: str = "<"

    # Sparkline characters (8 levels, low to high)
    sparkline: str = "_.-:=+#@"


@dataclass
class BoxChars:
    """Box drawing characters."""

    # Heavy box (double lines)
    top_left: str = "+"
    top_right: str = "+"
    bottom_left: str = "+"
    bottom_right: str = "+"
    horizontal: str = "-"
    vertical: str = "|"
    cross: str = "+"
    t_down: str = "+"
    t_up: str = "+"
    t_right: str = "+"
    t_left: str = "+"

    # Light box (single lines)
    light_h: str = "-"
    light_v: str = "|"
    light_tl: str = "+"
    light_tr: str = "+"
    light_bl: str = "+"
    light_br: str = "+"


class Theme(ABC):
    """Abstract base class for chart themes."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Theme name."""
        ...

    @property
    @abstractmethod
    def colors(self) -> ColorPalette:
        """Color palette."""
        ...

    @property
    @abstractmethod
    def symbols(self) -> Symbols:
        """Unicode symbols."""
        ...

    @property
    @abstractmethod
    def box(self) -> BoxChars:
        """Box drawing characters."""
        ...

    def rgb_to_ansi(self, rgb: tuple[int, int, int], foreground: bool = True) -> str:
        """
        Convert RGB tuple to ANSI escape code.

        Args:
            rgb: RGB color tuple (r, g, b)
            foreground: If True, returns foreground color; otherwise background

        Returns:
            ANSI escape sequence
        """
        r, g, b = rgb
        code = 38 if foreground else 48
        return f"\033[{code};2;{r};{g};{b}m"

    def reset(self) -> str:
        """Get ANSI reset code."""
        return "\033[0m"

    def colorize(
        self,
        text: str,
        color: tuple[int, int, int],
        bold: bool = False,
    ) -> str:
        """
        Apply color to text.

        Args:
            text: Text to colorize
            color: RGB color tuple
            bold: Whether to make text bold

        Returns:
            Colorized text with ANSI codes
        """
        codes = []
        if bold:
            codes.append("\033[1m")
        codes.append(self.rgb_to_ansi(color))
        return f"{''.join(codes)}{text}{self.reset()}"

    def positive_text(self, text: str, bold: bool = False) -> str:
        """Apply positive (green) color to text."""
        return self.colorize(text, self.colors.positive, bold)

    def negative_text(self, text: str, bold: bool = False) -> str:
        """Apply negative (red) color to text."""
        return self.colorize(text, self.colors.negative, bold)

    def warning_text(self, text: str, bold: bool = False) -> str:
        """Apply warning (yellow/amber) color to text."""
        return self.colorize(text, self.colors.warning, bold)

    def info_text(self, text: str, bold: bool = False) -> str:
        """Apply info (cyan/blue) color to text."""
        return self.colorize(text, self.colors.info, bold)

    def muted_text(self, text: str) -> str:
        """Apply muted (gray) color to text."""
        return self.colorize(text, self.colors.muted)

    def primary_text(self, text: str, bold: bool = False) -> str:
        """Apply primary theme color to text."""
        return self.colorize(text, self.colors.primary, bold)

    def border_text(self, text: str) -> str:
        """Apply border color to text."""
        return self.colorize(text, self.colors.border)

    def highlight_text(self, text: str, bold: bool = True) -> str:
        """Apply highlight color to text."""
        return self.colorize(text, self.colors.highlight, bold)
