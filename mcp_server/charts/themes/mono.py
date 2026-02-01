"""Monochrome theme - high contrast black and white."""

from .base import BoxChars, ColorPalette, Symbols, Theme


class MonoTheme(Theme):
    """
    Monochrome theme with high contrast.

    Uses only black, white, and gray tones. Good for terminals
    without color support or for accessibility.
    """

    @property
    def name(self) -> str:
        return "mono"

    @property
    def colors(self) -> ColorPalette:
        return ColorPalette(
            # Primary tones (all grayscale)
            primary=(255, 255, 255),  # White
            secondary=(200, 200, 200),  # Light gray
            # Semantic colors (distinguished by intensity)
            positive=(255, 255, 255),  # White (bold will differentiate)
            negative=(200, 200, 200),  # Light gray
            warning=(220, 220, 220),  # Off-white
            info=(180, 180, 180),  # Medium gray
            # Text colors
            text=(255, 255, 255),  # White
            muted=(128, 128, 128),  # Gray
            # UI elements
            border=(200, 200, 200),  # Light gray
            highlight=(255, 255, 255),  # White
            # Chart-specific
            sparkline=(200, 200, 200),  # Light gray
            bar_fill=(255, 255, 255),  # White
            bar_empty=(64, 64, 64),  # Dark gray
        )

    @property
    def symbols(self) -> Symbols:
        return Symbols(
            # Trend indicators (ASCII compatible)
            positive="+",
            negative="-",
            neutral="=",
            # Bullets and markers
            bullet="*",
            empty_bullet="o",
            # Status icons
            check="[+]",
            cross="[x]",
            warning="[!]",
            star="[*]",
            # Arrows
            arrow_up="^",
            arrow_down="v",
            arrow_right=">",
            arrow_left="<",
            # Sparkline characters (ASCII compatible, 8 levels)
            sparkline="_.-:=+#@",
        )

    @property
    def box(self) -> BoxChars:
        return BoxChars(
            # Heavy box (ASCII compatible)
            top_left="+",
            top_right="+",
            bottom_left="+",
            bottom_right="+",
            horizontal="-",
            vertical="|",
            cross="+",
            t_down="+",
            t_up="+",
            t_right="+",
            t_left="+",
            # Light box
            light_h="-",
            light_v="|",
            light_tl="+",
            light_tr="+",
            light_bl="+",
            light_br="+",
        )
