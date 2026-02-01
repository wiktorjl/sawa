"""Osaka Jade theme - calm, sophisticated jade green aesthetic."""

from .base import BoxChars, ColorPalette, Symbols, Theme


class OsakaJadeTheme(Theme):
    """
    Default theme with jade green tones.

    Inspired by Japanese jade aesthetics - calm greens, soft contrast, elegant.
    """

    @property
    def name(self) -> str:
        return "osaka-jade"

    @property
    def colors(self) -> ColorPalette:
        return ColorPalette(
            # Primary jade tones
            primary=(0, 168, 107),  # Jade green #00A86B
            secondary=(157, 193, 131),  # Sage #9DC183
            # Semantic colors
            positive=(152, 255, 152),  # Mint green #98FF98
            negative=(255, 107, 107),  # Coral red #FF6B6B
            warning=(255, 179, 71),  # Amber #FFB347
            info=(135, 206, 235),  # Sky blue #87CEEB
            # Text colors
            text=(240, 240, 240),  # Pearl white #F0F0F0
            muted=(112, 128, 144),  # Stone gray #708090
            # UI elements
            border=(157, 193, 131),  # Sage #9DC183
            highlight=(255, 215, 0),  # Gold #FFD700
            # Chart-specific
            sparkline=(0, 128, 128),  # Teal #008080
            bar_fill=(0, 168, 107),  # Jade #00A86B
            bar_empty=(64, 64, 64),  # Dark gray
        )

    @property
    def symbols(self) -> Symbols:
        return Symbols(
            # Trend indicators
            positive="\u25b2",  # Black up-pointing triangle
            negative="\u25bc",  # Black down-pointing triangle
            neutral="\u25b6",  # Black right-pointing triangle
            # Bullets and markers
            bullet="\u25cf",  # Black circle
            empty_bullet="\u25cb",  # White circle
            # Status icons
            check="\u2713",  # Check mark
            cross="\u2717",  # Ballot X
            warning="\u26a0",  # Warning sign
            star="\u2605",  # Black star
            # Arrows
            arrow_up="\u2191",  # Upwards arrow
            arrow_down="\u2193",  # Downwards arrow
            arrow_right="\u2192",  # Rightwards arrow
            arrow_left="\u2190",  # Leftwards arrow
            # Sparkline characters (8 levels, low to high)
            sparkline="\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588",
        )

    @property
    def box(self) -> BoxChars:
        return BoxChars(
            # Heavy box (double lines)
            top_left="\u2554",  # Double down and right
            top_right="\u2557",  # Double down and left
            bottom_left="\u255a",  # Double up and right
            bottom_right="\u255d",  # Double up and left
            horizontal="\u2550",  # Double horizontal
            vertical="\u2551",  # Double vertical
            cross="\u256c",  # Double cross
            t_down="\u2566",  # Double down and horizontal
            t_up="\u2569",  # Double up and horizontal
            t_right="\u2560",  # Double vertical and right
            t_left="\u2563",  # Double vertical and left
            # Light box (single lines for inner divisions)
            light_h="\u2500",  # Light horizontal
            light_v="\u2502",  # Light vertical
            light_tl="\u256d",  # Light arc down and right
            light_tr="\u256e",  # Light arc down and left
            light_bl="\u2570",  # Light arc up and right
            light_br="\u256f",  # Light arc up and left
        )
