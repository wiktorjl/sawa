"""Gauge/progress bar widget."""

from ..themes import Theme, get_theme


class Gauge:
    """
    Render progress bars and gauge indicators.

    Useful for showing ratios, percentages, and relative values.
    """

    def __init__(self, theme: Theme | None = None):
        """
        Initialize gauge renderer.

        Args:
            theme: Theme to use for styling
        """
        self.theme = theme or get_theme()

    def render(
        self,
        value: float,
        width: int = 20,
        min_val: float = 0.0,
        max_val: float = 1.0,
        show_percent: bool = True,
        label: str | None = None,
        colorize: bool = True,
    ) -> str:
        """
        Render a horizontal gauge/progress bar.

        Args:
            value: Current value
            width: Bar width in characters
            min_val: Minimum value
            max_val: Maximum value
            show_percent: Show percentage label
            label: Optional label before bar
            colorize: Apply colors

        Returns:
            Gauge string
        """
        # Normalize value to 0-1 range
        if max_val == min_val:
            normalized = 0.5
        else:
            normalized = (value - min_val) / (max_val - min_val)
            normalized = max(0.0, min(1.0, normalized))

        # Calculate filled portion
        filled = int(normalized * width)
        empty = width - filled

        # Build bar using block characters
        fill_char = "\u2588"  # Full block
        empty_char = "\u2591"  # Light shade

        bar = fill_char * filled + empty_char * empty

        # Colorize
        if colorize:
            # Use gradient from negative to positive based on fill level
            if normalized < 0.3:
                color = self.theme.colors.negative
            elif normalized < 0.7:
                color = self.theme.colors.warning
            else:
                color = self.theme.colors.positive

            bar = self.theme.colorize(bar, color)

        # Add label and percentage
        parts = []
        if label:
            parts.append(label)
        parts.append(f"[{bar}]")
        if show_percent:
            pct = f"{normalized * 100:.0f}%"
            parts.append(pct)

        return " ".join(parts)

    def render_comparison(
        self,
        value1: float,
        value2: float,
        width: int = 20,
        label1: str = "",
        label2: str = "",
    ) -> str:
        """
        Render two values as side-by-side comparison bars.

        Args:
            value1: First value
            value2: Second value
            width: Total width for both bars
            label1: Label for first bar
            label2: Label for second bar

        Returns:
            Comparison gauge string
        """
        max_val = max(value1, value2, 1)
        bar_width = (width - 3) // 2  # -3 for separator

        bar1 = self.render(value1, bar_width, 0, max_val, show_percent=False, colorize=True)
        bar2 = self.render(value2, bar_width, 0, max_val, show_percent=False, colorize=True)

        lines = []
        if label1 or label2:
            lines.append(f"{label1:<{bar_width}} | {label2:<{bar_width}}")
        lines.append(f"{bar1} | {bar2}")

        return "\n".join(lines)

    def render_multi(
        self,
        values: list[tuple[str, float]],
        width: int = 30,
        max_val: float | None = None,
    ) -> str:
        """
        Render multiple labeled gauges stacked vertically.

        Args:
            values: List of (label, value) tuples
            width: Total width
            max_val: Maximum value for normalization (None = use max from values)

        Returns:
            Multi-gauge string
        """
        if not values:
            return ""

        # Find longest label
        max_label = max(len(label) for label, _ in values)
        bar_width = width - max_label - 10  # Space for label, bar, and percentage

        # Find max value if not specified
        if max_val is None:
            max_val = max(val for _, val in values)

        lines = []
        for label, value in values:
            bar = self.render(
                value,
                bar_width,
                0,
                max_val,
                show_percent=True,
                colorize=True,
            )
            lines.append(f"{label:<{max_label}} {bar}")

        return "\n".join(lines)

    def render_health_indicator(
        self,
        value: float,
        thresholds: tuple[float, float] = (0.3, 0.7),
        labels: tuple[str, str, str] = ("Low", "Moderate", "High"),
    ) -> str:
        """
        Render a health indicator with status label.

        Args:
            value: Value (0-1 range)
            thresholds: (low_threshold, high_threshold)
            labels: (low_label, moderate_label, high_label)

        Returns:
            Health indicator string with icon and label
        """
        low_thresh, high_thresh = thresholds
        low_label, mod_label, high_label = labels

        if value < low_thresh:
            symbol = self.theme.symbols.cross
            color = self.theme.colors.negative
            label = low_label
        elif value < high_thresh:
            symbol = self.theme.symbols.warning
            color = self.theme.colors.warning
            label = mod_label
        else:
            symbol = self.theme.symbols.check
            color = self.theme.colors.positive
            label = high_label

        indicator = f"{symbol} {label}"
        return self.theme.colorize(indicator, color)
