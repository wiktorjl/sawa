"""Table widget for structured data display."""

from typing import Any

from ..core.colors import pad_to_width, truncate_to_width, visible_len
from ..themes import Theme, get_theme


class Table:
    """
    Render data tables with Unicode borders.

    Supports headers, alignment, and automatic column width calculation.
    """

    def __init__(self, theme: Theme | None = None):
        """
        Initialize table renderer.

        Args:
            theme: Theme to use for styling
        """
        self.theme = theme or get_theme()

    def render(
        self,
        headers: list[str],
        rows: list[list[Any]],
        alignments: list[str] | None = None,
        max_col_width: int = 30,
        min_col_width: int = 5,
    ) -> str:
        """
        Render a table with headers and rows.

        Args:
            headers: Column headers
            rows: List of rows (each row is a list of values)
            alignments: List of alignments ('left', 'right', 'center') per column
            max_col_width: Maximum column width
            min_col_width: Minimum column width

        Returns:
            Formatted table string
        """
        if not headers:
            return ""

        num_cols = len(headers)

        # Default alignments
        if alignments is None:
            alignments = ["left"] * num_cols

        # Calculate column widths
        col_widths = self._calculate_widths(headers, rows, max_col_width, min_col_width)

        box = self.theme.box
        lines = []

        # Top border
        top_parts = []
        for width in col_widths:
            top_parts.append(box.horizontal * (width + 2))
        top = box.top_left + box.t_down.join(top_parts) + box.top_right
        lines.append(self.theme.border_text(top))

        # Header row
        header_cells = []
        for i, (header, width) in enumerate(zip(headers, col_widths)):
            cell = self._format_cell(header, width, alignments[i])
            # Make headers bold
            cell = self.theme.primary_text(cell, bold=True)
            header_cells.append(cell)
        header_line = (
            self.theme.border_text(box.vertical)
            + self.theme.border_text(box.vertical).join([f" {cell} " for cell in header_cells])
            + self.theme.border_text(box.vertical)
        )
        lines.append(header_line)

        # Header separator
        sep_parts = []
        for width in col_widths:
            sep_parts.append(box.horizontal * (width + 2))
        sep = box.t_right + box.cross.join(sep_parts) + box.t_left
        lines.append(self.theme.border_text(sep))

        # Data rows
        for row in rows:
            row_cells = []
            for i, (value, width) in enumerate(zip(row, col_widths)):
                align = alignments[i] if i < len(alignments) else "left"
                cell = self._format_cell(str(value), width, align)
                row_cells.append(cell)
            row_line = (
                self.theme.border_text(box.vertical)
                + self.theme.border_text(box.vertical).join([f" {cell} " for cell in row_cells])
                + self.theme.border_text(box.vertical)
            )
            lines.append(row_line)

        # Bottom border
        bottom_parts = []
        for width in col_widths:
            bottom_parts.append(box.horizontal * (width + 2))
        bottom = box.bottom_left + box.t_up.join(bottom_parts) + box.bottom_right
        lines.append(self.theme.border_text(bottom))

        return "\n".join(lines)

    def render_compact(
        self,
        headers: list[str],
        rows: list[list[Any]],
        separator: str = " | ",
    ) -> str:
        """
        Render a compact table without borders.

        Args:
            headers: Column headers
            rows: List of rows
            separator: Column separator

        Returns:
            Formatted table string
        """
        if not headers:
            return ""

        # Calculate column widths
        col_widths = []
        for i, header in enumerate(headers):
            max_width = visible_len(header)
            for row in rows:
                if i < len(row):
                    max_width = max(max_width, visible_len(str(row[i])))
            col_widths.append(max_width)

        lines = []

        # Header
        header_cells = [
            self.theme.primary_text(pad_to_width(h, w), bold=True)
            for h, w in zip(headers, col_widths)
        ]
        lines.append(separator.join(header_cells))

        # Separator line
        sep_line = self.theme.muted_text(separator.join(["-" * w for w in col_widths]))
        lines.append(sep_line)

        # Data rows
        for row in rows:
            cells = []
            for i, (value, width) in enumerate(zip(row, col_widths)):
                cells.append(pad_to_width(str(value), width))
            lines.append(separator.join(cells))

        return "\n".join(lines)

    def render_key_value(
        self,
        data: list[tuple[str, Any]],
        separator: str = ": ",
        key_width: int | None = None,
    ) -> str:
        """
        Render key-value pairs in a simple format.

        Args:
            data: List of (key, value) tuples
            separator: Separator between key and value
            key_width: Fixed key width (None = auto)

        Returns:
            Formatted key-value string
        """
        if not data:
            return ""

        if key_width is None:
            key_width = max(len(k) for k, _ in data)

        lines = []
        for key, value in data:
            key_str = self.theme.muted_text(f"{key:<{key_width}}")
            lines.append(f"{key_str}{separator}{value}")

        return "\n".join(lines)

    def _calculate_widths(
        self,
        headers: list[str],
        rows: list[list[Any]],
        max_width: int,
        min_width: int,
    ) -> list[int]:
        """Calculate column widths based on content."""
        widths = []

        for i, header in enumerate(headers):
            col_max = visible_len(header)
            for row in rows:
                if i < len(row):
                    col_max = max(col_max, visible_len(str(row[i])))
            widths.append(max(min_width, min(max_width, col_max)))

        return widths

    def _format_cell(self, value: str, width: int, align: str) -> str:
        """Format a cell value with alignment."""
        # Truncate if needed
        if visible_len(value) > width:
            value = truncate_to_width(value, width)

        return pad_to_width(value, width, align)
