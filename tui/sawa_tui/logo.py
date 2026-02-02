"""Logo downloading, caching, and ASCII art conversion.

This module handles:
- Downloading company logos from URLs
- Converting images to colored ASCII art using Unicode half-blocks
- Caching converted ASCII art to avoid re-downloading
- Asynchronous loading to avoid blocking the UI
"""

import hashlib
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path

import httpx
from PIL import Image
from rich.text import Text
from sawa.utils.xdg import get_cache_dir

logger = logging.getLogger(__name__)

# Character sets for ASCII art
# Using Unicode half-block characters for better resolution
BLOCK_UPPER = "\u2580"  # ▀ Upper half block
BLOCK_LOWER = "\u2584"  # ▄ Lower half block

# Thread pool for background loading (2 workers max)
_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="logo")


def get_logo_cache_dir() -> Path:
    """
    Get the logo cache directory.

    Returns:
        Path to cache directory (e.g., ~/.cache/sp500-tui/logos/)
    """
    cache_dir = get_cache_dir("sp500-tui") / "logos"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _get_cache_key(url: str, width: int, height: int) -> str:
    """
    Generate cache key from URL and dimensions.

    Args:
        url: Logo URL
        width: Target width in characters
        height: Target height in lines

    Returns:
        MD5 hash of the key
    """
    key = f"{url}_{width}_{height}"
    return hashlib.md5(key.encode()).hexdigest()


def _download_image(url: str, timeout: float = 5.0) -> Image.Image | None:
    """
    Download image from URL.

    Args:
        url: Image URL
        timeout: Request timeout in seconds

    Returns:
        PIL Image or None if download fails
    """
    try:
        # Check if URL requires Polygon API authentication
        headers = {}
        if "api.polygon.io" in url:
            import os

            api_key = os.environ.get("POLYGON_API_KEY")
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"

        response = httpx.get(url, headers=headers, timeout=timeout, follow_redirects=True)
        response.raise_for_status()

        # Handle SVG files by converting to PNG
        content_type = response.headers.get("content-type", "")
        if "svg" in content_type or url.endswith(".svg"):
            try:
                import cairosvg

                png_data = cairosvg.svg2png(bytestring=response.content)
                return Image.open(BytesIO(png_data))
            except ImportError:
                logger.debug("cairosvg not available, cannot convert SVG")
                return None
            except Exception as e:
                logger.debug(f"Failed to convert SVG: {e}")
                return None

        return Image.open(BytesIO(response.content))
    except Exception as e:
        logger.debug(f"Failed to download logo from {url}: {e}")
        return None


def _image_to_colored_ascii(image: Image.Image, width: int = 30, height: int = 12) -> Text:
    """
    Convert image to colored ASCII art using Rich Text.

    Uses Unicode half-block characters with foreground/background colors
    to achieve 2x vertical resolution. Each character represents 2 pixels
    stacked vertically.

    Args:
        image: PIL Image to convert
        width: Target width in characters
        height: Target height in lines (actual pixel height is 2x)

    Returns:
        Rich Text object with colored ASCII art
    """
    # Calculate target size maintaining aspect ratio
    # Terminal characters are approximately 2:1 height:width ratio
    # We use height*2 pixels because we combine 2 vertical pixels per half-block character
    orig_width, orig_height = image.size

    # Calculate aspect ratios
    # Effective terminal aspect = width / (height * 2) because each char is ~2x tall
    target_pixel_height = height * 2
    target_pixel_width = width

    # Adjust for terminal character aspect ratio (~2:1 height to width)
    # Each terminal character cell is about twice as tall as it is wide
    # Use 0.35 to stretch logos horizontally (instead of 0.5 for exact aspect)
    terminal_aspect_correction = 0.35  # Wider to avoid squeezed appearance

    # Calculate scaling to fit within bounds while preserving aspect
    scale_w = target_pixel_width / orig_width
    scale_h = (target_pixel_height * terminal_aspect_correction) / orig_height
    scale = min(scale_w, scale_h)

    # Calculate new dimensions
    new_width = int(orig_width * scale)
    new_height = int(orig_height * scale / terminal_aspect_correction)

    # Ensure we don't exceed bounds and dimensions are even
    new_width = min(new_width, target_pixel_width)
    new_height = min(new_height, target_pixel_height)
    new_height = (new_height // 2) * 2  # Make height even for half-block pairing

    img = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
    img = img.convert("RGBA")

    # Check if logo is predominantly dark (will be invisible on dark terminals)
    # Sample pixels to check average brightness
    pixels_sample = list(img.getdata())
    opaque_pixels = [(r, g, b) for r, g, b, a in pixels_sample if a > 128]

    if opaque_pixels:
        avg_brightness = sum(r + g + b for r, g, b in opaque_pixels) / (len(opaque_pixels) * 3)
        # If average brightness is < 64 (out of 255), logo is very dark
        if avg_brightness < 64:
            # Invert the logo to make it visible on dark backgrounds
            from PIL import ImageOps

            # Convert to RGB, invert, then back to RGBA
            rgb = img.convert("RGB")
            inverted = ImageOps.invert(rgb)
            img = inverted.convert("RGBA")
            # Restore alpha channel
            img.putalpha(
                image.resize((new_width, new_height), Image.Resampling.LANCZOS).getchannel("A")
            )

    result = Text()
    pixels = list(img.getdata())

    # Get actual dimensions after resize
    actual_width, actual_height = img.size

    # Calculate centering offsets
    char_height = actual_height // 2
    pad_left = (width - actual_width) // 2
    pad_top = (height - char_height) // 2

    # Render with centering
    for line_idx in range(height):
        # Check if we're in the padded area
        if line_idx < pad_top or line_idx >= pad_top + char_height:
            # Empty padding line
            result.append(" " * width)
        else:
            # Add left padding
            if pad_left > 0:
                result.append(" " * pad_left)

            # Render actual image data for this line
            y = (line_idx - pad_top) * 2
            for x in range(actual_width):
                upper_idx = y * actual_width + x
                lower_idx = (y + 1) * actual_width + x

                upper = pixels[upper_idx] if upper_idx < len(pixels) else (0, 0, 0, 0)
                lower = pixels[lower_idx] if lower_idx < len(pixels) else upper

                r1, g1, b1, a1 = upper
                r2, g2, b2, a2 = lower

                # Handle transparency (alpha < 128 is considered transparent)
                if a1 < 128 and a2 < 128:
                    result.append(" ")
                elif a1 < 128:
                    result.append(BLOCK_LOWER, style=f"rgb({r2},{g2},{b2})")
                elif a2 < 128:
                    result.append(BLOCK_UPPER, style=f"rgb({r1},{g1},{b1})")
                else:
                    result.append(BLOCK_LOWER, style=f"rgb({r2},{g2},{b2}) on rgb({r1},{g1},{b1})")

            # Add right padding
            pad_right = width - actual_width - pad_left
            if pad_right > 0:
                result.append(" " * pad_right)

        # Add newline except after last row
        if line_idx < height - 1:
            result.append("\n")

    return result


def _load_cached(cache_path: Path) -> Text | None:
    """
    Load ASCII art from cache file.

    Args:
        cache_path: Path to cache file

    Returns:
        Rich Text object or None if cache miss/error
    """
    if cache_path.exists():
        try:
            markup = cache_path.read_text(encoding="utf-8")
            return Text.from_markup(markup)
        except Exception as e:
            logger.debug(f"Failed to load cached logo: {e}")
            return None
    return None


def _save_to_cache(cache_path: Path, text: Text) -> None:
    """
    Save ASCII art to cache file.

    Args:
        cache_path: Path to cache file
        text: Rich Text to cache
    """
    try:
        cache_path.write_text(text.markup, encoding="utf-8")
    except Exception as e:
        logger.debug(f"Failed to cache logo: {e}")


def load_logo_async(
    url: str, width: int, height: int, callback: Callable[[Text | None], None]
) -> None:
    """
    Load logo asynchronously in background thread.

    Downloads logo from URL, converts to ASCII art, caches the result,
    and calls the callback with the result. If cached version exists,
    loads from cache immediately.

    Args:
        url: Logo URL
        width: Target width in characters
        height: Target height in lines
        callback: Called with result (Text or None) when done
    """

    def _load() -> None:
        cache_dir = get_logo_cache_dir()
        cache_key = _get_cache_key(url, width, height)
        cache_path = cache_dir / f"{cache_key}.txt"

        # Try cache first
        cached = _load_cached(cache_path)
        if cached:
            callback(cached)
            return

        # Download and convert
        image = _download_image(url)
        if image is None:
            callback(None)
            return

        ascii_art = _image_to_colored_ascii(image, width, height)
        _save_to_cache(cache_path, ascii_art)
        callback(ascii_art)

    _executor.submit(_load)


def get_placeholder(width: int = 30, height: int = 12) -> Text:
    """
    Return placeholder for missing logo.

    Args:
        width: Placeholder width in characters
        height: Placeholder height in lines

    Returns:
        Rich Text with centered placeholder
    """
    lines = []
    for i in range(height):
        if i == height // 2:
            label = "[No Logo]"
            padding = (width - len(label)) // 2
            lines.append(" " * padding + label)
        else:
            lines.append(" " * width)
    return Text("\n".join(lines), style="dim")


def invalidate_cache(ticker: str | None = None) -> None:
    """
    Invalidate logo cache.

    Args:
        ticker: If provided, invalidate only that ticker's cache.
                If None, clear entire cache.

    Note:
        Currently clears entire cache as we don't maintain
        a ticker-to-cache-key mapping.
    """
    cache_dir = get_logo_cache_dir()
    for cache_file in cache_dir.glob("*.txt"):
        try:
            cache_file.unlink()
        except Exception as e:
            logger.debug(f"Failed to delete cache file {cache_file}: {e}")
