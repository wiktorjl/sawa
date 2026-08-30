"""Small security helpers shared by logging and provider integrations."""

from __future__ import annotations

import os
import re
from io import TextIOWrapper
from pathlib import Path
from typing import Any, cast
from urllib.parse import urljoin, urlsplit

PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_FILE_MODE = 0o600

_URL_SECRET_RE = re.compile(
    r"(?i)([?&](?:api[_-]?key|access[_-]?token|auth[_-]?token|refresh[_-]?token|"
    r"session[_-]?token|token|ssl[_-]?password|password|client[_-]?secret|secret|"
    r"signature)=)([^&#\s]+)"
)
_INLINE_SECRET_RE = re.compile(
    r"(?i)(\b(?:api[_-]?key|access[_-]?token|auth[_-]?token|refresh[_-]?token|"
    r"session[_-]?token|token|ssl[_-]?password|password|client[_-]?secret|secret)"
    r"\s*=\s*)([^\s,;&#]+)"
)
_MAPPING_SECRET_RE = re.compile(
    r"(?i)((?<![A-Za-z0-9_])(?:[\"']?)(?:api[_-]?key|access[_-]?token|"
    r"auth[_-]?token|refresh[_-]?token|session[_-]?token|token|"
    r"ssl[_-]?password|password|client[_-]?secret|secret)(?:[\"']?)\s*:\s*)"
    r"(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\s,}\]]+)"
)
_ENV_SECRET_RE = re.compile(
    r"(?i)\b((?:POLYGON|MASSIVE|FRED|NTFY|AWS|PG)[A-Z0-9_]*"
    r"(?:KEY|TOKEN|SECRET|PASSWORD)\s*=\s*)([^\s,;]+)"
)
_BEARER_SECRET_RE = re.compile(r"(?i)(\bBearer\s+)([^\s,;]+)")
_AUTHORIZATION_HEADER_RE = re.compile(
    r"(?i)(\bAuthorization\s*:\s*(?:(?:Bearer|Basic|Token)\s+)?)([^\s,;]+)"
)
_API_KEY_HEADER_RE = re.compile(
    r"(?i)(\b(?:X-API-Key|API-Key)\s*:\s*)([^\s,;]+)"
)
_DATABASE_PASSWORD_RE = re.compile(
    r"(?i)(\bpostgres(?:ql)?://[^\s:/@]+:)([^\s@/]+)(@)"
)
_SINGLE_QUOTED_SQL_RE = re.compile(r"'(?:''|[^'])*'", re.DOTALL)
_DOLLAR_QUOTED_SQL_RE = re.compile(
    r"(?P<tag>\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$).*?(?P=tag)",
    re.DOTALL,
)


def _restrict_open_file(fd: int, path: Path) -> None:
    """Apply owner-only mode using the descriptor API available on this OS."""
    fchmod = getattr(os, "fchmod", None)
    if fchmod is not None:
        fchmod(fd, PRIVATE_FILE_MODE)
        return
    try:  # pragma: no cover - exercised on Windows
        os.chmod(path, PRIVATE_FILE_MODE, follow_symlinks=False)
    except (NotImplementedError, TypeError):  # pragma: no cover - OS-specific
        path.chmod(PRIVATE_FILE_MODE)


def ensure_private_directory(path: Path) -> Path:
    """Create ``path`` and ensure only its owner can traverse it."""
    path.mkdir(mode=PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
    if path.is_symlink():
        raise OSError(f"Refusing to use symlink as private directory: {path}")
    path.chmod(PRIVATE_DIRECTORY_MODE)
    return path


def ensure_private_file(path: Path) -> Path:
    """Create ``path`` if needed and enforce owner-only read/write access."""
    ensure_private_directory(path.parent)
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, PRIVATE_FILE_MODE)
    try:
        _restrict_open_file(fd, path)
    finally:
        os.close(fd)
    return path


def open_private_text(path: Path, mode: str = "a") -> TextIOWrapper:
    """Open a private text file without following a final-component symlink."""
    if mode not in {"a", "r", "w"}:
        raise ValueError("Private text files support only append, read, or write mode")
    ensure_private_directory(path.parent)
    if mode == "r":
        flags = os.O_RDONLY
    else:
        flags = os.O_WRONLY | os.O_CREAT
        flags |= os.O_APPEND if mode == "a" else os.O_TRUNC
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, PRIVATE_FILE_MODE)
    try:
        _restrict_open_file(fd, path)
        return cast(TextIOWrapper, os.fdopen(fd, mode, encoding="utf-8"))
    except Exception:
        os.close(fd)
        raise


def redact_sensitive_text(value: object) -> str:
    """Return a log-safe representation with common credential forms removed."""
    text = str(value)
    text = _DATABASE_PASSWORD_RE.sub(r"\1<redacted>\3", text)
    text = _URL_SECRET_RE.sub(r"\1<redacted>", text)
    text = _INLINE_SECRET_RE.sub(r"\1<redacted>", text)
    text = _MAPPING_SECRET_RE.sub(r"\1<redacted>", text)
    text = _ENV_SECRET_RE.sub(r"\1<redacted>", text)
    text = _AUTHORIZATION_HEADER_RE.sub(r"\1<redacted>", text)
    text = _API_KEY_HEADER_RE.sub(r"\1<redacted>", text)
    return _BEARER_SECRET_RE.sub(r"\1<redacted>", text)


def redact_sql_literals(query: str) -> str:
    """Preserve SQL shape for audit analysis while removing literal values."""
    redacted = _DOLLAR_QUOTED_SQL_RE.sub("$<redacted>$", query)
    redacted = _SINGLE_QUOTED_SQL_RE.sub("'<redacted>'", redacted)
    return redact_sensitive_text(redacted)


def redact_parameter_values(params: dict[str, Any] | None) -> dict[str, str]:
    """Keep parameter names for diagnostics without persisting their values."""
    if not params:
        return {}
    return {str(key): "<redacted>" for key in params}


def validate_https_origin_url(base_url: str, path_or_url: str) -> str:
    """Resolve ``path_or_url`` and require the exact HTTPS origin of ``base_url``.

    This is intended for provider-controlled pagination links used alongside
    authenticated HTTP clients. Rejections deliberately do not echo the URL.
    """
    base = urlsplit(base_url)
    candidate = urljoin(f"{base_url.rstrip('/')}/", path_or_url)
    parsed = urlsplit(candidate)
    try:
        port = parsed.port
    except ValueError as e:
        raise ValueError("Invalid provider pagination URL") from e

    if (
        base.scheme.lower() != "https"
        or parsed.scheme.lower() != "https"
        or base.hostname is None
        or parsed.hostname is None
        or parsed.hostname.lower() != base.hostname.lower()
        or port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(
            "Refusing provider URL outside the configured HTTPS API origin"
        )
    return candidate
