"""Credential redaction and private-file portability regressions."""

import os

import pytest

from sawa.utils import security


@pytest.mark.parametrize(
    ("value", "secret"),
    [
        ("X-API-Key: header-secret", "header-secret"),
        ("API-Key: provider-secret", "provider-secret"),
        ("Authorization: Basic dXNlcjpwYXNz", "dXNlcjpwYXNz"),
        ("Authorization: token auth-secret", "auth-secret"),
        ("Authorization: Bearer bearer-secret", "bearer-secret"),
        ("postgresql://u:p@h/db?sslpassword=ssl-secret", "ssl-secret"),
        ("https://provider.test/?client_secret=client-secret", "client-secret"),
        ("refresh_token=refresh-secret", "refresh-secret"),
        ("session-token=session-secret", "session-secret"),
        ('{"apiKey": "json-secret"}', "json-secret"),
        ("{'api_key': 'repr-secret'}", "repr-secret"),
        ('{"client-secret":"client-json-secret"}', "client-json-secret"),
        ("accessToken: unquoted-secret", "unquoted-secret"),
    ],
)
def test_header_credentials_are_redacted(value: str, secret: str) -> None:
    redacted = security.redact_sensitive_text(value)

    assert secret not in redacted
    assert "<redacted>" in redacted


def test_private_file_mode_has_non_fchmod_fallback(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "private.log"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)
    calls: list[tuple[object, ...]] = []
    monkeypatch.delattr(security.os, "fchmod", raising=False)
    monkeypatch.setattr(
        security.os,
        "chmod",
        lambda *args, **kwargs: calls.append((*args, kwargs)),
    )
    try:
        security._restrict_open_file(fd, path)
    finally:
        os.close(fd)

    assert calls
