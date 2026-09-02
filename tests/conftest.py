"""Test-wide safety guards.

Several modules call ``load_dotenv()`` — ``sawa.cli.main`` and the one-off
scripts under ``scripts/`` among them — and the suite both calls ``main()`` and
imports those scripts. That pulls the operator's real ``.env`` into
``os.environ`` for the rest of the session, ``NTFY_TOPIC`` included, so any
test exercising a missing-key or failure path built a real ``NtfyNotifier``
and pushed to the operator's live phone topic. One full run sent five
"missing FRED_API_KEY" alerts to a real device.

Tests that mock ``get_notifier`` were never the problem; the ones that forget
are, and so is every future test nobody has written yet. Rather than rely on
per-test discipline, keep the operator's topic out of the environment no
matter who loads the file: ``get_notifier`` then falls back to
``NullNotifier``. A test that wants notifier behaviour sets its own topic with
``monkeypatch``, which applies on top of this.

``load_dotenv`` is left working so tests that depend on other settings behave
normally; only the notification topic is stripped.
"""

from __future__ import annotations

import os

import dotenv

_TOPIC_VARS = ("NTFY_TOPIC",)
_real_load_dotenv = dotenv.load_dotenv


def _drop_operator_topic() -> None:
    for name in _TOPIC_VARS:
        os.environ.pop(name, None)


def _load_dotenv_without_operator_topic(*args: object, **kwargs: object) -> bool:
    """Load .env as usual, then strip the operator's notification topic."""
    loaded = _real_load_dotenv(*args, **kwargs)
    _drop_operator_topic()
    return loaded


# Patch before test modules are imported, so a module-level `from dotenv import
# load_dotenv` in a script the suite loads binds this wrapper rather than the
# original.
dotenv.load_dotenv = _load_dotenv_without_operator_topic


def pytest_configure(config: object) -> None:
    """Make it impossible for the suite to notify a real device."""
    _drop_operator_topic()
