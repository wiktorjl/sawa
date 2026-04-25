"""Tests for packaged project resource resolution."""

from pathlib import Path

from sawa.utils.resources import resolve_project_resource


def test_default_schema_dir_resolves_to_existing_resource() -> None:
    """The default schema path should work from source and installed wheels."""
    schema_dir = resolve_project_resource(Path("sqlschema"), "sqlschema")

    assert schema_dir.exists()
    assert (schema_dir / "00_setup.sql").exists()


def test_custom_missing_resource_path_is_not_rewritten(tmp_path) -> None:
    """Only default resource names should fall back to package/source assets."""
    custom_path = tmp_path / "missing-schema"

    assert resolve_project_resource(custom_path, "sqlschema") == custom_path
