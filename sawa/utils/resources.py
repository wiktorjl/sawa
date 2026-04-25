"""Helpers for resolving files that are bundled with the package."""

from pathlib import Path


def project_root() -> Path:
    """Return the source checkout root when running from a source tree."""
    return Path(__file__).resolve().parents[2]


def packaged_resource_path(resource_name: str) -> Path:
    """Return the expected path for a resource inside the installed package."""
    return Path(__file__).resolve().parents[1] / resource_name


def source_resource_path(resource_name: str) -> Path:
    """Return the expected path for a resource in the source checkout."""
    return project_root() / resource_name


def resolve_project_resource(path: Path, resource_name: str) -> Path:
    """Resolve a default resource path from either source checkout or wheel data.

    Args:
        path: User-supplied or default path
        resource_name: Default project resource name, such as ``sqlschema``

    Returns:
        Existing path when present, package/source resource for default paths, or the original
        missing path for custom user paths.
    """
    if path.exists():
        return path

    if path != Path(resource_name):
        return path

    packaged_path = packaged_resource_path(resource_name)
    if packaged_path.exists():
        return packaged_path

    return source_resource_path(resource_name)
