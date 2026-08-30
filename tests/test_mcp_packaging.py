from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib


def test_mcp_package_declares_editable_import_root() -> None:
    pyproject = Path(__file__).parents[1] / "mcp_server" / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text(encoding="utf-8"))

    assert config["tool"]["hatch"]["build"]["dev-mode-dirs"] == [".."]
    assert config["tool"]["hatch"]["build"]["targets"]["wheel"]["sources"] == {
        "": "mcp_server"
    }
