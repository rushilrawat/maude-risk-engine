from typer.testing import CliRunner

from maude import __version__
from maude.cli import app


def test_package_exposes_version_and_cli() -> None:
    assert __version__ == "0.1.0"
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == "0.1.0"
