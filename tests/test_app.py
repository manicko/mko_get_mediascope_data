from typer.testing import CliRunner

from mko_get_mediascope_data.app import app


def test_cli_init():
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--help"])
    assert result.exit_code == 0
    assert "Инициализировать" in result.stdout
