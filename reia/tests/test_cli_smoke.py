"""
Simple smoke tests for CLI commands to verify they don't crash on import.
Tests function signatures and help commands without mocking heavy dependencies.
"""
import inspect

from typer.testing import CliRunner

from reia.cli import main as cli


def test_cli_function_imports():
    """Test that all CLI functions can be imported without errors."""

    # Database commands
    assert callable(cli.run_alembic_upgrade)
    assert callable(cli.run_alembic_downgrade)
    assert callable(cli.show_migration_history)
    assert callable(cli.show_current_revision)
    assert callable(cli.stamp_database)

    # Exposure commands
    assert callable(cli.add_exposure)
    assert callable(cli.delete_exposure)
    assert callable(cli.list_exposure)
    assert callable(cli.create_exposure)
    assert callable(cli.add_exposure_geometries)
    assert callable(cli.delete_exposure_geometries)

    # Vulnerability commands
    assert callable(cli.add_vulnerability)
    assert callable(cli.delete_vulnerability)
    assert callable(cli.list_vulnerability)
    assert callable(cli.create_vulnerability)

    # Fragility commands
    assert callable(cli.add_fragility)
    assert callable(cli.delete_fragility)
    assert callable(cli.list_fragility)
    assert callable(cli.create_fragility)

    # Taxonomy commands
    assert callable(cli.add_taxonomymap)
    assert callable(cli.delete_taxonomymap)
    assert callable(cli.list_taxonomymap)
    assert callable(cli.create_taxonomymap)

    # Calculation commands
    assert callable(cli.create_calculation_files)
    assert callable(cli.run_test_calculation_cmd)
    assert callable(cli.run_calculation)
    assert callable(cli.list_calculations)
    assert callable(cli.delete_calculation)

    # Risk assessment commands
    assert callable(cli.add_risk_assessment)
    assert callable(cli.delete_risk_assessment)
    assert callable(cli.list_risk_assessment)
    assert callable(cli.run_risk_assessment)


def test_cli_help_commands():
    """Test that CLI help commands work without crashing."""
    runner = CliRunner()

    # Test main app help
    result = runner.invoke(cli.app, ["--help"])
    assert result.exit_code == 0
    assert "REIA - Rapid Earthquake Impact Assessment" in result.stdout

    # Test subcommand help
    result = runner.invoke(cli.app, ["db", "--help"])
    assert result.exit_code == 0

    result = runner.invoke(cli.app, ["exposure", "--help"])
    assert result.exit_code == 0

    result = runner.invoke(cli.app, ["vulnerability", "--help"])
    assert result.exit_code == 0

    result = runner.invoke(cli.app, ["fragility", "--help"])
    assert result.exit_code == 0

    result = runner.invoke(cli.app, ["taxonomymap", "--help"])
    assert result.exit_code == 0

    result = runner.invoke(cli.app, ["calculation", "--help"])
    assert result.exit_code == 0

    result = runner.invoke(cli.app, ["risk-assessment", "--help"])
    assert result.exit_code == 0


def test_cli_command_help():
    """Test individual command help without executing."""
    runner = CliRunner()

    # Test a few individual commands
    commands_to_test = [
        ["db", "migrate", "--help"],
        ["exposure", "list", "--help"],
        ["vulnerability", "add", "--help"],
        ["fragility", "delete", "--help"],
        ["calculation", "run", "--help"],
        ["risk-assessment", "run", "--help"]
    ]

    for cmd in commands_to_test:
        result = runner.invoke(cli.app, cmd)
        assert result.exit_code == 0, f"Command {' '.join(cmd)} failed"
        assert "Usage:" in result.stdout or "Show this message" in result.stdout


def test_cli_app_structure():
    """Test that the CLI app is properly structured."""

    # Verify main app exists and is a Typer instance
    assert cli.app is not None
    assert str(type(cli.app)) == "<class 'typer.main.Typer'>"

    # Verify we can get help from the app (proves structure works)
    runner = CliRunner()
    result = runner.invoke(cli.app, ["--help"])
    assert result.exit_code == 0

    # Check that major subcommands appear in help text
    help_text = result.stdout
    assert 'db' in help_text
    assert 'exposure' in help_text
    assert 'vulnerability' in help_text
    assert 'fragility' in help_text
    assert 'calculation' in help_text
    assert 'risk-assessment' in help_text


def test_helper_functions():
    """Test that helper functions work."""

    # Test that _get_alembic_directory doesn't crash on import
    assert callable(cli._get_alembic_directory)

    # Test main callback function
    assert callable(cli.main)
    sig = inspect.signature(cli.main)
    assert 'verbose' in sig.parameters
