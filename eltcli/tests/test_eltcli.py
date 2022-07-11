from eltcli import __version__
from typer.testing import CliRunner
import typer
from eltcli.main import extract
runner = CliRunner()
app = typer.Typer()
app.command()(extract)

def test_version():
    assert __version__ == '0.1.0'

def test_extract():
    result = runner.invoke(
        app ,
        ['--database','testsource.db',
        '--sql','test_query.sql',
        '--params','{"date":"2011-12-08"}',
        '--target','test_transactions_2011-12-08.csv'
    ])
    assert result.exit_code == 0
    assert "The file is saved on test_transactions_2011-12-08.csv" in result.stdout

