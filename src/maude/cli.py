import typer

from maude import __version__

app = typer.Typer(no_args_is_help=True)


@app.callback()
def main() -> None:
    """MAUDE reporting-signal analysis and triage commands."""


@app.command()
def version() -> None:
    """Print the installed application version."""
    typer.echo(__version__)
