"""The `fraud` command-line app (Typer). Each job and admin task is a sub-command."""

from pathlib import Path
from typing import Annotated

import typer

from fraud.adapters.db import connect
from fraud.config import get_settings
from fraud.db.migrate import MigrationError, migrate

app = typer.Typer(no_args_is_help=True, help="Fraud ML platform commands.")
db_app = typer.Typer(no_args_is_help=True, help="Database commands.")
app.add_typer(db_app, name="db")


@db_app.command("migrate")
def db_migrate(
    migrations_dir: Annotated[
        Path,
        typer.Option(help="Folder of numbered .sql files.", exists=True, file_okay=False),
    ] = Path("migrations"),
) -> None:
    """Apply pending migrations to DATABASE_URL. Safe to run any number of times."""
    with connect(get_settings(), autocommit=True, application_name="fraud-migrate") as conn:
        try:
            applied = migrate(conn, migrations_dir)
        except MigrationError as e:
            typer.echo(f"Migration refused: {e}", err=True)
            raise typer.Exit(code=1) from e
    typer.echo(f"Applied: {', '.join(applied)}" if applied else "Up to date.")


if __name__ == "__main__":
    app()
