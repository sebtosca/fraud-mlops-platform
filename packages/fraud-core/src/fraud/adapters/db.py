from typing import Any

import psycopg

from fraud.config import Settings


def connect(
    settings: Settings,
    *,
    autocommit: bool = False,
    application_name: str = "fraud",
) -> psycopg.Connection[tuple[Any, ...]]:
    """Open a connection to the fraud database from `DATABASE_URL`.

    The caller owns the connection: use it as `with connect(settings) as conn:` so it is
    committed (or rolled back on error) and closed. Connection errors propagate as
    `psycopg.OperationalError`; `application_name` shows up in `pg_stat_activity`.
    """
    return psycopg.connect(
        settings.DATABASE_URL,
        autocommit=autocommit,
        application_name=application_name,
    )
