"""Apply the SQL files in migrations/ to a database, each exactly once.

`schema_migrations` records every applied file with its SHA-256, so re-running is a no-op
and an edited, already-applied file is refused instead of silently diverging.
"""

import hashlib
from pathlib import Path
from typing import Any

import psycopg

# Any fixed number: every `migrate` call takes the same Postgres advisory lock, so two
# runs at once (two services starting together) cannot apply the same file twice.
LOCK_ID = 20261007

SCHEMA_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    text        PRIMARY KEY,
    filename   text        NOT NULL,
    sha256     text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)
"""

Connection = psycopg.Connection[tuple[Any, ...]]


class MigrationError(Exception):
    """A migration file was edited after it had already been applied."""


def migration_files(migration_dir: Path) -> list[Path]:
    """Every .sql file in the folder, in name order (001 before 002)."""
    return sorted(migration_dir.glob("*.sql"))


def fingerprint(path: Path) -> str:
    """SHA-256 of the file: changes if even one character changes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def version_of(path: Path) -> str:
    """'001_core.sql' -> '001'."""
    return path.name.split("_", 1)[0]


def migrate(conn: Connection, migrations_dir: Path) -> list[str]:
    """Apply every migration not applied yet, in order. Return the versions applied.

    `conn` must be in autocommit mode, so each `conn.transaction()` block is a real
    transaction: a file's SQL and its `schema_migrations` row are saved together or not at all.
    """
    conn.execute(SCHEMA_MIGRATIONS)
    conn.execute("SELECT pg_advisory_lock(%s)", (LOCK_ID,))
    try:
        rows = conn.execute("SELECT version, sha256 FROM schema_migrations").fetchall()
        applied: dict[str, str] = dict(rows)

        newly_applied: list[str] = []
        for path in migration_files(migrations_dir):
            version = version_of(path)
            digest = fingerprint(path)

            if version in applied:
                if applied[version] != digest:
                    raise MigrationError(
                        f"{path.name} changed after it was applied; add a new migration instead"
                    )
                continue

            with conn.transaction():
                # A whole file is several statements: allowed only without parameters.
                conn.execute(path.read_text())
                conn.execute(
                    "INSERT INTO schema_migrations (version, filename, sha256) VALUES (%s, %s, %s)",
                    (version, path.name, digest),
                )
            newly_applied.append(version)

        return newly_applied
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))
