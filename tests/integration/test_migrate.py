"""Migration runner against the Compose Postgres (needs `make up`).

Each test runs in its own throwaway database, so `fraud_local` is never touched.
"""

import secrets
import shutil
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from fraud.config import get_settings
from fraud.db.migrate import Connection, MigrationError, migrate

pytestmark = pytest.mark.integration

MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"

EXPECTED_TABLES = {
    "schema_migrations",
    "predictions",
    "card_state",
    "consumer_offsets",
    "sim_clock",
    "sim_control",
    "replay_progress",
    "labels",
    "oracle_labels",
    "shift_events",
    "shift_audit",
    "drift_metrics",
    "alerts",
    "monitor_state",
    "pipeline_requests",
    "batch_scores",
    "model_promotions",
    "retrain_runs",
}


@pytest.fixture
def conn() -> Iterator[Connection]:
    """A connection to a brand-new, empty database, dropped after the test."""
    url = get_settings().DATABASE_URL
    name = f"test_migrate_{secrets.token_hex(4)}"
    admin_url = make_conninfo(url, dbname="postgres")

    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        with psycopg.connect(make_conninfo(url, dbname=name), autocommit=True) as test_conn:
            yield test_conn
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def table_names(conn: Connection) -> set[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    ).fetchall()
    return {row[0] for row in rows}


def test_first_run_applies_and_second_run_is_a_no_op(conn: Connection) -> None:
    assert migrate(conn, MIGRATIONS) == ["001"]
    assert migrate(conn, MIGRATIONS) == []


def test_all_tables_exist(conn: Connection) -> None:
    migrate(conn, MIGRATIONS)

    assert table_names(conn) == EXPECTED_TABLES


def test_edited_migration_is_refused(conn: Connection, tmp_path: Path) -> None:
    shutil.copy(MIGRATIONS / "001_core.sql", tmp_path / "001_core.sql")
    migrate(conn, tmp_path)

    with (tmp_path / "001_core.sql").open("a") as f:
        f.write("\n-- edited after it was applied\n")

    with pytest.raises(MigrationError, match=r"001_core\.sql changed"):
        migrate(conn, tmp_path)


def test_failed_migration_leaves_nothing_behind(conn: Connection, tmp_path: Path) -> None:
    (tmp_path / "001_broken.sql").write_text("CREATE TABLE half_done (x int);\nNOT VALID SQL;")

    with pytest.raises(psycopg.errors.SyntaxError):
        migrate(conn, tmp_path)

    assert "half_done" not in table_names(conn)
    assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone() == (0,)


def test_model_promotions_is_append_only(conn: Connection) -> None:
    migrate(conn, MIGRATIONS)
    conn.execute(
        "INSERT INTO model_promotions (alias, model_version, reason) VALUES (%s, %s, %s)",
        ("champion", "1", "initial Champion"),
    )

    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE model_promotions SET reason = 'rewritten'")
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("DELETE FROM model_promotions")
