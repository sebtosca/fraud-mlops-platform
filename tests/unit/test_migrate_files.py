"""The database-free parts of the migration runner (the rest is in tests/integration)."""

from pathlib import Path

from fraud.db.migrate import fingerprint, migration_files, version_of

MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"


def test_files_are_listed_in_version_order(tmp_path: Path) -> None:
    for name in ["010_later.sql", "002_second.sql", "001_first.sql", "notes.txt"]:
        (tmp_path / name).write_text("")

    names = [path.name for path in migration_files(tmp_path)]

    assert names == ["001_first.sql", "002_second.sql", "010_later.sql"]


def test_version_is_the_prefix_before_the_first_underscore() -> None:
    assert version_of(Path("migrations/001_core.sql")) == "001"
    assert version_of(Path("012_add_index_on_card_hash.sql")) == "012"


def test_fingerprint_changes_when_the_file_changes(tmp_path: Path) -> None:
    path = tmp_path / "001_x.sql"
    path.write_text("CREATE TABLE a (x int);")
    before = fingerprint(path)

    path.write_text("CREATE TABLE a (x bigint);")

    assert len(before) == 64
    assert fingerprint(path) != before


def test_repo_migrations_have_unique_versions() -> None:
    versions = [version_of(path) for path in migration_files(MIGRATIONS)]

    assert versions
    assert len(versions) == len(set(versions))
