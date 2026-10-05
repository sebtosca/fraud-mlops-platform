from collections.abc import Iterator

import boto3
import pytest
from moto import mock_aws

from fraud.adapters.s3 import S3ObjectStore
from fraud.ports import ObjectAlreadyExists, ObjectNotFound

BUCKET = "lake"


@pytest.fixture
def store() -> Iterator[S3ObjectStore]:
    """A fresh, empty bucket in an in-memory fake S3 for each test."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield S3ObjectStore(client, BUCKET)


def test_put_then_get_returns_same_bytes(store: S3ObjectStore) -> None:
    store.put_bytes("silver/a.parquet", b"hello")

    assert store.get_bytes("silver/a.parquet") == b"hello"


def test_get_missing_key_raises_object_not_found(store: S3ObjectStore) -> None:
    with pytest.raises(ObjectNotFound) as exc_info:
        store.get_bytes("missing")

    assert exc_info.value.key == "missing"


def test_put_if_absent_twice_raises(store: S3ObjectStore) -> None:
    store.put_bytes("silver/_MANIFEST.json", b"first", if_absent=True)

    with pytest.raises(ObjectAlreadyExists) as exc_info:
        store.put_bytes("silver/_MANIFEST.json", b"second", if_absent=True)

    assert exc_info.value.key == "silver/_MANIFEST.json"
    assert store.get_bytes("silver/_MANIFEST.json") == b"first"


def test_put_without_if_absent_overwrites(store: S3ObjectStore) -> None:
    store.put_bytes("k", b"first")
    store.put_bytes("k", b"second")

    assert store.get_bytes("k") == b"second"


def test_exists(store: S3ObjectStore) -> None:
    store.put_bytes("present", b"x")

    assert store.exists("present") is True
    assert store.exists("absent") is False


def test_list_keys_filters_by_prefix(store: S3ObjectStore) -> None:
    for key in ["silver/a", "silver/b", "bronze/c"]:
        store.put_bytes(key, b"")

    assert sorted(store.list_keys("silver/")) == ["silver/a", "silver/b"]


def test_list_keys_empty_prefix_returns_nothing(store: S3ObjectStore) -> None:
    store.put_bytes("bronze/c", b"")

    assert list(store.list_keys("gold/")) == []


def test_list_keys_paginates_past_1000(store: S3ObjectStore) -> None:
    for i in range(1005):
        store.put_bytes(f"bronze/{i:04d}", b"")

    assert sum(1 for _ in store.list_keys("bronze/")) == 1005
