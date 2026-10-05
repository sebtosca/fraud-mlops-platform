from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from fraud.config import Settings
from fraud.ports import ObjectAlreadyExists, ObjectNotFound

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class S3ObjectStore:
    """`ObjectStore` over one bucket of any S3-compatible store (SeaweedFS locally, GCS later)."""

    def __init__(self, client: S3Client, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    @classmethod
    def from_settings(cls, settings: Settings) -> S3ObjectStore:
        client = boto3.client(
            "s3",
            endpoint_url=settings.S3_ENDPOINT_URL,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY.get_secret_value(),
            region_name=settings.REGION,  # ignored by SeaweedFS, required by boto3
            config=Config(s3={"addressing_style": "path"}),  # SeaweedFS needs path-style URLs
        )
        return cls(client, settings.LAKE_BUCKET)

    def put_bytes(self, key: str, data: bytes, *, if_absent: bool = False) -> None:
        try:
            if if_absent:
                # Conditional write: S3 rejects it with 412 if the key already exists.
                self._client.put_object(Bucket=self._bucket, Key=key, Body=data, IfNoneMatch="*")
            else:
                self._client.put_object(Bucket=self._bucket, Key=key, Body=data)
        except ClientError as e:
            if e.response["Error"]["Code"] == "PreconditionFailed":
                raise ObjectAlreadyExists(key) from e
            raise

    def get_bytes(self, key: str) -> bytes:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise ObjectNotFound(key) from e
            raise
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as e:
            # HEAD responses have no body, so the code is the bare HTTP status.
            if e.response["Error"]["Code"] == "404":
                return False
            raise
        return True

    def list_keys(self, prefix: str) -> Iterator[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]
