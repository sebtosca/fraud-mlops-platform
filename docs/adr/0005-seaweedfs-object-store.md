# ADR-0005: SeaweedFS as the local S3-compatible object store (replaces MinIO)

Status: accepted (2026-10-04, discovery Q36); supersedes the MinIO choice in Q30

## Context
The platform needs a local S3-compatible store for MLflow artifacts, data snapshots and the bronze/silver/gold layers, standing in for GCS. MinIO was chosen in discovery, but research on 2026-10-04 found the MinIO community repository archived ("no longer maintained", source-only distribution) and `minio/minio` images no longer pullable from Docker Hub (RESEARCH.md §04, §08).

## Decision
Use SeaweedFS (Apache-2.0) through its S3 API, deployed with its Helm chart (`allInOne`) on kind and as a container in Compose. Documentation refers to "the S3-compatible object store". Considered: MinIO/AIStor (licensing and availability), RustFS (1.0 too new), Garage (no versioning), fake-gcs-server (no S3 API for MLflow/Spark).

## Consequences
- MLflow (`MLFLOW_S3_ENDPOINT_URL`) and Spark s3a work unchanged against an S3 endpoint; the s3a checksum options must be set and writes tested early.
- The SeaweedFS bucket-creation Helm hook behaves differently under Argo CD; buckets may need a separate Job.
- On GCP the store is GCS; only endpoint configuration changes.
