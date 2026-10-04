# ADR-0003: kind + Helm + Argo CD locally; portable GKE Terraform that is never applied

Status: accepted (2026-10-02, discovery Q25, Q26)

## Context
The target role lists Kubernetes, GitOps, Docker and GitLab, and the user wants GCP infrastructure as Terraform without paying to run it. Options: Docker Compose locally plus unrelated cloud code; managed GCP services (Composer, Vertex AI, Pub/Sub) in Terraform; or the same Kubernetes deployment locally and on GKE.

## Decision
Run everything on a local kind cluster, deployed by Argo CD from Helm charts in `deploy/`. Terraform describes a portable GCP target — GKE (with GPU node pool), Cloud SQL, GCS, Artifact Registry, Secret Manager, Workload Identity, Kafka via Strimzi — that would run the same charts. Terraform is validated in CI (fmt, validate, tflint, checkov) but never planned against or applied to a real project. Compose survives only as an optional fast inner loop.

## Consequences
- One deployment path is exercised for real (kind); the GKE path is credible because it reuses those charts.
- Terraform correctness beyond static checks is unproven; the README says so honestly.
- Airflow, Kafka, MLflow, Postgres, MinIO and vLLM on kind need meaningful RAM (31 GiB available) and GPU passthrough for vLLM.
- Managed services (Composer, Vertex AI, Managed Kafka) are discussed only in the build-vs-buy section.
- Moving back to Compose-only later would drop the GitOps and Kubernetes evidence.

## Amendment (2026-10-04, discovery Q37, Q38)
- **Phase 1 runs on Docker Compose**; Phase 2 moves the same images, env-var config and service names to kind. Compose is therefore a real runtime for Phase 1, not just an inner loop.
- On kind, **platform services are shared** (Kafka, Postgres with per-env databases, MLflow, object store, Airflow, vLLM); only application services are duplicated in `staging` and `prod`. A full stack per namespace (~31–35 GiB) does not fit the 31 GiB host. vLLM runs at `replicas: 0` by default.
- GPU on kind requires nvkind + the NVIDIA Container Toolkit; timeboxed to ~2 h. Fallback: vLLM as a plain Docker container on the `kind` network behind a selector-less Service, stated in the README.
