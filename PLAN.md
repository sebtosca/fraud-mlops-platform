# Plan: Fraud ML Platform

Generated: 2026-10-04 from DISCOVERY.md + RESEARCH.md

## Goal
A local MLOps platform for real-time card-fraud detection, built as an interview portfolio for an ML/MLOps engineer role. Sparkov transactions for 2020 are replayed through Kafka on an accelerated Simulated clock. A LightGBM Champion scores each Transaction through a shared per-card feature function and makes a Decision (block / review / allow). Labels arrive late, and blocked Transactions are labelled only through a random Exploration sample. Covariate, label and concept Shifts are injected on demand and detected by drift and action-rate monitoring. Alerts trigger Stateless and Stateful retraining. Challengers run in Shadow mode, then Canary, and are promoted by moving an MLflow alias only if they win. Part A runs the whole loop on Docker Compose with one `make demo` that writes a results report. Part B rebuilds it as a platform:
- GitLab CI/CD with SonarQube Cloud, Trivy, and model and prompt gates;
- kind + Helm + Argo CD with `staging` and `prod`;
- Airflow 3 and PySpark pipelines;
- a self-hosted vLLM Case summary service with an eval gate;
- GCP Terraform that is validated but never applied.

The README, the build-vs-buy section and the Google Cloud MLOps Level 2 mapping make it interview-ready.

## Tech stack
| Area | Choice (version as of 2026-10-04) | Source |
|---|---|---|
| Language / env | Python 3.13, uv workspace; `pandas>=2.2,<3` pinned workspace-wide | DISCOVERY Q3, Q43; RESEARCH §04 |
| Data | Sparkov (Kaggle `kartik2112/fraud-detection`, CC0); train 2019, replay 2020 | ADR-0001; RESEARCH §01 |
| Validation | pandera 0.33.1 (`pandera.pandas`, `pandera.pyspark` generated from one column spec) | DISCOVERY Q35; RESEARCH §04 |
| Model | LightGBM 4.7.0; isotonic calibration (scikit-learn) | DISCOVERY Q14; RESEARCH §03 |
| ONNX | onnxmltools 1.16.0 (opset 15, `zipmap=False`), onnxruntime 1.30.0 | DISCOVERY Q6; RESEARCH §03 |
| Drift stats | custom PSI/KS (scipy 1.18, numpy 2.5) | DISCOVERY Q19, Q39; RESEARCH §01 |
| Streaming | Kafka 4.3.1 (`apache/kafka` on Compose; Strimzi 1.2.0 KRaft on kind), confluent-kafka 2.15.x | DISCOVERY Q15; RESEARCH §02 |
| Services | FastAPI 0.142, uvicorn (1 worker/pod), psycopg 3.3 (COPY + executemany) | RESEARCH §02 |
| DB | PostgreSQL 18 (official image) | DISCOVERY Q23; RESEARCH §08 |
| Object store | SeaweedFS 4.48 (S3 API, port 8333) | ADR-0005 |
| Tracking / registry | MLflow 3.16.1 (server and clients pinned), aliases, prompt registry, GenAI evaluate | DISCOVERY Q16, Q28; RESEARCH §03, §05 |
| Orchestration | Airflow 3.3.2 on chart 1.22.0, LocalExecutor + KubernetesPodOperator (provider cncf-kubernetes 10.22.0), asset events | DISCOVERY Q41; RESEARCH §04 |
| Batch | PySpark 4.2.0 local mode, OpenJDK 21, hadoop-aws 3.5.0 + AWS SDK bundle 2.35.4 | DISCOVERY Q29; RESEARCH §04 |
| LLM | vLLM 0.30.0, `mistralai/Ministral-3-8B-Instruct-2512` (FP8); fallback `RedHatAI/Qwen3.5-9B-quantized.w4a16` | ADR-0004; RESEARCH §05 |
| Kubernetes | kind 0.33.0 (`kindest/node:v1.37.0@sha256:a1ed56cf…`), nvkind @`c5705049`, NVIDIA device plugin 0.20.1, Helm, Argo CD 3.5.3 | ADR-0003; RESEARCH §05, §06, §08 |
| CI/CD | gitlab.com + self-hosted GitLab Runner 19.4, Buildah 1.43, Trivy 0.75.0 (pinned by digest), SonarQube Cloud Free, Schemathesis 4.29, k6 2.3.0 | DISCOVERY Q27; RESEARCH §06 |
| Images | `registry.access.redhat.com/ubi9/ubi-minimal` + uv-installed CPython 3.13, `USER 1001`, port 8080 | DISCOVERY Q43; RESEARCH §06 |
| IaC | Terraform 1.16.x, `hashicorp/google ~> 8.5`, tflint 0.64 + google ruleset 0.40.0, checkov 3.3.21, `terraform test` with `mock_provider` | DISCOVERY Q26; RESEARCH §07 |

## Architecture overview

```
            Part A: Docker Compose (host-run orchestrator)       Part B: kind + Argo CD (Airflow orchestrates)
 ┌────────────┐  silver snapshot  ┌──────────────┐  {env}.transactions  ┌───────────────────────────┐
 │ ingest job │ ───────────────▶ │ replayer +    │ ───────────────────▶ │ scoring service (FastAPI) │
 │ (pandas →  │  S3 (SeaweedFS)  │ shift injector│  key = card_hash      │ consumer thread:          │
 │  Spark B)  │                  │ Simulated clk │                       │ update_state → features   │
 └────────────┘                  └──────┬───────┘                       │ champion + challenger     │
        │ oracle_labels, shift_events    │ sim_clock / sim_control       │ Decision + Exploration    │
        ▼                                ▼                               └────────┬──────────────────┘
 ┌──────────────────────── PostgreSQL (per-env DB) ───────────────────────────────┘ predictions, card_state,
 │ predictions · card_state · consumer_offsets · labels · oracle_labels · drift_metrics · alerts ·        offsets (1 txn)
 │ shift_events · shift_audit · batch_scores · model_promotions · pipeline_requests · case_summaries
 └──────▲──────────────▲─────────────────────────▲───────────────────────────▲──────────────
        │              │                         │                           │
 ┌──────┴─────┐ ┌──────┴────────┐        ┌───────┴─────────┐         ┌───────┴──────────┐
 │ labeler    │ │ monitoring    │ alert  │ PipelineTrigger │ ──────▶ │ retrain / promote│ ──▶ MLflow registry
 │ delayed    │ │ PSI/KS, action│ ─────▶ │ local (A) or    │         │ batch-score jobs │     @champion
 │ Labels     │ │ rates, CBPE   │        │ Airflow asset(B)│         │ (KPO pods in B)  │     @challenger
 └────────────┘ └───────────────┘        └─────────────────┘         └──────────────────┘
                                                        case-summary service ──▶ vLLM (GPU) ──▶ MLflow traces/prompts
```

Data flow: ingest writes immutable snapshots. The replayer streams the replay window, injects Shifts, and records ground truth and oracle truth. The scorer applies the shared feature function and the Decision policy, and commits predictions, card state and Kafka offsets in one Postgres transaction. The labeler releases delayed Labels. Monitoring closes sim-day windows and writes alerts. The PipelineTrigger starts retraining, which registers Challengers. The promotion job compares Champion and Challenger on Matured labels (IPW over the Exploration sample) and moves aliases. The scorer hot-reloads the aliases.

## Decisions
Choices made while planning, inside the settled DISCOVERY decisions and ADRs:

1. **Part A / Part B.** DISCOVERY's "Phase 1 (~2 days core loop)" is **Part A = plan phases 1–5**. "Phase 2 (platform, ~3–4 days)" is **Part B = phases 6–11**. Part A must demo on its own. Part B is ordered so it can stop after any phase with a coherent story (DISCOVERY Q24, interview date unknown).
2. **Part A orchestration.** A host-run `fraud orchestrate` loop calls job containers with `docker compose run`. Airflow arrives in Part B (P8). Both go through one `PipelineTrigger` port (adapters `LocalTrigger` and `AirflowAssetTrigger`), so services never know which orchestrator is running. This follows the RESEARCH §08 Q4 abstractions and keeps Part A within 2 days.
3. **Demo timeline.** `make demo` replays 2020-01-01 → 2020-07-31 (no December, which has its own natural label shift) at speedup 8640 (1 sim day ≈ 10 s). The default speedup elsewhere is 2880 (Q10). The replayer pauses while gated jobs run (Q41). Batch scoring runs every K = 7 sim days.
4. **Alert budget.** 1% total: block 0.3%, review 0.7% (Q12). Thresholds are frozen per model version (Q39). For the promotion comparison, each model's threshold is its own quantile over all traffic, so they are compared at equal budget (RESEARCH §01 Q5).
5. **Features.** Exclude `gender`, `dob`, names and street. High-cardinality `merchant`, `city` and `job` become frequency or rate features, never LightGBM categoricals. Categoricals (`category`, `hour`, `state`) are stable integer codes from a frozen, versioned vocabulary (RESEARCH §03).
6. **Covariate scenario.** Keep ×1.8 on `gas_transport` + `grocery_pos`, and document that it also shifts P(Y|X) in the scaled region (RESEARCH §01 Q2).
7. **Oracle truth.** The replayer writes the true `is_fraud` of every Transaction, injected ones included, to `oracle_labels`. That table is read only by evaluation and reporting code, never by features or training. A test enforces this (import-boundary check).
8. **Kafka and DB per env.** One Kafka cluster. Topics are prefixed `local.`, `staging.` and `prod.`, and consumer groups are per env. One Postgres with databases `mlflow`, `airflow`, `fraud_local` (Part A), `fraud_staging` and `fraud_prod` (DISCOVERY Q37).
9. **Repo visibility and images.** The gitlab.com project is **public**: SonarQube Cloud Free needs it, and kind can pull from `registry.gitlab.com` anonymously. Argo CD-managed manifests reference GitLab registry images by `git-<sha>`. A local registry `localhost:5001` serves the developer inner loop on kind only. Compose uses locally built images with the same names.
10. **Per-env pipeline code.** Two Airflow `GitDagBundle`s, `staging` (tracks `main`) and `prod` (tracks the latest `v*` tag). DAG ids are prefixed with the env. This overrides RESEARCH §04's "DAGs baked into the image" suggestion in order to close the Level 2 per-env pipeline-CD gap (RESEARCH §08).
11. **GitOps layout.** An app-of-apps root Application for platform services, plus one ApplicationSet for the apps (`staging` → `main`, `prod` → `v*`), all in the same repo under `deploy/` (RESEARCH §06).
12. **Card hashing.** `card_hash = HMAC-SHA256(cc_num, CARD_HASH_KEY)`, with the key from `.env` or a Secret (DISCOVERY Q22).
13. **LLM judge.** By default the judge is the same local vLLM model, advisory only and documented as a compromise. A hosted judge is optional, enabled only when an API key env var is set, and never required by CI (RESEARCH §05).
14. **Cluster topology** is decided by the GPU spike (P7.T2) and recorded in ADR-0006. Use nvkind with a control-plane plus one GPU worker if it works within the ~2 h timebox. Otherwise use a single-node kind cluster and run vLLM as a Docker container on the `kind` network.
15. **Schema migrations.** Numbered plain-SQL files applied by `fraud db migrate`, a small runner with a `schema_migrations` table. No ORM.
16. **Added from ARCHITECTURE.md (2026-10-04):** P1.T9 (structured logging and invariant guard tests), to enforce the invariants listed in ARCHITECTURE.md.
17. **Added from THREAT-MODEL.md (2026-10-07):** P1.T10 (secret scan, Action pinning, dependency audit; TM-102, TM-103), P6.T10 (self-hosted runner hardening; TM-101, TM-104, TM-105) and P11.T8 (go-public checklist; TM-102, TM-104). P1.T9 also checks that published ports bind to 127.0.0.1 (TM-001).

## Out of scope
- Java, Keras/TensorFlow, Jenkins (mentioned in the README), Oracle, Elasticsearch, MongoDB (DISCOVERY Q31).
- Databricks, Composer, Vertex AI, Managed Kafka, Feast / Vertex Feature Store, KServe, vLLM production-stack, Argo CD Image Updater, promptfoo, DeepEval, Ragas: these appear in build-vs-buy only (Q26, Q35, RESEARCH §03, §05, §06).
- `terraform plan` / `apply`, terratest, real GCP credentials (ADR-0003).
- Delta Lake / Iceberg, DVC (Q30, RESEARCH §04).
- Redis or an online feature store (ADR-0002); Kafka transactions / EOS (RESEARCH §02).
- kube-prometheus-stack (RESEARCH §08). Grafana is optional (P11.T5).
- Real authentication on local services; multi-node HA; GKE Autopilot; OpenTofu in CI (Q22, RESEARCH §07).
- Custom SonarQube quality gates, which need a paid plan (RESEARCH §06).
- LLM fine-tuning.

## Phases at a glance
| Phase | Name | Outcome | Tasks |
|---|---|---|---|
| 1 | Repo and local platform skeleton | `make up` runs Postgres, Kafka, SeaweedFS and MLflow on Compose; `make ingest` writes validated snapshots | 10 |
| 2 | Features, Champion and streaming scoring | 2020 replays through Kafka; the scorer writes Champion predictions with exactly-once effects | 7 |
| 3 | Decisions, Labels and the feedback loop | Decisions, the Exploration sample and delayed Labels flow; IPW metrics exist | 6 |
| 4 | Shift injection and monitoring | Shifts are injected with ground truth; drift and action-rate alerts fire; null replay calibrated | 7 |
| 5 | Continual learning and `make demo` (end of Part A) | Alert → retrain → Shadow → promote, unattended, with `reports/results.md` | 11 |
| 6 | GitLab CI pipeline | Lint, test, Sonar, UBI builds, Trivy and model gates run on the self-hosted runner; images pushed | 10 |
| 7 | kind platform and GitOps | Argo CD deploys platform and apps; `main` → staging with k6 and contract verification; tag → prod | 10 |
| 8 | Airflow and Spark on kind | Asset-triggered DAGs run Spark medallion, retraining, batch scoring and promotion per env | 8 |
| 9 | LLM Case summaries | vLLM serves Ministral 3 8B; the case-summary service writes summaries; the prompt eval gate runs in CI | 8 |
| 10 | GCP Terraform | Portable GKE / Cloud SQL / GCS code passes fmt, validate, test, tflint and checkov in CI | 7 |
| 11 | Docs, Level 2 mapping and interview pack | README, build-vs-buy, Level 2 table, walkthrough, rehearsal done | 8 |

---

## Phase 1 — Repo and local platform skeleton
**Outcome:** `make up` brings up Postgres, Kafka, SeaweedFS and MLflow on Docker Compose, all healthy. `make ingest` turns the Kaggle CSVs into validated bronze and silver Parquet snapshots with a SHA-256 manifest.
**Depends on:** none

### P1.T1 — Repo scaffold and uv workspace
- **What:** Create the uv workspace. Root `pyproject.toml` with `requires-python = "==3.13.*"`. Members are `packages/fraud-core` (import name `fraud`) and `services/*`, added as they are created. Add dev deps (ruff, mypy, pytest, pytest-cov, pre-commit) and a pinned `pandas>=2.2,<3`. Set coverage `relative_files = True`. `.gitignore` covers `data/`, `.env`, `reports/*.png` caches and `.uv-cache`. Add `.env.example` with every variable from P1.T2, a `.pre-commit-config.yaml` (ruff, ruff-format, mypy, end-of-file) and a `Makefile` with stub targets (`up`, `down`, `ingest`, `test`, `lint`, `demo`). Write a README stub that links DISCOVERY, RESEARCH and PLAN.
- **Files:** `pyproject.toml`, `uv.lock`, `packages/fraud-core/pyproject.toml`, `packages/fraud-core/src/fraud/__init__.py`, `.gitignore`, `.env.example`, `.pre-commit-config.yaml`, `Makefile`, `README.md`, `tests/test_smoke.py`
- **Depends on:** none
- **Source:** DISCOVERY Q3, Q22, Q30, Q43; RESEARCH §04 (pandas pin)
- **Done when:** `uv sync && uv run pytest` passes, `uv run pre-commit run --all-files` is clean, and `git status` shows no `data/` or `.env` tracked.

### P1.T2 — Settings and infrastructure adapters (ports and adapters)
- **What:** In `fraud.config`, add one pydantic-settings `Settings` class that reads env vars: `ENV`, `DATABASE_URL`, `KAFKA_BOOTSTRAP`, `TOPIC_PREFIX`, `S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `LAKE_BUCKET`, `MLFLOW_TRACKING_URI`, `MODEL_NAME`, `CARD_HASH_KEY`, `EXPLORATION_RATE`, `CANARY_PCT`, `SPEEDUP`, `LLM_BASE_URL`. In `fraud.ports`, define Protocols `ObjectStore`, `Clock`, `PipelineTrigger` and `ModelRegistry`. In `fraud.adapters`, implement `S3ObjectStore` (boto3, endpoint override, put-if-absent for write-once) and a psycopg connection factory. Code never branches on "am I in k8s".
- **Files:** `packages/fraud-core/src/fraud/config.py`, `.../ports.py`, `.../adapters/s3.py`, `.../adapters/db.py`, `tests/unit/test_config.py`, `tests/unit/test_s3_adapter.py`
- **Depends on:** P1.T1
- **Source:** DISCOVERY Q30; RESEARCH §08 Q4 (12-factor config)
- **Done when:** unit tests pass. The S3 adapter is tested against moto, and a second write to an existing key raises.

### P1.T3 — Compose infrastructure stack
- **What:** `deploy/compose/docker-compose.yml` with these services:
  - `postgres:18`, with an init script creating databases `mlflow`, `airflow` and `fraud_local`;
  - `apache/kafka:4.3.1` single-node KRaft, with an init container creating `local.transactions` (6 partitions, `retention.ms=-1`), `local.labels` and `local.sim-clock`;
  - SeaweedFS 4.48 in server mode with S3 on 8333 and a bucket-init job for `lake` and `mlflow` (versioning enabled);
  - an MLflow image `FROM ghcr.io/mlflow/mlflow:v3.16.1`, adding `psycopg2-binary boto3 'mlflow[auth]'`, run with `--workers 1`, a Postgres backend, `--artifacts-destination s3://mlflow`, `MLFLOW_S3_ENDPOINT_URL`, and `--allowed-hosts` that include the service names.

  Network aliases match the future k8s Service names (`postgres`, `fraud-kafka-kafka-bootstrap`, `s3`, `mlflow`). Every service has a healthcheck and `deploy.resources.limits.memory` matching the RESEARCH §08 slim budget. Add Makefile targets `up`, `down`, `ps` and `logs`.
- **Files:** `deploy/compose/docker-compose.yml`, `deploy/compose/postgres-init.sql`, `deploy/compose/kafka-topics.sh`, `deploy/compose/seaweedfs-s3.json`, `images/mlflow/Containerfile`, `Makefile`
- **Depends on:** P1.T1
- **Source:** DISCOVERY Q23, Q36, Q38; RESEARCH §02 Q5 (retention), §03 Q4, §04 Q6, §08 Q4
- **Done when:** after `make up`, every container reports `healthy`. `curl -sf localhost:5000/health` succeeds. `aws --endpoint-url http://localhost:8333 s3 ls` lists `lake` and `mlflow`. `kafka-topics.sh --describe` shows 6 partitions on `local.transactions`. Total `docker stats` memory is ≤ 6 GiB.

### P1.T4 — Fraud database schema and migration runner
- **What:** Add `fraud db migrate`, which applies `migrations/NNN_*.sql` in order and records them in `schema_migrations`. It is idempotent. Initial tables:
  - `predictions`: txn_id, card_hash, event_time, role, model_version, score, decision, would_be_decision, explored, propensity, applied, champion_version, champion_score, kafka_partition, kafka_offset; primary key (txn_id, role).
  - State and clock: `card_state`, `consumer_offsets`, `sim_clock` (single-row watermark), `sim_control` (pause flag, active scenarios), `replay_progress`.
  - Labels: `labels`, `oracle_labels`.
  - Shifts and monitoring: `shift_events`, `shift_audit`, `drift_metrics`, `alerts`, `monitor_state`.
  - Pipeline and models: `pipeline_requests` (outbox), `batch_scores`, `model_promotions` (append-only), `retrain_runs`.
- **Files:** `packages/fraud-core/src/fraud/db/migrate.py`, `migrations/001_core.sql`, `packages/fraud-core/src/fraud/cli.py` (Typer app `fraud`), `tests/integration/test_migrate.py`
- **Depends on:** P1.T2, P1.T3
- **Source:** DISCOVERY Q23, Q44; RESEARCH §02 Q3, §03 Q5
- **Done when:** running `uv run fraud db migrate` twice against Compose Postgres succeeds both times. The integration test (pytest marker `integration`) checks that all tables exist.

### P1.T5 — Transaction data contract (pandera)
- **What:** In `fraud.contracts`, define one neutral column `SPEC` (name → pandas dtype, Spark dtype, checks, nullable) for the raw 23 Sparkov columns and for the silver schema (`trans_num`, `card_hash`, `event_time`, `category` in the 14 values, `amt` between 0 and 50,000, `merchant` without the `fraud_` prefix, lat/long, `city_pop`, `state`, `job`, `is_fraud` in {0,1}). `pandas_schema()` builds the `pandera.pandas` schema. A `SchemaSkew` exception carries the errors. `spark_schema()` is a stub, finished in P8.T2.
- **Files:** `packages/fraud-core/src/fraud/contracts/transactions.py`, `tests/unit/test_contracts.py`
- **Depends on:** P1.T1
- **Source:** DISCOVERY Q35; RESEARCH §01 Q1 (schema), §04 Q5
- **Done when:** tests pass. A valid fixture validates, and fixtures with a wrong dtype, an unknown category, a negative amount or an extra column each raise `SchemaSkew`.

### P1.T6 — Sample fixture and data download guide
- **What:** `docs/data.md` covers the manual Kaggle download (`kartik2112/fraud-detection`, CC0) into `data/raw/`, the expected row counts (1,296,675 / 555,719) and a warning about truncated mirrors. `tests/fixtures/sparkov_sample.csv` holds about 3k real rows (CC0 allows it) spanning both years, every category, and some fraud episodes, chosen to cover whole card episodes. Add a script to regenerate it.
- **Files:** `docs/data.md`, `tests/fixtures/sparkov_sample.csv`, `scripts/make_fixture.py`
- **Depends on:** P1.T1
- **Source:** ADR-0001; RESEARCH §01 Q1 (quirks, mirrors)
- **Done when:** the fixture loads and passes the raw pandera schema. `docs/data.md` lists the download steps and checks.

### P1.T7 — Ingest job (bronze → silver snapshots)
- **What:** `fraud ingest` does the following:
  - reads `data/raw/fraudTrain.csv` and `fraudTest.csv`, and asserts the row counts (overridable for the fixture);
  - concatenates them, stable-sorts by `trans_date_trans_time`, drops `Unnamed: 0`, ignores `unix_time` for time, strips `fraud_` from `merchant`, and sets `card_hash = HMAC(cc_num)`;
  - parses `event_time` (naive, treated as UTC) and validates with pandera;
  - writes bronze (raw Parquet) and silver (clean) to write-once prefixes `s3://lake/{bronze,silver}/transactions/snapshot=<id>/`, each with `_MANIFEST.json` (files plus per-file SHA-256 and a manifest SHA-256);
  - also writes derived views `silver/train_2019` and `silver/replay_2020`.
- **Files:** `packages/fraud-core/src/fraud/jobs/ingest.py`, `packages/fraud-core/src/fraud/lineage.py` (manifest hashing), `tests/unit/test_ingest.py`, `tests/unit/test_lineage.py`
- **Depends on:** P1.T2, P1.T5, P1.T6
- **Source:** DISCOVERY Q22, Q30, Q45; RESEARCH §01 Recommendation 1
- **Done when:** `make ingest` on the real data prints the manifest SHA-256 and the row counts 924,850 (2019) and 927,544 (2020). Re-running with the same snapshot id fails as write-once. The fixture test passes.

### P1.T8 — Lineage helper for MLflow runs
- **What:** `fraud.lineage.log_lineage(run)` sets these tags: `data_sha256` (the manifest hash), `snapshot_uri`, `git_sha` (from `GIT_SHA` env or `git rev-parse`), `image_digest` (from `IMAGE_DIGEST` env), `trigger_reason`, plus a `config.json` artifact. It never relies on the `mlflow.data` digest.
- **Files:** `packages/fraud-core/src/fraud/lineage.py`, `tests/unit/test_lineage.py`
- **Depends on:** P1.T7
- **Source:** DISCOVERY Q30, Q45; RESEARCH §03 Q3, §08 Q3 (metadata gaps)
- **Done when:** a unit test against a local `sqlite:///` MLflow store asserts that all tags and the artifact are present.

### P1.T9 — Structured logging and invariant guard tests
- **What:** `fraud.logging.configure()` sets up JSON logs to stdout, with fields `service`, `env`, `sim_time` and `txn_id` when known, and a filter that rejects anything shaped like a raw card number (13–19 digits) by masking it. Every service and the CLI call it at start. Add a guard test module that enforces the ARCHITECTURE.md invariants that can be checked statically:
  - every Kafka producer config built by `fraud` code sets `partitioner=murmur2_random` and idempotence;
  - no `datetime.now`/`time.time` in `fraud.features`, `fraud.jobs` (except `benchmark`) or the services' business logic;
  - no `timestamp=` argument on Kafka `produce` calls;
  - Helm and Compose manifests never use `:latest` image tags and always set memory limits.
  - every published Compose port is bound to `127.0.0.1` (`127.0.0.1:<host>:<container>`), and kind `extraPortMappings` set `listenAddress: 127.0.0.1` (THREAT-MODEL TM-001).

  Extend it as new invariants become checkable.
- **Files:** `packages/fraud-core/src/fraud/logging.py`, `tests/unit/test_invariants.py`, `tests/unit/test_logging.py`
- **Depends on:** P1.T2
- **Source:** ARCHITECTURE.md Invariants; DISCOVERY Q22
- **Done when:** both tests are green. A temp module that violates any rule makes `test_invariants.py` fail, and logging a 16-digit number outputs it masked.

### P1.T10 — Repo security baseline (secret scan, pinning, dependency audit)
- **What:** Add a `gitleaks` hook to `.pre-commit-config.yaml` and a blocking `secrets` job to `.github/workflows/ci.yml` (`gitleaks` pinned by digest or SHA). Pin every GitHub Action by commit SHA with a `# vX.Y.Z` comment. Add `.github/dependabot.yml` for `github-actions` and `uv`. Add a dependency audit job that scans `uv.lock` (for example `uvx pip-audit` on an exported requirements file, or `trivy fs`) and fails on critical or high findings.
- **Files:** `.pre-commit-config.yaml`, `.github/workflows/ci.yml`, `.github/dependabot.yml`
- **Depends on:** P1.T1
- **Source:** THREAT-MODEL TM-102, TM-103
- **Done when:** committing a fake AWS key is blocked by pre-commit and by CI; every `uses:` line is a 40-character SHA; the audit job runs green on `main`.

### Phase 1 checkpoint
`make up && uv run fraud db migrate && make ingest`, then check that `aws --endpoint-url http://localhost:8333 s3 ls s3://lake/silver/transactions/ --recursive` shows the snapshot with `_MANIFEST.json`, and that `uv run pytest -m "not integration"` and `uv run pytest -m integration` are both green.

---

## Phase 2 — Features, Champion and streaming scoring
**Outcome:** a Champion trained on 2019 is registered in MLflow. The replayer streams 2020 on the Simulated clock. The scoring service computes features with the shared state function, scores, and writes predictions, card state and offsets in one transaction. Killing and restarting it loses or duplicates nothing.
**Depends on:** Phase 1

### P2.T1 — Per-card state function (ADR-0002)
- **What:** `fraud.features` provides `empty_state(card_hash)` and a pure `update_state(state, txn) -> (state, FeatureVector)`. "Now" is always `txn.event_time`. Features:
  - amount: `amt`, `log_amt`;
  - categorical codes: `hour`, `category_code`, `state_code`, all from the frozen `vocab.json`;
  - velocity: transaction counts over 1h, 24h and 7d; amount sum over 24h;
  - card behaviour: z-score of the amount against the card's running mean and std (Welford); seconds since the last transaction; distinct categories over 24h; how often the card has used this merchant; home-to-merchant haversine distance; `log_city_pop`; a job-frequency feature.

  Exclude `gender` and `dob`. State is JSON-serialisable with bounded deques. `FEATURE_NAMES` and `CATEGORICAL_INDEX` are constants, and the vocab version is in the state.
- **Files:** `packages/fraud-core/src/fraud/features/__init__.py`, `.../features/state.py`, `.../features/vocab.json`, `tests/unit/test_features.py`
- **Depends on:** P1.T5
- **Source:** ADR-0002; DISCOVERY Q15; RESEARCH §01 Q1 (quirks), §03 (stable codes, high cardinality); PLAN Decision 5
- **Done when:** every feature has a unit test. The same input sequence always gives identical outputs. A JSON round-trip of the state is lossless. A grep test confirms no `datetime.now` in `fraud/features`.

### P2.T2 — Offline training-set builder (pandas replay)
- **What:** `fraud build-dataset --snapshot <id> --as-of <sim date> --window-days N --label-source {oracle_history,matured}`. For each card it sorts by `(event_time, trans_num)` and replays through `update_state`. It joins Labels:
  - `oracle_history` (2019 pre-platform: everything labelled);
  - `matured` (filled in by P3.T4).

  It writes gold Parquet to `s3://lake/gold/training/snapshot=<id>/` with a manifest, and records wall time.
- **Files:** `packages/fraud-core/src/fraud/jobs/build_dataset.py`, `tests/unit/test_build_dataset.py`
- **Depends on:** P2.T1, P1.T7
- **Source:** ADR-0002; DISCOVERY Q10; RESEARCH §04 Q4 (sort inside)
- **Done when:** gold 2019 is produced from the real snapshot and its row count equals 924,850. The fixture test passes with input rows shuffled.

### P2.T3 — Train and register the initial Champion
- **What:** `fraud train --mode stateless --dataset <gold uri>`. Steps:
  1. Time split: 2019-01..10 for training, 2019-11..12 for validation.
  2. Train with `lgb.train`: fixed params, `categorical_feature=CATEGORICAL_INDEX`, `free_raw_data=False`.
  3. Fit isotonic calibration on validation.
  4. Freeze thresholds for the Alert budget (block top 0.3%, review next 0.7%) on validation.
  5. Build the drift reference profile: reference-quantile bin edges per numeric feature, per-category `amt`, categorical frequencies, and score edges at quantiles 0.5/0.9/0.99/0.997/0.999.
  6. Log to MLflow with `log_model(metadata={thresholds, alert_budget, vocab_version, feature_names})`, `thresholds.json`, `reference_profile.json`, the calibrator, train and test metrics (PR-AUC, fraud-dollar recall at budget) and lineage.
  7. Register `fraud-lgbm` and set `@champion` when no champion exists.
- **Files:** `packages/fraud-core/src/fraud/jobs/train.py`, `.../model/calibration.py`, `.../model/thresholds.py`, `.../monitoring/reference.py`, `tests/unit/test_thresholds.py`
- **Depends on:** P2.T2, P1.T8
- **Source:** DISCOVERY Q12, Q14, Q39; RESEARCH §01 Q3, Q4 (calibration, edges), §03 Q3
- **Done when:** the MLflow UI shows `fraud-lgbm` v1 at `@champion`, with thresholds in the model metadata, both artifacts present, and train and test metrics logged.

### P2.T4 — Train/serve parity test
- **What:** A test that replays a fixture card's Transactions through the batch builder (P2.T2) and through the scorer's in-process stream path (P2.T7 `StreamScorer` core, without Kafka), then asserts identical feature vectors and scores. It also checks that shuffled input to the batch path gives the same result.
- **Files:** `tests/unit/test_parity.py`
- **Depends on:** P2.T2, P2.T7
- **Source:** ADR-0002; DISCOVERY Q21
- **Done when:** the test is green and is part of the default `pytest` run.

### P2.T5 — Service images and Compose app wiring
- **What:** Write multi-stage Containerfiles on `ubi9/ubi-minimal`:
  - `uv python install 3.13` and `uv sync --frozen --no-dev --package <svc>`;
  - `USER 1001`, group-0 permissions, port 8080.

  Build one image each for `jobs` (the `fraud` CLI), `replayer`, `scoring`, `labeler` and `monitoring`; the last two start as stubs. `make build` tags them `localhost:5001/fraud/<svc>:git-<sha>`. Add the app services to Compose under a `apps` profile, configured by env from `.env`.
- **Files:** `images/python-base/Containerfile` (shared build stage), `services/{scoring,replayer,labeler,monitoring}/Containerfile`, `images/jobs/Containerfile`, `deploy/compose/docker-compose.apps.yml`, `Makefile`
- **Depends on:** P1.T3
- **Source:** DISCOVERY Q43; RESEARCH §06 Q4, §08 Q4
- **Done when:** `make build` succeeds. `docker run --rm localhost:5001/fraud/jobs:git-<sha> fraud --help` works, and `id -u` inside the container prints `1001`.

### P2.T6 — Replayer with Simulated clock
- **What:** `services/replayer` reads the silver replay window from S3 and drives an anchor-and-sleep `SimClock` (`SPEEDUP`, `--start`, `--end`). It produces to `{prefix}.transactions` with key `card_hash`, `partitioner=murmur2_random`, `enable.idempotence=true`, and event time in the payload and a header, never as the Kafka timestamp. Every sim hour it emits `{prefix}.sim-clock` heartbeats. It writes the true `is_fraud` to `oracle_labels` and its last produced event time to `replay_progress` (resumable). Each loop it honours the `sim_control.paused` flag. Injection hooks are a no-op until P4.T1.
- **Files:** `services/replayer/pyproject.toml`, `services/replayer/src/replayer/main.py`, `packages/fraud-core/src/fraud/simclock.py`, `tests/unit/test_simclock.py`
- **Depends on:** P2.T5, P1.T4
- **Source:** DISCOVERY Q10, Q41; RESEARCH §02 Q2, Q5; PLAN Decision 7
- **Done when:**
  - Replaying 2020-01-01..01-03 at speedup 8640 produces about 7.6k messages in about 30 s, spread over 6 partitions.
  - After a restart, it resumes from `replay_progress` without duplicates (checked by counting `trans_num` in the topic).
  - Setting `paused` stops production within 1 s.

### P2.T7 — Scoring service
- **What:** A FastAPI app (`services/scoring`) with:
  - **Consumer.** A `lifespan`-started consumer thread (`group.protocol=consumer`, auto-commit off). `on_assign` restores `card_state` and seeks to the offsets stored in `consumer_offsets`. `on_revoke` flushes. `on_lost` drops.
  - **Scoring.** Micro-batches of up to 500 messages or 200 ms. For each message, call `update_state`, then score the Champion (native LightGBM `num_threads=1`) and the Challenger if one exists, as a shadow with `applied=false`.
  - **Flush.** One Postgres transaction per flush: `COPY` into `predictions`, upsert dirty `card_state`, upsert `consumer_offsets`. It then updates the `sim_clock` watermark and commits Kafka offsets asynchronously, for lag reporting only.
  - **Models.** `ModelHolder` polls the aliases every 15 s, loads by resolved version, and swaps atomically.
  - **Endpoints.** A dry-run `POST /score` with no state mutation and no writes, plus `/healthz`, `/readyz` and `/metrics` (latency histogram, throughput, errors, lag).
  - Decision fields are filled by P3.T2.
- **Files:** `services/scoring/pyproject.toml`, `services/scoring/src/scoring/{app.py,stream.py,models.py,schemas.py}`, `tests/unit/test_scoring_api.py`, `tests/integration/test_exactly_once.py`
- **Depends on:** P2.T3, P2.T6, P2.T1
- **Source:** DISCOVERY Q15, Q16, Q44; RESEARCH §02 Q3, Q4; ADR-0002 amendment
- **Done when:**
  - With replayer and scorer running for 10 sim days, every replayed `trans_num` has exactly one `role='champion'` row.
  - The integration test kills the scorer with SIGKILL mid-run, restarts it, and asserts no duplicate and no missing `txn_id`.
  - `POST /score` returns 200 and leaves row counts unchanged.

### Phase 2 checkpoint
`make up && make build && uv run fraud train --mode stateless …`, then `docker compose --profile apps up replayer scoring` for 20 sim days. Check that `SELECT count(*), count(DISTINCT txn_id) FROM predictions WHERE role='champion'` returns equal values matching the replayed count. `pytest tests/unit/test_parity.py` is green.

---

## Phase 3 — Decisions, Labels and the feedback loop
**Outcome:** every Transaction gets a Decision under the frozen thresholds. Would-be blocks enter the Exploration sample at the configured rate, with propensities logged. The labeler releases delayed Labels and never labels unexplored blocks. Training data assembly and the IPW / SNIPS metrics are in place.
**Depends on:** Phase 2

### P3.T1 — Decision policy and Exploration sample
- **What:** `fraud.policy.decide(score, thresholds, txn_id, card_hash, cfg)` returns `would_be_decision` (block / review / allow from the frozen thresholds). If it would block, a deterministic uniform draw `u = H(txn_id, seed)` below `EXPLORATION_RATE` turns the Decision into allow with `explored=true` and `propensity=rate`. The draw happens **before** Canary routing and applies to both arms. `route(card_hash, canary_pct)` decides which arm applies (`int(card_hash[:8],16) % 100 < pct`). Rates come from config: default 0.02, demo profile 0.10.
- **Files:** `packages/fraud-core/src/fraud/policy.py`, `tests/unit/test_policy.py`
- **Depends on:** P2.T3
- **Source:** DISCOVERY Q5, Q12, Q40; RESEARCH §01 Q5 (propensity table), §03 Q5 (explore before routing)
- **Done when:** tests check the threshold boundaries, determinism of the draw, an exploration share within ±0.5 pp of the rate on 100k synthetic would-be blocks, and that routing is stable per card.

### P3.T2 — Wire Decisions into the scorer
- **What:** The scorer calls `decide` and `route`. Prediction rows carry `decision`, `would_be_decision`, `explored`, `propensity`, `champion_version`, `champion_score` and `applied`. Shadow Challenger rows store their own would-be decision with `applied=false`.
- **Files:** `services/scoring/src/scoring/stream.py`, `tests/unit/test_scoring_decisions.py`
- **Depends on:** P3.T1, P2.T7
- **Source:** DISCOVERY Q12, Q16
- **Done when:** after 30 sim days of replay, the applied block share is 0.3% ±0.1 pp and the review share 0.7% ±0.2 pp (before any shift). Explored rows equal the configured share of would-be blocks.

### P3.T3 — Labeler (delayed Labels)
- **What:** `services/labeler` follows the `sim_clock` watermark and, for each applied Decision, releases a Label:
  - **allow:** fraud arrives as a chargeback at `event_time + U(7,45)` sim days (seeded by `txn_id`); legit is confirmed at `+45` days;
  - **review:** labelled at `+1` day;
  - **block:** never labelled, unless explored, in which case it follows the allow path.

  It reads truth only from `oracle_labels`, writes `labels` (txn_id, label, label_time, source) and publishes to `{prefix}.labels`. It is idempotent on `txn_id`.
- **Files:** `services/labeler/pyproject.toml`, `services/labeler/src/labeler/main.py`, `tests/unit/test_label_delays.py`, `tests/integration/test_labeler.py`
- **Depends on:** P3.T2, P2.T6
- **Source:** DISCOVERY Q11; RESEARCH §01 Q4
- **Done when:**
  - The delay distribution test passes (KS against U(7,45)).
  - After 60 sim days, no `labels` row exists for an unexplored applied block (a SQL assertion in the integration test).
  - Every explored block older than 45 sim days has a Label.

### P3.T4 — Matured labels and censored training data assembly
- **What:** Add a SQL function or view `matured_labels(as_of)`: Labels with `label_time <= as_of`, plus legit confirmations once 45 days have passed. Extend `build-dataset --label-source matured` to:
  - join the 2019 oracle history (pre-platform, fully labelled) with replay-period matured Labels;
  - set weight `1/propensity` on explored rows;
  - drop unlabelled blocks;
  - offer a `--weighting {ipw,naive}` flag.

  Optional: `--legacy-censoring` applies a simple rule-based block policy to late 2019, so censoring is visible early (RESEARCH §01 risk).
- **Files:** `migrations/002_matured_labels.sql`, `packages/fraud-core/src/fraud/jobs/build_dataset.py`, `tests/unit/test_censoring.py`
- **Depends on:** P3.T3, P2.T2
- **Source:** DISCOVERY Q18, Q40; RESEARCH §01 Q5
- **Done when:** on a synthetic decision table, the IPW dataset's weighted fraud count matches the oracle within the expected tolerance and the naive dataset undercounts. Tests are green.

### P3.T5 — Feedback-loop metrics library
- **What:** `fraud.metrics` provides:
  - Horvitz–Thompson and SNIPS fraud-dollar recall and precision at an equal budget;
  - weighted PR-AUC (`average_precision_score(sample_weight=)`);
  - Kish effective sample size (ESS);
  - bootstrap by sim-day for confidence intervals;
  - oracle-evaluation helpers that read `oracle_labels` and are allowed only in `fraud.evaluation`.
- **Files:** `packages/fraud-core/src/fraud/metrics.py`, `packages/fraud-core/src/fraud/evaluation/oracle.py`, `tests/unit/test_metrics.py`
- **Depends on:** P3.T1
- **Source:** DISCOVERY Q16, Q18; RESEARCH §01 Q5
- **Done when:** tests reproduce the qualitative result of the RESEARCH §01 synthetic month: oracle above naive, and IPW close to oracle inside the CI.

### P3.T6 — Oracle import boundary test
- **What:** A test, using an AST import scan, that fails if any module under `fraud.features`, `fraud.jobs.train`, `fraud.jobs.build_dataset` (except the documented 2019 history path) or `services/scoring` references `oracle_labels`. Only `fraud.evaluation`, `fraud.jobs.report` and the labeler may read it.
- **Files:** `tests/unit/test_oracle_boundary.py`
- **Depends on:** P3.T4, P3.T5
- **Source:** PLAN Decision 7; DISCOVERY Q5
- **Done when:** the test is green, and it fails when a forbidden reference is added to a temp module.

### Phase 3 checkpoint
Replay 90 sim days and run:
- `SELECT decision, explored, count(*) FROM predictions WHERE role='champion' GROUP BY 1,2`, which shows blocks, reviews and explored rows;
- `SELECT count(*) FROM labels l JOIN predictions p USING (txn_id) WHERE p.decision='block' AND NOT p.explored`, which returns 0.

`uv run pytest` is green.

---

## Phase 4 — Shift injection and monitoring
**Outcome:** covariate, label and concept Shifts can be switched on with logged ground truth. The monitoring job closes sim-day windows and raises alerts from feature drift, per-category drift, action rates and delayed-label estimators. A null replay measures false alarms, and the concept scenario is proven to be a blind spot.
**Depends on:** Phase 3

### P4.T1 — Scenario model and Shift injector
- **What:** `fraud.shift` defines `Scenario(scenario_id, kind, start, end, seed, params, ramp)` with a sigmoid ramp and a pure `inject(chunk, active) -> (chunk, audit_rows)` that sits between the replay reader and the producer. Scenarios:
  - **covariate:** `amt ×1.8` in `gas_transport` and `grocery_pos`;
  - **label:** about 3× fraud via **whole fraud-episode cloning** from the 2019 pool onto synthetic `clone-…` cards, keeping hour of day and offsets;
  - **concept:** `home` category, 00:00–03:59, $0.50–$3.00, 10–20 existing cards per day × 1–3 Transactions.

  Injected rows get oracle truth. Ground truth goes to `shift_events` and `shift_audit`. Scenarios are loaded from a YAML file, or toggled live with `fraud shift on|off <kind>` (through `sim_control`).
- **Files:** `packages/fraud-core/src/fraud/shift.py`, `services/replayer/src/replayer/main.py`, `config/scenarios/*.yaml`, `tests/unit/test_shift.py`
- **Depends on:** P2.T6
- **Source:** DISCOVERY Q13, Q42; RESEARCH §01 Q2; PLAN Decision 6
- **Done when:**
  - Unit tests on the fixture show that covariate scales only the chosen categories inside the window, label cloning roughly triples fraud with intact episodes, and concept adds night-only rows.
  - Rows produced in the window carry the same partitioner key.
  - `shift_audit` covers every touched `trans_num`.

### P4.T2 — Drift statistics library
- **What:** `fraud.monitoring.stats` provides:
  - `psi_numeric` with stored edges and ε = 1e-4, and `psi_categorical`;
  - `ks_drift` (`method="asymp"`: D, p, normalised Wasserstein);
  - `psi_null_threshold` (Yurdakul–Naranjo χ²);
  - a one-sided `binomtest` wrapper for action rates;
  - a `Streak` state machine (fires at 2 consecutive windows).
- **Files:** `packages/fraud-core/src/fraud/monitoring/stats.py`, `tests/unit/test_drift_stats.py`
- **Depends on:** P1.T1
- **Source:** DISCOVERY Q19, Q39; RESEARCH §01 Q3, Q4
- **Done when:** the tests match the RESEARCH §01 synthetic checks: no-shift PSI ≈ 0.0004, and for ×1.8 on 1/7 of rows D ≈ 0.029 with p < 1e-10. The streak fires only on the second consecutive alert.

### P4.T3 — Monitoring service
- **What:** `services/monitoring` is a loop driven by the `sim_clock` watermark. For each closed sim day it computes, against the Champion's `reference_profile.json`:
  - per-feature PSI and KS;
  - per-category `amt` PSI and KS;
  - categorical PSI for `category` and `hour`;
  - score PSI on tail-aware bins, plus mean and p99, p99.7 and p99.9;
  - block and review rates at the frozen cutoffs, against the expected budget (binomial test);
  - operational metrics: throughput, p50/p99 latency, consumer lag and error counts, scraped from the scorer's `/metrics`.

  It writes everything to `drift_metrics`. The Q19 rule plus per-category, action-rate and streak ≥ 2 checks write to `alerts`. After the alert commits, it calls `PipelineTrigger.emit("drift", …)`. Streak state persists in `monitor_state`.
- **Files:** `services/monitoring/pyproject.toml`, `services/monitoring/src/monitoring/{main.py,windows.py,rules.py}`, `tests/unit/test_rules.py`, `tests/integration/test_monitoring.py`
- **Depends on:** P4.T2, P3.T2, P4.T6
- **Source:** DISCOVERY Q19, Q23, Q39; RESEARCH §01 Q3–Q4, §02 Q5
- **Done when:** with the covariate scenario on from sim day 20, an alert of kind `feature_drift` or `category_drift` naming `amt` appears within 3 sim days. With label ×3 on, an `action_rate` alert appears within 3 sim days. Integration test is green.

### P4.T4 — Delayed-label estimators
- **What:** `fraud.monitoring.delayed` provides:
  - CBPE-lite: expected precision at block and expected fraud-dollar recall from calibrated scores;
  - a delay-CDF-corrected early fraud-count estimate `observed_by_age_k / F(k)`;
  - Matured-label performance on Transactions at least 45 sim days old, including review and IPW-weighted explored rows;
  - a CBPE-vs-matured gap signal that raises a `concept_gap` alert.

  The monitoring service writes these daily.
- **Files:** `packages/fraud-core/src/fraud/monitoring/delayed.py`, `services/monitoring/src/monitoring/main.py`, `tests/unit/test_delayed.py`
- **Depends on:** P4.T3, P3.T4
- **Source:** DISCOVERY Q19; RESEARCH §01 Q4
- **Done when:** in a synthetic test the corrected count estimates the eventual fraud count within 10% from day 8. Under the label scenario, an `early_fraud_rate` alert fires before day 45.

### P4.T5 — Null replay calibration
- **What:** `fraud experiment null-replay` replays 2020-01..07 with no scenarios and counts alerts per rule. It writes `reports/null_replay.md` with false-alarm counts per rule and the Yurdakul–Naranjo null thresholds next to 0.2. Thresholds are not tuned silently: any change is recorded in `config/monitoring.yaml` with a comment and logged in the report.
- **Files:** `packages/fraud-core/src/fraud/jobs/experiments.py`, `reports/null_replay.md` (generated), `config/monitoring.yaml`
- **Depends on:** P4.T3, P4.T4
- **Source:** RESEARCH §01 Q2 (null replay first)
- **Done when:** the report exists with per-rule false-alarm counts over the window, and the counts are referenced by P5.T9.

### P4.T6 — PipelineTrigger port and LocalTrigger
- **What:** Implement `PipelineTrigger.emit(kind, payload)` with kinds `drift`, `retrain_due`, `sim_week_closed` and `shadow_window_done`. `LocalTrigger` inserts into the `pipeline_requests` outbox, deduplicated by (kind, sim_date). The replayer emits `retrain_due` every 30 sim days and `sim_week_closed` every 7.
- **Files:** `packages/fraud-core/src/fraud/trigger.py`, `services/replayer/src/replayer/main.py`, `tests/unit/test_trigger.py`
- **Depends on:** P1.T4
- **Source:** DISCOVERY Q20, Q41; PLAN Decision 2
- **Done when:** unit tests pass. A 60-sim-day replay produces 2 `retrain_due` and 8 `sim_week_closed` requests.

### P4.T7 — Concept blind-spot acceptance check
- **What:** `fraud check concept-blindspot --model @champion` generates the concept pattern (P4.T1 params, 1,000 rows over fixture cards) and scores it. It passes if fewer than 10% of rows score above the review threshold, meaning outside the top 1%. If it fails, it reports which parameter (hour band or amount) moves the pattern into the blind spot. The tuned parameters are recorded in `config/scenarios/concept.yaml` and in the report.
- **Files:** `packages/fraud-core/src/fraud/jobs/checks.py`, `config/scenarios/concept.yaml`, `tests/unit/test_concept_check.py` (with a tiny fixture model)
- **Depends on:** P4.T1, P2.T3
- **Source:** DISCOVERY Q42; RESEARCH §01 Q2
- **Done when:** the check passes against the real Champion, with parameters adjusted if needed, and the result is printed and saved to `reports/concept_check.json`.

### Phase 4 checkpoint
Run a replay with `config/scenarios/demo.yaml` (covariate on day 20, concept on day 60, label on day 110). Check that `SELECT kind, feature, min(sim_date) FROM alerts GROUP BY 1,2` shows an alert after each scenario start, and that `reports/null_replay.md` and `reports/concept_check.json` exist.

---

## Phase 5 — Continual learning and `make demo` (end of Part A)
**Outcome:** a drift alert pauses the replay and triggers retraining of Stateless, Stateful and naive Challengers. The best one runs in Shadow mode, then Canary, then is promoted by the Q16 gate with a full audit trail. Batch scores, the ONNX benchmark and `reports/results.md` are produced, all from one unattended `make demo`.
**Depends on:** Phase 4

### P5.T1 — Retraining job (Stateless vs Stateful, IPW vs naive)
- **What:** `fraud retrain --as-of <sim date> --trigger <request id>`. Steps:
  1. Run the pandera gate first; a `SchemaSkew` aborts and writes an alert.
  2. Build the dataset from Matured labels over a 12-month window.
  3. Train these Challengers:
     - **stateless:** from scratch, IPW weights;
     - **stateful:** `init_model=` the Champion booster on data since the last training, same `categorical_feature`, a tree cap that forces stateless when trees exceed 2× the baseline;
     - **naive:** stateless with no IPW, for comparison only, tagged `promotable=false`.
  4. For each Challenger: new calibration, frozen thresholds and a reference profile.
  5. Log `retrain_mode`, `parent_model_version`, tree count, wall-clock and CPU-seconds, train and test metrics, and lineage, including the trigger reason (alert ids).
  6. Register all of them. Set `@challenger` on the better of stateless and stateful by validation fraud-dollar recall.
- **Files:** `packages/fraud-core/src/fraud/jobs/retrain.py`, `packages/fraud-core/src/fraud/jobs/train.py`, `tests/unit/test_retrain_modes.py`
- **Depends on:** P3.T4, P2.T3
- **Source:** DISCOVERY Q14, Q18, Q20, Q35; RESEARCH §03 Q1, Recommendation
- **Done when:** one run produces three MLflow runs with the required tags, `retrain_runs` rows exist, and `@challenger` points to a promotable version.

### P5.T2 — Model safety tests (warm start, encoding, convergence, NaN)
- **What:**
  - Warm-start invariance: `b_new.predict(X, num_iteration=old_iters) == b_old.predict(X)`.
  - Categorical code stability when a new level appears.
  - Convergence: training loss decreases on the fixture, and the model overfits 50 rows.
  - No NaN or inf in features, predictions or LightGBM eval history.
  - ONNX parity, added in P5.T7.
- **Files:** `tests/unit/test_model_safety.py`
- **Depends on:** P5.T1
- **Source:** DISCOVERY Q21, Q33; RESEARCH §03 Risks; RESEARCH §08 Q3 (NaN gap)
- **Done when:** all tests are green in `pytest`.

### P5.T3 — Shadow evaluation and promotion gate
- **What:** `fraud promote evaluate --window <sim dates>` runs on Transactions whose Labels have matured, with the Exploration sample IPW-weighted. Champion and Challenger are compared at equal Alert budget, each at its own threshold.
  - **Metrics:** fraud-dollar recall (HT and SNIPS, bootstrap CI), weighted PR-AUC, and p99 single-row latency from the serving benchmark.
  - **Segment check:** by category, amount band and night/day; no segment may regress by more than 5 pp.
  - **Q16 rule:** at least +1 pp recall, no PR-AUC loss, p99 within budget, segment check passed.
  - **Recording:** an MLflow promotion run with `decision.json`, version tags `promotion_decision` and `promotion_run_id`, and an append-only `model_promotions` row.
  - **On pass:** shadow → canary, sets the `CANARY_PCT` config; canary → champion moves `@previous_champion` and `@champion`.
- **Files:** `packages/fraud-core/src/fraud/jobs/promote.py`, `packages/fraud-core/src/fraud/evaluation/gate.py`, `tests/unit/test_gate.py`
- **Depends on:** P3.T5, P5.T1
- **Source:** DISCOVERY Q16; RESEARCH §03 Q5, §01 Q5, §08 Q3 (segment gap)
- **Done when:** unit tests cover pass, fail on each rule, and fail on a segment regression. In a replay with a deliberately worse Challenger the gate rejects it and no alias moves.

### P5.T4 — Canary routing in the scorer
- **What:** The scorer reads `CANARY_PCT` (env in Part A, Git values in Part B). For cards where `route` is true, it applies the Challenger's Decision with `applied=true` on the Challenger row and `false` on the Champion row. The Exploration draw stays shared.
- **Files:** `services/scoring/src/scoring/stream.py`, `tests/unit/test_canary.py`
- **Depends on:** P3.T2
- **Source:** DISCOVERY Q16; RESEARCH §03 Q5
- **Done when:** with `CANARY_PCT=10`, the Challenger is applied for 10% ±1 pp of cards, and each card stays in one arm.

### P5.T5 — Local orchestrator (Part A)
- **What:** `fraud orchestrate` is a host-run loop that polls `pipeline_requests`:
  - **`drift` / `retrain_due`:** set `sim_control.paused`, run `docker compose run --rm jobs fraud retrain …`, then resume.
  - **`shadow_window_done`** (N = 14 sim days after `@challenger` is set): run `fraud promote evaluate`, then canary for 7 sim days, then evaluate again for promotion.
  - **`sim_week_closed`:** run `fraud score-batch`.

  It marks each request done or failed and logs every step.
- **Files:** `packages/fraud-core/src/fraud/orchestrate.py`, `tests/unit/test_orchestrate.py` (with a fake runner)
- **Depends on:** P4.T6, P5.T1, P5.T3, P5.T6
- **Source:** DISCOVERY Q20, Q41; PLAN Decisions 2, 3
- **Done when:** the unit test with a fake runner executes the full request sequence in order, and replay is paused during retraining.

### P5.T6 — Batch scoring per Account (every K sim days)
- **What:** `fraud score-batch --as-of <sim date>` runs the pandera gate on the input slice first. It then computes one risk score per Account (card): max and mean Champion score over the last 7 sim days, plus the Champion score of the current `card_state` features. It writes `batch_scores` and `s3://lake/gold/batch_scores/as_of=<date>/`.
- **Files:** `packages/fraud-core/src/fraud/jobs/score_batch.py`, `tests/unit/test_score_batch.py`
- **Depends on:** P2.T7
- **Source:** DISCOVERY Q29, Q41; RESEARCH §08 Q3 (pandera before batch prediction)
- **Done when:** after a 21-sim-day replay, `batch_scores` holds 3 as-of dates × all active cards.

### P5.T7 — ONNX export and latency benchmark
- **What:** `fraud benchmark --model @champion` does the following:
  - converts with `onnxmltools.convert_lightgbm(..., zipmap=False, target_opset=15)` and logs `mlflow.onnx` as a sibling artifact;
  - checks parity: max |Δp| < 1e-5 and identical Decisions at the stored thresholds;
  - benchmarks single-threaded on both sides (`num_threads=1`, ORT `intra_op_num_threads=1`): 200 warm-up calls, 10k single-row calls (p50, p99, p99.9) and 1024-row batches;
  - logs the CPU model, versions and tree count to MLflow.

  The scorer gets an optional `USE_ONNX=true` path. The parity test is added to P5.T2.
- **Files:** `packages/fraud-core/src/fraud/jobs/benchmark.py`, `services/scoring/src/scoring/models.py`, `tests/unit/test_model_safety.py`
- **Depends on:** P2.T3
- **Source:** DISCOVERY Q6, Q14; RESEARCH §03 Q2
- **Done when:** the benchmark run is in MLflow with all latency metrics and the parity test is green.

### P5.T8 — Results report
- **What:** `fraud report` writes `reports/results.md` plus PNG charts. It covers:
  - Shifts: detection lag per Shift type (`shift_events.sim_start` → first alert) and false alarms, with the null-replay baseline;
  - retraining: Stateless vs Stateful compute (wall and CPU), quality and tree count over cycles;
  - censored labels: the 2×3 oracle / naive / IPW table with ESS and bootstrap CIs, across 3 exploration seeds when available;
  - ONNX vs native latency;
  - the promotion history from `model_promotions`;
  - the Decision mix over time.
- **Files:** `packages/fraud-core/src/fraud/jobs/report.py`, `reports/.gitkeep`
- **Depends on:** P5.T3, P5.T7, P4.T5, P3.T5
- **Source:** DISCOVERY Q17, Q23; RESEARCH §01 Q5 (2×3 design)
- **Done when:** after a demo run, `reports/results.md` contains every section with numbers and the PNGs referenced in it exist.

### P5.T9 — Demo profile and `make demo`
- **What:** `config/profiles/demo.yaml` sets the replay window 2020-01-01 → 07-31, speedup 8640, exploration 0.10, the scenario schedule (covariate, concept, label; none in December), shadow and canary windows, and K = 7. `make demo` runs:
  1. `up`, `build`, `db migrate`, `ingest` if no snapshot exists;
  2. `train` (initial Champion);
  3. `check concept-blindspot`;
  4. start replayer, scorer, labeler and monitoring;
  5. `fraud orchestrate` until the replay ends;
  6. `fraud report`.

  It must be idempotent and resumable.
- **Files:** `config/profiles/demo.yaml`, `Makefile`, `scripts/demo.sh`
- **Depends on:** P5.T5, P5.T8, P4.T7
- **Source:** DISCOVERY Q17; PLAN Decision 3
- **Done when:** `make demo` on a clean machine with the dataset downloaded finishes unattended in ≤ 60 min, `reports/results.md` shows at least one promotion and alerts for all three Shifts, and its wall time is recorded in the README.

### P5.T10 — Model card
- **What:** `docs/model-card.md` covers intended use, data (synthetic Sparkov with measured counts), features with excluded protected attributes (gender, dob) and why, metrics at budget, limits (generator artefacts, the compressed dispute window of 7–45 days vs 1–3 months in reality), Exploration and IPW, and calibration.
- **Files:** `docs/model-card.md`
- **Depends on:** P5.T8
- **Source:** RESEARCH §01 Q1 (PII, protected attributes), Risks
- **Done when:** the file exists and every number in it matches the latest `reports/results.md`.

### P5.T11 — Part A README
- **What:** The README gives: what the platform does, an architecture diagram for Part A, prerequisites (Docker, uv, the dataset), `make demo`, the headline results copied from `reports/results.md`, a feedback-loop explanation, and the simulation compressions (clock speed, label delays).
- **Files:** `README.md`
- **Depends on:** P5.T9
- **Source:** DISCOVERY Q1, Q17, Q23
- **Done when:** a reader can run Part A from the README alone. All numbers come from the latest demo run.

### Phase 5 checkpoint
On a clean checkout with `data/raw` present, run `make demo`. Then check that `reports/results.md` shows detection lags for all three Shifts, the Stateless vs Stateful table, the oracle / naive / IPW table and the ONNX numbers; that MLflow shows `@champion` moved at least once; that `SELECT * FROM model_promotions` shows the audit rows; and that `uv run pytest` is green. **Part A is complete and demoable.**

---

## Phase 6 — GitLab CI pipeline
**Outcome:** a public gitlab.com project runs MR and `main` pipelines on the self-hosted runner: lint, tests, SonarQube Cloud gate, Buildah UBI image builds, Trivy gate with SBOM, model quality gate, contract tests, secret detection and SAST. Gated images are pushed to the GitLab registry.
**Depends on:** Phase 5

### P6.T1 — GitLab project and self-hosted runners (user-performed, guided)
- **What:** Write `docs/ops/gitlab-setup.md` and a wizard script that walk the user through:
  - creating the **public** gitlab.com project and pushing;
  - disabling instance runners;
  - installing `gitlab-runner` (deb repo);
  - creating three project runners in the UI with the `glrt-` flow: `fraud-docker` (unprotected), `gpu` (protected, `gpus = "all"`) and `kind` (protected, `network_mode = "kind"`, read-only kubeconfig mounted);
  - writing `config.toml` as in RESEARCH §06 Q2 (`concurrent = 3`, `allowed_images`, `security_opt` for Buildah, never privileged);
  - enabling job-token push;
  - setting CI/CD visibility.

  The `gpu` and `kind` runners are registered later, in P9.T6 and P7.T8.
- **Files:** `docs/ops/gitlab-setup.md`, `scripts/wizards/gitlab_runner.sh`, `deploy/runner/config.toml.example`
- **Depends on:** P5.T11
- **Source:** DISCOVERY Q27; RESEARCH §06 Q1, Q2; PLAN Decision 9
- **Done when:** a pipeline with one `echo` job tagged `fraud-docker` succeeds on the self-hosted runner, and the project shows instance runners disabled.

### P6.T2 — Lint and test stages
- **What:** `.gitlab-ci.yml` with:
  - `workflow:rules` (MR, `main`, `v*` tags, no duplicate pipelines);
  - stages `lint test quality build scan iac gates release verify`;
  - a `.uv` template (`ghcr.io/astral-sh/uv:0.12.23-python3.13-trixie-slim`, `UV_LINK_MODE=copy`, cache keyed on `uv.lock`);
  - `lint` (ruff check and format, mypy);
  - `test` (pytest with `--cov --cov-branch`, `coverage.xml`, JUnit report; integration tests excluded).
- **Files:** `.gitlab-ci.yml`, `ci/lint-test.yml`
- **Depends on:** P6.T1
- **Source:** DISCOVERY Q21, Q27; RESEARCH §06 Q1
- **Done when:** an MR pipeline runs `lint` and `test` green and the JUnit report shows in the MR.

### P6.T3 — SonarQube Cloud quality gate
- **What:** Import the GitLab group as a SonarQube Cloud org, add masked and protected `SONAR_TOKEN`, write `sonar-project.properties` (sources, tests, `sonar.python.version=3.13`, coverage path, `sonar.qualitygate.wait=true`), and add a `sonarcloud-check` job that needs `test`. Accept the fixed "Sonar way" gate (80% coverage on new code).
- **Files:** `sonar-project.properties`, `ci/quality.yml`
- **Depends on:** P6.T2
- **Source:** DISCOVERY Q27; RESEARCH §06 Q3
- **Done when:** a `main` pipeline shows the Sonar job passing on the quality gate, and the project dashboard shows coverage.

### P6.T4 — Buildah image builds per service
- **What:** A `.build` template (`quay.io/buildah/stable@sha256:7f69b766…`, `STORAGE_DRIVER=vfs`, OCI format) that writes an `oci-archive:$SVC.tar` artifact. There is one job per image (`jobs`, `replayer`, `scoring`, `labeler`, `monitoring`, `mlflow`) with `rules:changes` (service plus `packages/**` plus `uv.lock`) and `when: never` on tags. Containerfiles are finalised for OpenShift-style arbitrary UIDs and keep writes to `/tmp` only.
- **Files:** `ci/build.yml`, `services/*/Containerfile`, `images/*/Containerfile`
- **Depends on:** P6.T2, P2.T5
- **Source:** DISCOVERY Q27, Q43; RESEARCH §06 Q4
- **Done when:** a change to `services/scoring` builds only the scoring image (plus anything depending on `packages/`), and the archive artifact exists.

### P6.T5 — Trivy gate and SBOM
- **What:** A `.scan` template using `aquasec/trivy:0.75.0@sha256:af6acf9a…` with `--input $SVC.tar`. It produces a CycloneDX SBOM artifact, then gates with `--exit-code 1 --severity HIGH,CRITICAL --ignore-unfixed`. A `.trivyignore` lists accepted CVEs, each with a justification and an expiry date. No registry credentials are exposed in scan jobs.
- **Files:** `ci/scan.yml`, `.trivyignore`
- **Depends on:** P6.T4
- **Source:** DISCOVERY Q27; RESEARCH §06 Q4 (CVE-2026-33634)
- **Done when:** scan jobs pass on clean images and SBOM artifacts are downloadable. A test image with a known fixable HIGH CVE fails the job (verified once, then removed).

### P6.T6 — Model quality gate in CI
- **What:** A `gates:model` job that trains on the fixed fixture sample, asserts a PR-AUC floor, and runs `test_model_safety.py` (convergence, NaN, warm-start invariance, ONNX parity). Results go to the job log and a JUnit report.
- **Files:** `ci/gates.yml`, `tests/gates/test_model_gate.py`, `config/gates.yaml` (floors)
- **Depends on:** P6.T2, P5.T2
- **Source:** DISCOVERY Q21, Q33; RESEARCH §08 Q3
- **Done when:** the gate job is green on `main`. Lowering the floor above the achieved value makes it fail.

### P6.T7 — API contract tests and OpenAPI diff
- **What:** An in-process Schemathesis test (`schemathesis.openapi.from_asgi`) on the scoring app with a stubbed ModelHolder. Commit `services/scoring/openapi.json`; a CI step regenerates it and runs `git diff --exit-code`.
- **Files:** `tests/contract/test_scoring_openapi.py`, `services/scoring/openapi.json`, `scripts/export_openapi.py`, `ci/lint-test.yml`
- **Depends on:** P6.T2, P2.T7
- **Source:** DISCOVERY Q34; RESEARCH §06 Q6
- **Done when:** the contract test passes in the `test` job, and changing a response field without updating `openapi.json` fails CI.

### P6.T8 — Push gated images to the GitLab registry
- **What:** `push:<svc>` jobs on `main` only. Each needs its scan job and all gates, logs in with the job token, and pushes `$CI_REGISTRY_IMAGE/<svc>:git-<sha>` from the OCI archive. It records the digest in a `built-images.env` dotenv artifact.
- **Files:** `ci/release.yml`
- **Depends on:** P6.T5, P6.T6, P6.T7, P6.T3
- **Source:** DISCOVERY Q27; RESEARCH §06 Q1, Q5
- **Done when:** after a `main` pipeline the registry shows the new tags, and an anonymous `docker pull registry.gitlab.com/<user>/fraud-ml-platform/scoring:git-<sha>` works.

### P6.T9 — Secret detection and SAST components
- **What:** Include `gitlab.com/components/secret-detection@2.4.0` and `gitlab.com/components/sast@3.5.0` as extra evidence. They are non-blocking on the Free tier, and their JSON reports are kept as artifacts.
- **Files:** `.gitlab-ci.yml`
- **Depends on:** P6.T2
- **Source:** RESEARCH §06 Q1
- **Done when:** both jobs run on MR pipelines and their reports are attached as artifacts.

### P6.T10 — Self-hosted runner hardening check (user-performed, guided)
- **What:** On the Linux GPU box, beyond P6.T1: run `gitlab-runner` as a dedicated non-login user with no access to the owner's home or `~/.ssh`; set CI/CD visibility to "Only project members"; protect `main` and `v*` tags; confirm no runner is privileged or mounts the Docker socket; scope the `kind` runner's kubeconfig to read-only on `staging` and `prod`. Record the settings and the rule "never run a fork MR pipeline in the parent project" in `docs/ops/runner-security.md`.
- **Files:** `docs/ops/runner-security.md`, `deploy/runner/config.toml.example`
- **Depends on:** P6.T1
- **Source:** THREAT-MODEL TM-101, TM-104, TM-105; RESEARCH §06 Q2
- **Done when:** a job running `id` and `ls ~/.ssh` as the runner shows the runner user and no access; a fork MR does not start a pipeline on the project runners; the settings page screenshots are linked from the doc.

### Phase 6 checkpoint
Open an MR that touches `services/scoring`. The pipeline runs lint, test (with the contract test), Sonar, a build of scoring only, Trivy and the model gate, all green. Merge it: the `main` pipeline pushes `scoring:git-<sha>` to the GitLab registry.

---

## Phase 7 — kind platform and GitOps
**Outcome:** a kind cluster (GPU-capable if the spike succeeds) runs Argo CD. Argo CD deploys the shared platform (Postgres, SeaweedFS, MLflow, Strimzi Kafka) and the apps into `staging` (tracks `main`) and `prod` (tracks `v*` tags). A `main` pipeline bumps image tags, waits for the staging sync, and verifies it with an infrastructure-compatibility smoke test, Schemathesis and a k6 p99 gate. A gated tag promotes to prod.
**Depends on:** Phase 6

### P7.T1 — Host prerequisites (user-performed, guided)
- **What:** `docs/ops/host-setup.md` plus `make doctor`, which checks: kind v0.33.0, kubectl, helm, `fs.inotify.max_user_watches ≥ 524288` and instances 512 (sudo, persisted in `/etc/sysctl.d`), NVIDIA Container Toolkit 1.20.1 configured for Docker, the nvkind smoke tests from RESEARCH §05 Q1, and free RAM. Compose must be stopped before kind starts.
- **Files:** `docs/ops/host-setup.md`, `scripts/doctor.sh`, `Makefile`
- **Depends on:** P5.T11
- **Source:** RESEARCH §05 Q1, §08 Q2; DISCOVERY Q38
- **Done when:** `make doctor` passes every check, or reports exactly which manual step is missing.

### P7.T2 — GPU-in-kind spike (timebox ~2 h) and ADR-0006
- **What:** Build nvkind at commit `c5705049` (via the `golang` container). Create a test cluster from `deploy/kind/nvkind-cluster.yaml.tmpl`: a control-plane with port mappings, plus one worker with `nvidia.com/gpu.present=true` and the `/dev/null` extraMount. Install device plugin 0.20.1 and run an `nvidia-smi` pod. If this is not working within ~2 h, fall back to a single-node kind cluster with vLLM as a Docker container on the `kind` network behind a selector-less Service plus EndpointSlice. Write ADR-0006 with the outcome either way.
- **Files:** `deploy/kind/nvkind-cluster.yaml.tmpl`, `deploy/kind/kind-cluster.yaml`, `docs/adr/0006-gpu-runtime-on-kind.md`
- **Depends on:** P7.T1
- **Source:** ADR-0003 amendment, ADR-0004; RESEARCH §05 Q1; PLAN Decision 14
- **Done when:** either `kubectl get nodes -o json` shows `nvidia.com/gpu: 1` allocatable and the `nvidia-smi` pod lists the RTX 3090, or the fallback container answers `nvidia-smi`. In both cases ADR-0006 is committed.

### P7.T3 — Cluster bootstrap
- **What:** `make cluster-up` / `make cluster-down` create the cluster the ADR-0006 way, using the pinned node image `kindest/node:v1.37.0@sha256:a1ed56cf…` and port mappings to 127.0.0.1:
  - 8080 → Argo CD
  - 5000 → MLflow
  - 8081 → Airflow
  - 3000 → Grafana
  - 8001 → staging scoring
  - 8002 → prod scoring

  It also sets up the local registry `localhost:5001` (dev loop), metrics-server, and namespaces `platform`, `kafka`, `airflow`, `llm`, `staging` and `prod`. `make secrets` creates Kubernetes Secrets from `.env`; they are never committed.
- **Files:** `scripts/cluster.sh`, `scripts/secrets.sh`, `deploy/kind/registry.sh`, `Makefile`
- **Depends on:** P7.T2
- **Source:** RESEARCH §08 Q2, §06 Q2; DISCOVERY Q22
- **Done when:** `make cluster-up` from nothing gives a Ready node, `kubectl top nodes` works, and running it twice is idempotent.

### P7.T4 — Argo CD and root application
- **What:** Install Argo CD v3.5.3 with the pinned manifest URL, server-side apply, and the full install with dex and notifications scaled to 0. Every component gets resource requests and limits. Add a repo connection to the public GitLab repo, a root `Application` (app-of-apps) pointing at `deploy/argocd/platform/`, and a refresh-annotation RBAC role for the CI kubeconfig.
- **Files:** `deploy/argocd/install.sh`, `deploy/argocd/argocd-values-patch.yaml`, `deploy/argocd/root.yaml`, `deploy/argocd/ci-rbac.yaml`
- **Depends on:** P7.T3
- **Source:** DISCOVERY Q25; RESEARCH §06 Q5, §08 Q1; PLAN Decision 11
- **Done when:** the Argo CD UI at 127.0.0.1:8080 shows the root app Synced and Healthy.

### P7.T5 — Platform apps: Postgres, SeaweedFS, MLflow
- **What:** Argo CD Applications for:
  - an in-repo `postgres` chart: official `postgres:18` StatefulSet, PVC, an init Job creating `mlflow`, `airflow`, `fraud_staging` and `fraud_prod`, requests and limits;
  - the SeaweedFS chart 4.48.0 (`allInOne`, S3 on 8333, buckets created by a sync-wave Job rather than the Helm hook);
  - an in-repo `mlflow` chart: own image, `--workers 1`, proxied artifacts, `--allowed-hosts` for the Service DNS names, optional basic auth.

  Each component also gets a Job that runs `fraud db migrate` for each env DB.
- **Files:** `deploy/charts/postgres/**`, `deploy/charts/mlflow/**`, `deploy/argocd/platform/{postgres,seaweedfs,mlflow}.yaml`, `deploy/platform-values/*.yaml`
- **Depends on:** P7.T4
- **Source:** DISCOVERY Q36, Q37; ADR-0005; RESEARCH §03 Q4, §04 Q6, §08 Q1 (avoid Bitnami)
- **Done when:** all three apps are Healthy, `curl 127.0.0.1:5000/health` works, and buckets `lake` and `mlflow` exist.

### P7.T6 — Platform apps: Strimzi and Kafka
- **What:** Strimzi operator 1.2.0 from the OCI chart, with CRDs applied as a separate sync wave using `ServerSideApply`. A `Kafka` `fraud-kafka` with a dual-role `KafkaNodePool` (1 replica, heap 512m, 1.25Gi limit, 10Gi PVC) and the Topic Operator only. `KafkaTopic` CRs in the `kafka` namespace for `staging.` and `prod.` transactions (6 partitions, `retention.ms=-1`), labels and sim-clock.
- **Files:** `deploy/argocd/platform/strimzi.yaml`, `deploy/platform-values/kafka/{kafka.yaml,nodepool.yaml,topics.yaml}`
- **Depends on:** P7.T4
- **Source:** DISCOVERY Q37; RESEARCH §02 Q1, §08 Risks (Strimzi 1.x)
- **Done when:** `kubectl get kafka,kafkatopic -n kafka` shows everything Ready, and a test producer and consumer pod round-trips a message on `staging.transactions`.

### P7.T7 — App chart and ApplicationSet (staging / prod)
- **What:** An umbrella chart `deploy/charts/fraud-apps` containing scoring (Deployment, one uvicorn worker, NodePort per env), replayer (Deployment with resume), labeler and monitoring. Configuration:
  - a ConfigMap and Secret rendered from values (topic prefix, DB, MLflow URI, `CANARY_PCT`, `EXPLORATION_RATE`);
  - probes, requests and limits, `runAsNonRoot`, `readOnlyRootFilesystem` (plus an emptyDir for `/tmp`);
  - values files `deploy/images.yaml`, `deploy/envs/{staging,prod}/values.yaml` and `values-gke.yaml` (placeholder).

  An ApplicationSet with a list generator: `staging` → `main`, `prod` → `v*`.
- **Files:** `deploy/charts/fraud-apps/**`, `deploy/images.yaml`, `deploy/envs/staging/values.yaml`, `deploy/envs/prod/values.yaml`, `deploy/argocd/apps-applicationset.yaml`
- **Depends on:** P7.T5, P7.T6, P6.T8
- **Source:** DISCOVERY Q34, Q37; RESEARCH §06 Q5, §08 Q4
- **Done when:** `helm lint` and `helm template` pass for both envs, `fraud-staging` is Synced and Healthy, and `curl 127.0.0.1:8001/readyz` returns 200.

### P7.T8 — CI bump-deploy and staging verification
- **What:** Register the `kind` runner (P6.T1 doc). Then add:
  - a `bump-deploy` job: `resource_group: gitops-bump`, fetch and rebase, `yq` update of `deploy/images.yaml` from `built-images.env`, commit, and push with the job token (no pipeline loop), saving `bump.sha`;
  - a `verify:staging-synced` job on the `kind` runner: annotate refresh, then wait until the sync revision equals the bump SHA and the app is Healthy, with a 15 min timeout;
  - a `verify:infra-compat` job: the model loads in the serving image (readyz), the requested resources fit the node, and the GPU is allocatable once LLM exists;
  - a `verify:contract` job: the Schemathesis CLI against `fraud-control-plane:30001`;
  - a `verify:k6` job (`grafana/k6:2.3.0@sha256:9c2dee7f…`): constant arrival rate, `p(99)` under `P99_BUDGET_MS`, failure rate below 0.1%, artifact `k6-summary.json`.

  All verify jobs are `when: manual` plus `allow_failure: true` when `KIND_AVAILABLE != "true"`, so the pipeline does not hang when the laptop cluster is down.
- **Files:** `ci/release.yml`, `ci/verify.yml`, `tests/load/score.js`, `tests/load/sample_txn.json`, `deploy/envs/staging/values.yaml` (`P99_BUDGET_MS`)
- **Depends on:** P7.T7
- **Source:** DISCOVERY Q27, Q34; RESEARCH §06 Q2, Q5, Q6, §08 Q3 (infra compatibility)
- **Done when:** merging to `main` updates `deploy/images.yaml` automatically, staging rolls to the new SHA, and all verify jobs pass with `k6-summary.json` attached.

### P7.T9 — Prod promotion by tag with the "N green staging runs" rule
- **What:** `make release VERSION=vX.Y.Z` (and an equivalent manual CI job) checks through the GitLab API that the last N = 3 `main` pipelines that bumped staging have all `verify:*` jobs green. After P8 it also checks that the last N staging DAG runs succeeded. It then tags the **bump commit**. Argo CD `fraud-prod` syncs to the new tag. The command refuses with a clear message when the precondition fails.
- **Files:** `scripts/release.py`, `ci/release.yml`, `Makefile`
- **Depends on:** P7.T8
- **Source:** DISCOVERY Q34; RESEARCH §06 Q5, §08 Q3 (CD list)
- **Done when:** a release with a passing history deploys prod (`curl 127.0.0.1:8002/readyz` shows the new image digest), and a forced failing history is refused.

### P7.T10 — Measured resource budget
- **What:** With the whole platform running, record `kubectl top pods -A`, plus the peak during a demo replay on staging, in `docs/ops/resources.md` alongside the RESEARCH §08 estimates. Tune requests and limits in values files where the measurements disagree.
- **Files:** `docs/ops/resources.md`, `deploy/**/values*.yaml`
- **Depends on:** P7.T7
- **Source:** DISCOVERY Q37; RESEARCH §08 Q1
- **Done when:** the doc shows measured vs estimated per component, and steady-state usage with vLLM off is ≤ 13 GiB.

### Phase 7 checkpoint
`make cluster-up`, then Argo CD shows platform, `fraud-staging` and `fraud-prod` Healthy. Merge a trivial change: the pipeline pushes images, bumps, staging syncs, and contract and k6 pass. Run `make release VERSION=v0.1.0` and prod serves the same digests. Run a 30-sim-day replay in staging: predictions and labels flow in `fraud_staging`.

---

## Phase 8 — Airflow and Spark on kind
**Outcome:** Airflow 3.3.2 runs per-env pipelines from two Git DAG bundles. Asset events from the replayer and monitoring trigger KubernetesPodOperator tasks that run the Spark medallion, Stateless and Stateful retraining, batch scoring and promotion, with full lineage tags. This replaces the Part A local orchestrator on kind.
**Depends on:** Phase 7

### P8.T1 — Spark job image
- **What:** Base `ubi9/ubi-minimal`, plus `java-21-openjdk-headless` (microdnf), plus uv CPython 3.13. Install `pyspark[sql]==4.2.0`, `pandas>=2.2,<3`, `pyarrow` and `pandera[pandas,pyspark]==0.33.1`. Bake in the jars `hadoop-aws-3.5.0`, `bundle-2.35.4` and `analyticsaccelerator-s3-1.3.1`, and `spark-defaults.conf` (driver memory via defaults, S3A endpoint and path style, checksum generation and validation off, UTC). `fraud.spark.session()` builds the session from env.
- **Files:** `images/spark-jobs/Containerfile`, `images/spark-jobs/spark-defaults.conf`, `packages/fraud-core/src/fraud/spark.py`, `ci/build.yml`
- **Depends on:** P7.T5
- **Source:** DISCOVERY Q29, Q43; RESEARCH §04 Q4
- **Done when:** running the image in the cluster writes and reads a Parquet file on `s3a://lake/tmp/` against SeaweedFS.

### P8.T2 — Spark medallion and gold build with shared features
- **What:** Spark versions of ingest and build-dataset:
  - **bronze:** CSV → Parquet;
  - **silver:** clean and hash, then validate with `spark_schema()` generated from the P1.T5 `SPEC`; read `df.pandera.errors` immediately and raise `SchemaSkew`;
  - **gold:** `groupBy("card_hash").applyInPandas(replay_card, schema)`, which sorts inside on `(event_time, trans_num)` and calls `update_state`;
  - write-once snapshots plus a manifest.

  Add a parity test showing that Spark gold equals pandas gold on the fixture, and a parity test between the pandas and Spark schemas.
- **Files:** `packages/fraud-core/src/fraud/jobs/spark_ingest.py`, `.../jobs/spark_build_dataset.py`, `.../contracts/transactions.py`, `tests/spark/test_spark_parity.py` (runs in the Spark image in CI)
- **Depends on:** P8.T1, P2.T2
- **Source:** DISCOVERY Q29, Q35; ADR-0002; RESEARCH §04 Q4, Q5
- **Done when:** the parity tests pass inside the Spark image (a CI job uses the built image), and the gold manifest hash is logged.

### P8.T3 — Airflow image and chart deployment
- **What:** Custom image `apache/airflow:slim-3.3.2-python3.13` with `apache-airflow-providers-cncf-kubernetes==10.22.0`, the git provider and httpx. Chart 1.22.0 values:
  - `airflowVersion` and `defaultAirflowTag` set to 3.3.2;
  - `LocalExecutor`, with `postgresql`, `redis`, `statsd` and `pgbouncer` disabled;
  - metadata DB as a secret pointing at the platform Postgres;
  - helm hooks off for `createUserJob` and `migrateDatabaseJob`, with the Argo CD Sync annotation;
  - triggerer on, resources set, NodePort for the api-server;
  - a `monitoring` user with role `Op` from a Secret.

  An Argo CD Application lives in `platform`, with a `heavy` pool of 1 slot.
- **Files:** `images/airflow/Containerfile`, `deploy/platform-values/airflow-values.yaml`, `deploy/argocd/platform/airflow.yaml`
- **Depends on:** P7.T5
- **Source:** DISCOVERY Q37; RESEARCH §04 Q1, §08 Q1
- **Done when:** the Airflow UI at 127.0.0.1:8081 loads, `airflow pools list` shows `heavy` with 1 slot, and the app is Healthy in Argo CD.

### P8.T4 — DAG factory and per-env Git DAG bundles
- **What:** Configure `dagProcessor.dagBundleConfigList` with two `GitDagBundle`s against the public repo, `subdir: dags`: `staging` with `tracking_ref: main`, and `prod` with `tracking_ref` set to the latest `v*` tag (updated by `scripts/release.py`). `dags/factory.py` derives the env from the bundle name, prefixes dag ids (`staging__train`), and reads image tags from `deploy/images.yaml` in the same checkout. The KPO helper sets `deferrable=True`, `IfNotPresent`, `delete_succeeded_pod`, resources, `pool="heavy"` for Spark and train, and passes `IMAGE_DIGEST`, `GIT_SHA` and the env. Add RBAC for KPO pods in the `airflow` namespace and the ruff AIR301/AIR302 rules.
- **Files:** `dags/factory.py`, `dags/kpo.py`, `deploy/platform-values/airflow-values.yaml`, `deploy/charts/airflow-rbac/**`, `pyproject.toml` (ruff)
- **Depends on:** P8.T3
- **Source:** PLAN Decision 10; RESEARCH §04 Q1–Q2, §08 Q3–Q4
- **Done when:** the Airflow UI lists `staging__*` and `prod__*` DAGs that parse without errors, and a commit to `main` changes only the staging DAG version.

### P8.T5 — DAGs: train, batch_score, promote, ingest
- **What:** Asset-scheduled DAGs (`airflow.sdk`):
  - **`train`:** `schedule=(DRIFT | RETRAIN_DUE)`, `max_active_runs=1`. Tasks: `collect_trigger` (alert ids and sim date from `triggering_asset_events`) → pause replay → Spark validate (silver pandera) → Spark build gold → `fraud retrain` → resume replay. MLflow tags include the trigger reason, bundle version and image digests.
  - **`batch_score`:** on `SIM_WEEK_CLOSED`. Tasks: pandera, then `fraud score-batch`.
  - **`promote`:** on `SHADOW_WINDOW_DONE`. Tasks: evaluate → set canary (commits `CANARY_PCT` to `deploy/envs/<env>/values.yaml` through a GitLab API MR, or a ConfigMap patch documented as an exception) → evaluate → alias move.
  - **`ingest`:** manual.
- **Files:** `dags/train.py`, `dags/batch_score.py`, `dags/promote.py`, `dags/ingest.py`, `tests/dags/test_dag_integrity.py`
- **Depends on:** P8.T4, P8.T2, P5.T1, P5.T3, P5.T6
- **Source:** DISCOVERY Q20, Q29, Q33, Q41; RESEARCH §04 Q2–Q3, §08 Q3 (metadata gaps)
- **Done when:** the DAG integrity test passes (imports, no cycles, asset schedules). A manual trigger of `staging__train` runs all tasks green and registers a Challenger with the lineage tags.

### P8.T6 — AirflowAssetTrigger adapter
- **What:** `AirflowAssetTrigger` implements `PipelineTrigger`: it gets a JWT from `POST /auth/token`, resolves asset ids once (`GET /api/v2/assets?uri_pattern=`), and calls `POST /api/v2/assets/events` with `extra` (alert ids, sim date). Delivery goes through the `pipeline_requests` outbox with retries. The adapter is selected by `TRIGGER_BACKEND=airflow` in the Helm values. The replayer emits `retrain_due`, `sim_week_closed` and `shadow_window_done`, and monitoring emits `drift`.
- **Files:** `packages/fraud-core/src/fraud/adapters/airflow_trigger.py`, `deploy/envs/*/values.yaml`, `tests/unit/test_airflow_trigger.py` (respx)
- **Depends on:** P8.T5, P4.T6
- **Source:** DISCOVERY Q41; RESEARCH §04 Q3; PLAN Decision 2
- **Done when:** in staging, enabling the covariate scenario produces a drift alert that triggers a `staging__train` run within 3 sim days, visible in the Airflow UI with the alert id in its conf or extra.

### P8.T7 — Data scientist guide and notebook parity
- **What:** `make notebook` runs Jupyter in the `jobs` image (and optionally the Spark image) against the platform. `docs/data-scientist-guide.md` covers: how to add a feature (edit `update_state`, bump the vocab, run the parity tests), how to run an experiment that logs lineage, how to read MLflow runs and promotion records, and the test commands. Add one example notebook.
- **Files:** `docs/data-scientist-guide.md`, `notebooks/01_explore_snapshot.ipynb`, `Makefile`
- **Depends on:** P8.T5
- **Source:** DISCOVERY Q30, Q33; RESEARCH §08 Q3 (experimental-operational symmetry)
- **Done when:** following the guide from a fresh clone, a reader adds a dummy feature that passes the parity tests and runs the notebook end to end.

### P8.T8 — Optional: new-matured-labels asset trigger
- **What:** The labeler emits a `MATURED_LABELS` asset event every 7 sim days with label counts. `train` optionally adds it to its schedule behind a flag, as an "on availability of new training data" trigger.
- **Files:** `services/labeler/src/labeler/main.py`, `dags/train.py`
- **Depends on:** P8.T6
- **Source:** RESEARCH §08 Q3 (optional trigger)
- **Done when:** with the flag on, a `train` run's `triggering_asset_events` includes `MATURED_LABELS`.

### Phase 8 checkpoint
On staging: run the demo scenario profile. Drift alerts trigger `staging__train` (Spark tasks visible as KPO pods, one at a time in the `heavy` pool). Promotion runs through `staging__promote`, batch scores appear weekly, and MLflow runs carry the trigger reason, bundle version and image digests. A `v*` tag moves the `prod` bundle.

---

## Phase 9 — LLM Case summaries
**Outcome:** vLLM serves Ministral 3 8B (FP8) on the RTX 3090, scaled to 0 by default. A case-summary service writes structured Case summaries for flagged Transactions using prompts from the MLflow prompt registry, with tracing and metrics. A golden-set eval with deterministic hard gates runs on the GPU runner whenever prompts or the model change.
**Depends on:** Phase 7 (P8 not required)

### P9.T1 — vLLM deployment
- **What:** An in-repo `vllm` chart. The Deployment uses `vllm/vllm-openai:v0.30.0` with args `--model=mistralai/Ministral-3-8B-Instruct-2512 --served-model-name=case-summary-llm --tokenizer-mode=mistral --config-format=mistral --load-format=mistral --language-model-only --max-model-len=8192 --gpu-memory-utilization=0.85 --max-num-seqs=32`, `nvidia.com/gpu: 1`, a Memory-backed `/dev/shm` emptyDir, an hf-cache PVC, `/health` probes and **`replicas: 0` default**. Add `values-gke.yaml` (`nodeSelector` `cloud.google.com/gke-accelerator: nvidia-l4`, toleration). If ADR-0006 chose the fallback, `make llm-up` runs the same image as a Docker container on the `kind` network, and the chart renders only the Service and EndpointSlice. `make llm-up` / `make llm-down` scale vLLM and scale staging apps down and up to fit RAM.
- **Files:** `deploy/charts/vllm/**`, `deploy/argocd/platform/vllm.yaml`, `scripts/llm.sh`, `Makefile`
- **Depends on:** P7.T2, P7.T4
- **Source:** ADR-0004, ADR-0006; DISCOVERY Q37; RESEARCH §05 Q1–Q3
- **Done when:** after `make llm-up`, `curl …/v1/models` lists `case-summary-llm`, the vLLM log's KV-cache size line is recorded in `docs/llm-ops.md`, and `make llm-down` returns RAM to the vLLM-off budget.

### P9.T2 — Case-summary prompt in the MLflow prompt registry
- **What:** `prompts/case-summary/v1.md`, a template with the Transaction data in a delimited data block (never in instructions), the platform Decision, the top risk features and the target language. `fraud prompts register` registers new versions and sets the `production` alias. The repo file is the source of truth, and the registry version is recorded back into `prompts/case-summary/VERSION`.
- **Files:** `prompts/case-summary/v1.md`, `prompts/case-summary/VERSION`, `packages/fraud-core/src/fraud/jobs/prompts.py`
- **Depends on:** P7.T5
- **Source:** DISCOVERY Q28; RESEARCH §03 Q3, §05 Risks (prompt injection)
- **Done when:** `mlflow.genai.load_prompt("prompts:/case-summary@production")` returns v1 and the alias is visible in the MLflow UI.

### P9.T3 — Case-summary service
- **What:** FastAPI service `services/case-summary` (UBI image) with:
  - a worker that polls the env DB for new review and block Decisions without a summary;
  - `POST /summaries/{txn_id}`;
  - input assembly: the Transaction, risk features and the Decision;
  - an OpenAI client pointing at `LLM_BASE_URL` with `response_format` json_schema `CaseSummary` (summary, risk_factors, recommended_action ∈ Decision, language ∈ {en, fr});
  - a `case_summaries` table (migration);
  - `mlflow.openai.autolog()` traces tagged with the prompt version and `txn_id`;
  - Prometheus metrics `case_summary_requests_total{prompt_version,outcome}`, `case_summary_schema_failures_total` and `case_summary_latency_seconds`.

  When vLLM is at 0 it backs off and marks requests pending without crashing. Add it to the `fraud-apps` chart behind `caseSummary.enabled`.
- **Files:** `services/case-summary/**`, `migrations/003_case_summaries.sql`, `deploy/charts/fraud-apps/templates/case-summary.yaml`, `tests/unit/test_case_summary.py` (with a stub LLM)
- **Depends on:** P9.T1, P9.T2, P7.T7
- **Source:** DISCOVERY Q28; ADR-0004; RESEARCH §05 Q4–Q5
- **Done when:** with `make llm-up` on staging, flagged Transactions get `case_summaries` rows within 1 min, and traces appear in MLflow with the prompt version tag.

### P9.T4 — Golden set (hand-written)
- **What:** `evals/golden/v1.json` with about 30 hand-written cases. Each has inputs (Transaction, Decision, language), a reference summary, expected facts and `notes` saying what it catches. The cases cover: easy cases, edge cases (zero-history card, travel-distance), about 10 in French, and adversarial cases (merchant name containing instructions, a card number in a field). Cases are written by hand, not generated.
- **Files:** `evals/golden/v1.json`, `evals/README.md`
- **Depends on:** P9.T2
- **Source:** DISCOVERY Q28; RESEARCH §05 Q4 (PromptGuard reuse)
- **Done when:** the file validates against `evals/schema.json`, and `evals/README.md` lists the case categories with their counts.

### P9.T5 — Eval runner and gate
- **What:** `evals/run_eval.py` calls `mlflow.genai.evaluate(data=golden, predict_fn=summarise, scorers=[…])`.
  - **Hard gate (deterministic):** `schema_valid`, `no_hallucinated_numbers` (normalised for en/fr formats), `entities_grounded`, `decision_matches`, `language_matches` and `no_pan`.
  - **Advisory:** a `make_judge` analyst-usefulness score (1–5) using the local model by default, or a hosted judge only if `JUDGE_MODEL` and a key are set.
  - **Gate logic (PromptGuard semantics):** `WARNING_DELTA` and `CRITICAL_DELTA` against the previous production eval run, all cases errored ⇒ critical, a per-case regression list, a rolling-window drift check, and a static HTML report. Exit codes 0, 1 and 2.
- **Files:** `evals/run_eval.py`, `evals/scorers.py`, `evals/gate.py`, `evals/report.py`, `tests/unit/test_eval_scorers.py`
- **Depends on:** P9.T4, P9.T3
- **Source:** DISCOVERY Q28; RESEARCH §05 Q4; PLAN Decision 13
- **Done when:**
  - Scorer unit tests pass, including fr number formats and the adversarial cases.
  - A run against the live model writes an MLflow eval run and `reports/llm-eval.html`.
  - A deliberately broken prompt, one that drops the Decision, exits 2.

### P9.T6 — Prompt and model eval gate in CI (GPU runner)
- **What:** Register the protected `gpu` runner (P6.T1 doc). Add a `gates:llm` job tagged `gpu` that runs on changes to `prompts/**`, `services/case-summary/**` or `deploy/charts/vllm/**`.
  - **Prompt-only change:** register the candidate prompt version without an alias and evaluate it against the running vLLM.
  - **Model change:** scale the in-cluster vLLM to 0, start the candidate container, evaluate, then restore.

  On `main`, a pass moves the `production` alias. The HTML report is attached as an artifact.
- **Files:** `ci/gates.yml`, `scripts/llm_gate.sh`
- **Depends on:** P9.T5, P6.T6
- **Source:** DISCOVERY Q27, Q28; RESEARCH §05 Q4, Risks (one GPU)
- **Done when:** an MR that edits the prompt runs `gates:llm` on the GPU runner, and the result blocks or passes according to the exit code.

### P9.T7 — Optional: model bake-off
- **What:** Run the golden set against Ministral 3 8B FP8, Qwen3.5-9B W4A16 (`enable_thinking: false`) and Gemma 4 12B QAT W4A16. Record pass rates, judge scores, TTFT and tokens/s in `reports/llm-bakeoff.md`. If the winner is larger than 8B, update the ADR-0004 wording through a new ADR amendment.
- **Files:** `reports/llm-bakeoff.md`, `docs/adr/0004-self-hosted-llm-vllm.md` (amendment if needed)
- **Depends on:** P9.T6
- **Source:** RESEARCH §05 Q3, Recommendation 2
- **Done when:** the report exists with all three models' numbers and a stated choice.

### P9.T8 — LLMOps observability and cost model
- **What:** Measure throughput with `vllm bench serve` at the target p95. Document the TTFT and end-to-end p95, tokens/s and KV usage from `/metrics`, and the GPU-hour cost model: 350 W × the electricity price (cited, marked as needing re-verification), hardware amortisation as a stated assumption, and the GKE `g2-standard-8` comparison. Put it in `docs/llm-ops.md`, and add an optional OTLP dual-export flag.
- **Files:** `docs/llm-ops.md`, `services/case-summary/src/case_summary/tracing.py`
- **Depends on:** P9.T3
- **Source:** DISCOVERY Q28; RESEARCH §05 Q5
- **Done when:** `docs/llm-ops.md` contains measured numbers and a cost per 1k summaries.

### Phase 9 checkpoint
Run `make llm-up`. Replay on staging with the concept scenario: flagged Transactions get Case summaries in en and fr, MLflow shows traces and the `production` prompt alias, and an MR that edits the prompt runs `gates:llm` green. Then `make llm-down`.

---

## Phase 10 — GCP Terraform
**Outcome:** a portable GCP target as Terraform code: VPC with PSA and NAT, GKE Standard with system and L4 GPU pools, Cloud SQL Postgres 18, GCS, Artifact Registry, Workload Identity Federation, Secret Manager with ESO, and Helm `values-gke` overlays. It passes fmt, validate, mocked `terraform test`, tflint and checkov in CI without credentials, and is never applied.
**Depends on:** Phase 6 (CI); independent of Phases 7–9

### P10.T1 — Layout, versions and backends
- **What:** The tree is `infra/terraform/{bootstrap,modules/{network,gke,cloudsql,storage,registry,workload-iam},environments/{dev,prod},tests}`. Pin `required_version >= 1.9` and `hashicorp/google ~> 8.5`. Each env has a GCS backend (`prefix = fraud-ml/<env>`). `bootstrap/` creates the state buckets (versioning, UBLA, PAP) with local state. Add `.tflint.hcl` with the google ruleset 0.40.0. The README notes OpenTofu compatibility and that Terraform is BSL (source-available).
- **Files:** `infra/terraform/**/versions.tf`, `infra/terraform/environments/*/backend.tf`, `infra/terraform/bootstrap/main.tf`, `infra/terraform/.tflint.hcl`, `infra/terraform/README.md`
- **Depends on:** P6.T2
- **Source:** DISCOVERY Q26; RESEARCH §07 Q1, Q4
- **Done when:** for both envs, `terraform fmt -check -recursive` and `terraform -chdir=environments/<env> init -backend=false && terraform validate` pass locally (the user installs Terraform 1.16).

### P10.T2 — Network module
- **What:** VPC; subnet with `pods` and `services` secondary ranges; Cloud Router and Cloud NAT (private nodes need it to pull quay.io and docker.io); PSA global address and `google_service_networking_connection`. Raw resources.
- **Files:** `infra/terraform/modules/network/{main,variables,outputs}.tf`
- **Depends on:** P10.T1
- **Source:** RESEARCH §07 Q2–Q3, Risks (NAT)
- **Done when:** `validate` passes and the module outputs are consumed by the dev and prod roots.

### P10.T3 — GKE module
- **What:** Standard cluster (zonal in dev, regional in prod) with: `remove_default_node_pool`, REGULAR channel, Workload Identity Federation (`workload_pool`), VPC-native, `ADVANCED_DATAPATH` with `network_policy { enabled = false }`, private nodes, DNS endpoint on and IP endpoint off, shielded nodes and labels. The system pool is `e2-standard-4` (1–3 nodes, COS, `GKE_METADATA`, secure boot). The GPU pool is `g2-standard-8` with `nvidia-l4`, `gpu_driver_version = "LATEST"`, 0–1 nodes, optional Spot, a 200 GB disk and `node_locations` set to L4 zones. Add `#checkov:skip` lines with justifications (CKV_GCP_20, CKV_GCP_65; CKV_GCP_66 unless binary authorization is added).
- **Files:** `infra/terraform/modules/gke/{main,variables,outputs}.tf`
- **Depends on:** P10.T2
- **Source:** DISCOVERY Q26; RESEARCH §07 Q2
- **Done when:** `validate` and `tflint` are clean, and checkov shows no failures other than the justified skips.

### P10.T4 — Cloud SQL, storage and registry modules
- **What:**
  - **Cloud SQL:** `POSTGRES_18` with `edition = "ENTERPRISE"`; tier `db-custom-1-3840` ZONAL in dev and `db-custom-2-7680` REGIONAL with PITR and deletion protection in prod; private IP via PSA; `ssl_mode = "ENCRYPTED_ONLY"`; `cloudsql.iam_authentication=on` and the logging flags; databases `mlflow`, `airflow`, `fraud`; IAM SA users.
  - **GCS:** buckets for MLflow artifacts and data snapshots (UBLA, versioning, PAP).
  - **Artifact Registry:** a docker repo.

  Add a documented SQL GRANT bootstrap step.
- **Files:** `infra/terraform/modules/{cloudsql,storage,registry}/*.tf`, `docs/gcp.md` (GRANT step)
- **Depends on:** P10.T2
- **Source:** DISCOVERY Q26; RESEARCH §07 Q3
- **Done when:** `validate`, `tflint` and checkov are clean apart from justified dev skips (CKV_GCP_110, 111, 62, 84).

### P10.T5 — Workload IAM, ESO and GKE Helm overlays
- **What:** GSAs for mlflow, airflow and scorer with `roles/iam.workloadIdentityUser` KSA bindings, plus bucket and Cloud SQL roles. Secret Manager secret containers only, with no values in state. A direct `principal://` binding for the ESO controller. Overlays `values-gke.yaml` for platform and app charts add:
  - KSA annotations;
  - Cloud SQL Auth Proxy v2.26.0 as a native sidecar (`--private-ip --auto-iam-authn`);
  - `ExternalSecret` manifests;
  - Artifact Registry image paths;
  - vLLM `nodeSelector` and tolerations;
  - Strimzi 3+3 node pools with RF 3.
- **Files:** `infra/terraform/modules/workload-iam/*.tf`, `deploy/**/values-gke.yaml`, `deploy/gke/external-secrets/*.yaml`
- **Depends on:** P10.T3, P10.T4, P7.T7
- **Source:** DISCOVERY Q26; ADR-0003; RESEARCH §07 Q2, §02 Q1 (GKE Kafka)
- **Done when:** `helm template` with `values-gke.yaml` renders every chart without errors, and `validate` passes.

### P10.T6 — Terraform tests and CI jobs
- **What:** `tests/*.tftest.hcl` with `mock_provider "google"` and assertions:
  - dev Cloud SQL is ZONAL with no public IP and `edition == ENTERPRISE`;
  - prod is REGIONAL;
  - the GPU pool's minimum is 0;
  - private nodes are on;
  - the IP endpoint is disabled.

  CI jobs in the `iac` stage, with `rules:changes` on `infra/terraform/**`:
  - `terraform:checks`: fmt, then init `-backend=false` and validate for each env, then `terraform test`;
  - `terraform:tflint`: `GITHUB_TOKEN`, cached `~/.tflint.d`;
  - `terraform:checkov`: 3.3.21, `-o gitlab_sast`.
- **Files:** `infra/terraform/tests/*.tftest.hcl`, `ci/iac.yml`
- **Depends on:** P10.T5
- **Source:** DISCOVERY Q26, Q27; RESEARCH §07 Q4
- **Done when:** the three CI jobs are green on an MR touching `infra/terraform`. Removing `edition` makes `terraform test` fail.

### P10.T7 — GCP cost and target-architecture doc
- **What:** `docs/gcp.md` contains:
  - a target architecture diagram;
  - a resource-mapping table (kind → GKE);
  - cost tables: the us-central1 numbers dated 2026-10-04 (≈ $765/month always-on dev, ≈ $335 with the GPU pool at zero), a note to re-check a European region, and the managed alternatives (Composer 3 ≈ $526, Vertex endpoint ≈ $593, Managed Kafka ≥ $248);
  - current product names (Workload Identity Federation for GKE, private nodes + DNS endpoint);
  - a "never applied" limitations section.
- **Files:** `docs/gcp.md`
- **Depends on:** P10.T6
- **Source:** DISCOVERY Q26; RESEARCH §07 Q5
- **Done when:** the doc exists with dated prices and links to the RESEARCH §07 sources.

### Phase 10 checkpoint
An MR touching `infra/terraform` runs `terraform:checks`, `terraform:tflint` and `terraform:checkov`, all green. Locally, `helm template … -f values-gke.yaml` renders. `docs/gcp.md` gives the cost and mapping.

---

## Phase 11 — Docs, Level 2 mapping and interview pack
**Outcome:** the repo tells the whole story to an interviewer: the final README with architecture, results and limits, build-vs-buy, the MLOps Level 2 mapping, a "taking it to a real client" section, an optional Grafana board, a 2-minute walkthrough in English and French, and a verified fresh-clone rehearsal.
**Depends on:** Phase 5 (Part A docs). Each section is filled in as far as Phases 6–10 have actually been built.

### P11.T1 — Final README
- **What:** Structure: pitch → architecture (mermaid, Parts A and B) → quick start (Part A `make demo`; Part B `make cluster-up`) → results (from `reports/results.md`, `docs/llm-ops.md` and the ONNX numbers) → key design decisions linking the ADRs → limitations (synthetic data, compressed dispute window, Terraform never applied, the GPU fallback if used, local-only auth) → short French summary ("Résumé") → links to the docs.
- **Files:** `README.md`
- **Depends on:** P5.T11
- **Source:** DISCOVERY Q1, Q7, Q17
- **Done when:** every README number matches a report file, and every link resolves (checked with lychee or `markdown-link-check` in CI).

### P11.T2 — Build vs buy
- **What:** `docs/build-vs-buy.md` with one row per component: self-built choice, managed or commercial alternative, when to buy, and cost where known. Rows cover:
  - Databricks / managed MLflow
  - Composer
  - Vertex endpoints
  - Managed Kafka
  - Feast / Vertex Feature Store
  - Delta / Iceberg
  - KServe / llm-d / vLLM production-stack
  - Argo CD Image Updater
  - promptfoo / DeepEval
  - MinIO / AIStor vs SeaweedFS
  - self-hosted LLM vs hosted API
  - Jenkins vs GitLab CI
  - SonarQube Community Build
- **Files:** `docs/build-vs-buy.md`
- **Depends on:** P5.T11
- **Source:** DISCOVERY Q26, Q31; RESEARCH §03–§08
- **Done when:** every out-of-scope item in this PLAN appears with a reason.

### P11.T3 — MLOps Level 2 mapping
- **What:** `docs/mlops-level2.md` is the RESEARCH §08 Q3 table updated to the built state. Each Level 1 and Level 2 component, each of the 6 stages and each CI/CD test item maps to a file, task or job. Status is ✅ / 🟡 / ⛔ with a reason; the feature store is a deliberate ⛔. It also states which question each metadata store (MLflow vs Airflow) answers.
- **Files:** `docs/mlops-level2.md`
- **Depends on:** P8.T5 (if built), P7.T9 (if built)
- **Source:** DISCOVERY Q32; RESEARCH §08 Q3
- **Done when:** every row cites a concrete artifact, and none of the 9 gap items from RESEARCH §08 remains unaddressed or unexplained.

### P11.T4 — Taking it to a real client
- **What:** `docs/real-client.md` covers: access control (SSO/OIDC on Argo CD, Airflow and MLflow; RBAC), secrets (Secret Manager + ESO), PII (tokenisation, hashing key rotation), HA (Kafka RF 3, regional Cloud SQL, multi-replica scorer with partition ownership), scaling, governance (model cards, promotion audit, approval steps), data residency (EU region), real dispute windows, and the feedback-loop policy approval.
- **Files:** `docs/real-client.md`
- **Depends on:** P5.T11
- **Source:** brief "Interview talking points"; DISCOVERY Q22
- **Done when:** the doc exists and the README links to it.

### P11.T5 — Optional: Grafana dashboard
- **What:** Grafana alone (no kube-prometheus-stack) via an Argo CD app. A Postgres datasource feeds panels for alerts over time, Decision mix, block/review rates against the budget, drift metrics per feature, and promotion events. An optional Prometheus datasource covers scorer and vLLM metrics, if a lightweight Prometheus is affordable in RAM. The dashboard JSON lives in the repo.
- **Files:** `deploy/argocd/platform/grafana.yaml`, `deploy/grafana/dashboards/fraud.json`
- **Depends on:** P7.T5
- **Source:** DISCOVERY Q6, Q23; RESEARCH §08 Q1
- **Done when:** 127.0.0.1:3000 shows the dashboard populated during a staging replay.

### P11.T6 — Interview walkthrough and talking points
- **What:** `docs/interview/walkthrough.md` is a 2-minute script: architecture → Shifts injected → how they were detected (with numbers) → retraining and promotion → one measured result → what changes for production. `docs/interview/talking-points.md` covers: the degenerate feedback loop and IPW numbers, delayed labels (CBPE gap, delay-CDF), stateless vs stateful (measured, plus the additive-boosting caveat), shadow vs canary vs A/B for a bank, build vs buy, the job-description mapping and the Level 2 claim. Include a French version of the walkthrough.
- **Files:** `docs/interview/walkthrough.md`, `docs/interview/walkthrough.fr.md`, `docs/interview/talking-points.md`
- **Depends on:** P5.T8, P11.T3
- **Source:** brief "Interview talking points"; DISCOVERY Q1, Q7
- **Done when:** the walkthrough reads aloud in ≤ 2:15, and every number in it comes from a report file.

### P11.T7 — Fresh-clone rehearsal
- **What:** On a fresh clone, run Part A (`make demo`), then the Part B steps that were built. Time each step, fix any doc gaps, and record the timings and known issues in `docs/rehearsal.md`.
- **Files:** `docs/rehearsal.md`, fixes as needed
- **Depends on:** P11.T1, P11.T6
- **Source:** DISCOVERY Q17
- **Done when:** the rehearsal log shows each built phase's checkpoint passing from a fresh clone, with timings.

### P11.T8 — Go-public checklist
- **What:** Before switching the repo to public: run `gitleaks detect` over the full history and rotate any credential found (history rewrites are not enough); confirm `THREAT-MODEL.md` is untracked and absent from history (it is kept private); enable push protection and secret scanning; check branch and tag protection; check that `.env.example` holds placeholders only.
- **Files:** `docs/ops/go-public.md`
- **Depends on:** P1.T10, P6.T10
- **Source:** THREAT-MODEL TM-102, TM-104
- **Done when:** the history scan is clean (or every finding is rotated and noted), and the checklist is ticked in `docs/ops/go-public.md`.

### Phase 11 checkpoint
A reader with only the repo can run Part A, follow the Part B docs for every built phase, find every number's source report, and deliver the 2-minute walkthrough.

---

## Coverage check (DISCOVERY → tasks)
| DISCOVERY item | Tasks |
|---|---|
| Q1 goal (walkthrough, demo, README) | P5.T9, P5.T11, P11.T1, P11.T6 |
| Q2/Q24 timebox, Part A/B | Decision 1; phase order |
| Q3/Q27 GitLab CI | P6.T1–T9, P7.T8 |
| Q4 Sparkov, ADR-0001 | P1.T6, P1.T7 |
| Q5 acting on predictions, Q12/Q40 policy and Exploration | P3.T1, P3.T2 |
| Q6 ONNX | P5.T7; Grafana optional P11.T5; kind P7; LLM P9 |
| Q7 language | P11.T1, P11.T6 |
| Q9 Account = card | P1.T7, P2.T1, P5.T6 |
| Q10 Simulated clock | P2.T6; Decision 3 |
| Q11 label delays | P3.T3 |
| Q13/Q42 Shift scenarios | P4.T1, P4.T7 |
| Q14 LightGBM | P2.T3, P5.T1 |
| Q15/Q44 feature state, exactly-once | P2.T1, P2.T4, P2.T7 |
| Q16 promotion, Shadow mode, Canary | P5.T3, P5.T4, P8.T5 |
| Q17 `make demo` + results | P5.T8, P5.T9 |
| Q18 censored labels / IPW | P3.T4, P3.T5, P5.T1, P5.T8 |
| Q19/Q39 drift rules + additions | P4.T2, P4.T3, P4.T4, P4.T5 |
| Q20 retraining triggers | P4.T6, P5.T1, P5.T5, P8.T5, P8.T6 |
| Q21/Q33 testing, convergence, pipeline CI/CD | P2.T4, P5.T2, P6.T6, P8.T2, P8.T4 |
| Q22 security | P1.T1, P1.T7, P7.T3, P10.T5, P11.T4 |
| Q23 results location | P4.T3, P5.T8 |
| Q25/Q38 runtime by phase | P1.T3, P7.T1–T4 |
| Q26 GCP Terraform | P10.T1–T7 |
| Q28 LLM | P9.T1–T8 |
| Q29/Q41 Spark, batch cadence | P5.T6, P8.T1, P8.T2, P8.T5 |
| Q30/Q45 reproducibility, data hash, DS guide | P1.T7, P1.T8, P8.T7 |
| Q31/Q43 UBI images | P2.T5, P6.T4 |
| Q32 Level 2 mapping | P11.T3 (+ gap items in P5.T2, P5.T3, P5.T6, P7.T8, P7.T9, P8.T4, P8.T5, P8.T8) |
| Q34/Q37 staging/prod, shared infra | P7.T5–T9, P8.T4 |
| Q35 pandera, no feature store | P1.T5, P5.T1, P5.T6, P8.T2, P11.T2 |
| Q36 SeaweedFS | P1.T3, P7.T5 |
