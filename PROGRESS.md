# Progress: Fraud ML Platform

Plan: PLAN.md · Last updated: 2026-10-07 19:50
Overall: 6/92 tasks · Phase 1 of 11
PRD coverage: 0/72 stories verified

## Current state
- **Working on:** next up, P1.T4 — Fraud database schema and migration runner.
- **Status / approach:** P1.T1–T3, P1.T5, P1.T6 and P1.T10 are done. The Compose stack runs (`make up`). Host-run code (pytest integration tests, `fraud db migrate`) needs `/etc/hosts` to map `postgres s3 mlflow fraud-kafka-kafka-bootstrap` to 127.0.0.1 (owner action, needs sudo; see Blockers). P1.T4: numbered SQL files in `migrations/`, `fraud.db.migrate` runner with a `schema_migrations` table, `fraud db migrate` CLI (Typer), integration test against the Compose Postgres.
- **Also open:** P1.T9 (logging and invariant tests) needs no Docker; its port rule can now check the real `docker-compose.yml`.
- **Next after this:** P1.T7 — Ingest job (bronze → silver snapshots).

## Blockers
- **`/etc/hosts` entry missing** (needs sudo, owner action): `127.0.0.1  postgres s3 mlflow fraud-kafka-kafka-bootstrap`. Without it, code on the host cannot resolve the `.env` hostnames. It blocks P1.T4's integration test, not the stack itself.

## Phases
Legend: [ ] todo · [~] in progress · [x] done · [-] skipped

### Phase 1 — Repo and local platform skeleton (6/10)
- [x] P1.T1 — Repo scaffold and uv workspace (US-1) · 2026-10-05 · ad8b8fa
- [x] P1.T2 — Settings and infrastructure adapters (ports and adapters) (US-1) · 2026-10-07 · 3ecb912
- [x] P1.T3 — Compose infrastructure stack (US-1, TM-001, TM-005) · 2026-10-07 · 44769eb
- [ ] P1.T4 — Fraud database schema and migration runner (US-1)
- [x] P1.T5 — Transaction data contract (pandera) (US-3) · 2026-10-07 · 527e3b3
- [x] P1.T6 — Sample fixture and data download guide (US-2) · 2026-10-07 · fcffd34
- [ ] P1.T7 — Ingest job (bronze → silver snapshots) (US-2, US-4, US-5)
- [ ] P1.T8 — Lineage helper for MLflow runs (US-5)
- [ ] P1.T9 — Structured logging and invariant guard tests (US-4, TM-001, TM-002)
- [x] P1.T10 — Repo security baseline (secret scan, pinning, dependency audit) (TM-102, TM-103) · 2026-10-07 · 5035007

### Phase 2 — Features, Champion and streaming scoring (0/7)
- [ ] P2.T1 — Per-card state function (ADR-0002) (US-7, US-8)
- [ ] P2.T2 — Offline training-set builder (pandas replay) (US-6, US-7)
- [ ] P2.T3 — Train and register the initial Champion (US-6)
- [ ] P2.T4 — Train/serve parity test (US-7)
- [ ] P2.T5 — Service images and Compose app wiring (US-42)
- [ ] P2.T6 — Replayer with Simulated clock (US-9)
- [ ] P2.T7 — Scoring service (US-10, US-11, US-12, US-13, US-14, US-27, US-32)

### Phase 3 — Decisions, Labels and the feedback loop (0/6)
- [ ] P3.T1 — Decision policy and Exploration sample (US-10, US-15, US-16)
- [ ] P3.T2 — Wire Decisions into the scorer (US-10, US-15)
- [ ] P3.T3 — Labeler (delayed Labels) (US-17)
- [ ] P3.T4 — Matured labels and censored training data assembly (US-18)
- [ ] P3.T5 — Feedback-loop metrics library (US-19)
- [ ] P3.T6 — Oracle import boundary test (US-20)

### Phase 4 — Shift injection and monitoring (0/7)
- [ ] P4.T1 — Scenario model and Shift injector (US-21)
- [ ] P4.T2 — Drift statistics library (US-24)
- [ ] P4.T3 — Monitoring service (US-24, US-25, US-27)
- [ ] P4.T4 — Delayed-label estimators (US-26)
- [ ] P4.T5 — Null replay calibration (US-23)
- [ ] P4.T6 — PipelineTrigger port and LocalTrigger (US-28)
- [ ] P4.T7 — Concept blind-spot acceptance check (US-22)

### Phase 5 — Continual learning and `make demo` (end of Part A) (0/11)
- [ ] P5.T1 — Retraining job (Stateless vs Stateful, IPW vs naive) (US-3, US-5, US-29, US-30)
- [ ] P5.T2 — Model safety tests (warm start, encoding, convergence, NaN) (US-31)
- [ ] P5.T3 — Shadow evaluation and promotion gate (US-32, US-33, US-35)
- [ ] P5.T4 — Canary routing in the scorer (US-34)
- [ ] P5.T5 — Local orchestrator (Part A) (US-9, US-29)
- [ ] P5.T6 — Batch scoring per Account (every K sim days) (US-3, US-36)
- [ ] P5.T7 — ONNX export and latency benchmark (US-37)
- [ ] P5.T8 — Results report (US-19, US-30, US-39)
- [ ] P5.T9 — Demo profile and `make demo` (US-16, US-38)
- [ ] P5.T10 — Model card (US-40)
- [ ] P5.T11 — Part A README (US-66)

### Phase 6 — GitLab CI pipeline (0/10)
- [ ] P6.T1 — GitLab project and self-hosted runners (user-performed, guided) (US-45)
- [ ] P6.T2 — Lint and test stages (US-41)
- [ ] P6.T3 — SonarQube Cloud quality gate (US-41)
- [ ] P6.T4 — Buildah image builds per service (US-42)
- [ ] P6.T5 — Trivy gate and SBOM (US-42)
- [ ] P6.T6 — Model quality gate in CI (US-43)
- [ ] P6.T7 — API contract tests and OpenAPI diff (US-12, US-41)
- [ ] P6.T8 — Push gated images to the GitLab registry (US-44)
- [ ] P6.T9 — Secret detection and SAST components (US-46)
- [ ] P6.T10 — Self-hosted runner hardening check (user-performed, guided) (TM-101, TM-104, TM-105)

### Phase 7 — kind platform and GitOps (0/10)
- [ ] P7.T1 — Host prerequisites (user-performed, guided) (US-47)
- [ ] P7.T2 — GPU-in-kind spike (timebox ~2 h) and ADR-0006 (US-60)
- [ ] P7.T3 — Cluster bootstrap (US-47)
- [ ] P7.T4 — Argo CD and root application (US-47)
- [ ] P7.T5 — Platform apps: Postgres, SeaweedFS, MLflow (US-48)
- [ ] P7.T6 — Platform apps: Strimzi and Kafka (US-48)
- [ ] P7.T7 — App chart and ApplicationSet (staging / prod) (US-14, US-48, US-49)
- [ ] P7.T8 — CI bump-deploy and staging verification (US-49, US-50)
- [ ] P7.T9 — Prod promotion by tag with the "N green staging runs" rule (US-51)
- [ ] P7.T10 — Measured resource budget (US-48)

### Phase 8 — Airflow and Spark on kind (0/8)
- [ ] P8.T1 — Spark job image (US-53)
- [ ] P8.T2 — Spark medallion and gold build with shared features (US-3, US-53)
- [ ] P8.T3 — Airflow image and chart deployment (US-52)
- [ ] P8.T4 — DAG factory and per-env Git DAG bundles (US-52)
- [ ] P8.T5 — DAGs: train, batch_score, promote, ingest (US-5, US-29, US-36, US-52)
- [ ] P8.T6 — AirflowAssetTrigger adapter (US-28, US-52)
- [ ] P8.T7 — Data scientist guide and notebook parity (US-8, US-54)
- [ ] P8.T8 — Optional: new-matured-labels asset trigger (US-55)

### Phase 9 — LLM Case summaries (0/8)
- [ ] P9.T1 — vLLM deployment (US-60)
- [ ] P9.T2 — Case-summary prompt in the MLflow prompt registry (US-56, US-58)
- [ ] P9.T3 — Case-summary service (US-56, US-57)
- [ ] P9.T4 — Golden set (hand-written) (US-58, US-59)
- [ ] P9.T5 — Eval runner and gate (US-4, US-59)
- [ ] P9.T6 — Prompt and model eval gate in CI (GPU runner) (US-58, US-59)
- [ ] P9.T7 — Optional: model bake-off (US-62)
- [ ] P9.T8 — LLMOps observability and cost model (US-61)

### Phase 10 — GCP Terraform (0/7)
- [ ] P10.T1 — Layout, versions and backends (US-63)
- [ ] P10.T2 — Network module (US-63)
- [ ] P10.T3 — GKE module (US-63)
- [ ] P10.T4 — Cloud SQL, storage and registry modules (US-63)
- [ ] P10.T5 — Workload IAM, ESO and GKE Helm overlays (US-64)
- [ ] P10.T6 — Terraform tests and CI jobs (US-63)
- [ ] P10.T7 — GCP cost and target-architecture doc (US-65)

### Phase 11 — Docs, Level 2 mapping and interview pack (0/8)
- [ ] P11.T1 — Final README (US-66)
- [ ] P11.T2 — Build vs buy (US-67)
- [ ] P11.T3 — MLOps Level 2 mapping (US-68)
- [ ] P11.T4 — Taking it to a real client (US-69)
- [ ] P11.T5 — Optional: Grafana dashboard (US-72)
- [ ] P11.T6 — Interview walkthrough and talking points (US-70)
- [ ] P11.T7 — Fresh-clone rehearsal (US-71)
- [ ] P11.T8 — Go-public checklist (TM-102, TM-104)

## Deviations from plan
- 2026-10-07 · P1.T3 · SeaweedFS runs as `weed mini` with `-bucket=lake,mlflow` and its S3 identity from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, so `deploy/compose/seaweedfs-s3.json` (listed in PLAN) is not needed; a one-shot `s3-init` (aws-cli) enables versioning. MLflow needs `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false` and a 768m limit (512m in RESEARCH §08): 3.16's job runner adds ~1.6 GiB. Postgres pinned to 18.6. `.env.example` gains `POSTGRES_USER`/`POSTGRES_PASSWORD`. On macOS the Done-when `curl localhost:5000/health` must use `127.0.0.1` (AirPlay owns `[::1]:5000`).
- 2026-10-07 · P1.T6 · The fixture has 12 fraud episodes (6 per year) and 40 background cards, 2,828 rows / 736 KiB, so `check-added-large-files` allows 1 MiB. Offsets use `datetime.timedelta` because pandas 2.3 + NumPy 2.5 warns on every `pd.Timedelta`.
- 2026-10-07 · P1.T10 · The dependency audit uses uv's built-in `uv audit` (experimental, OSV database) instead of pip-audit or Trivy. The CI secret scan runs the official gitleaks image pinned by digest instead of the gitleaks GitHub Action (one less third-party action). The gitleaks pre-commit hook is skipped in the CI lint job, because it only scans staged changes; the `secrets` job scans the full history.
- 2026-10-05 · P1.T1 · ruff and ruff-format in pre-commit are limited to `.py`/`.pyi` files: ruff 0.16 also reformats Python code blocks inside Markdown and rewrote about 800 lines of `research/*.md`. `trailing-whitespace` keeps Markdown line breaks.
- 2026-10-05 · P1.T1 · Added `[tool.ruff.lint.isort] known-first-party = ["fraud"]`, so imports in `tests/` sort correctly.
- 2026-10-05 · CI (Phase 6, early) · Added a GitHub Actions workflow (`.github/workflows/ci.yml`: pre-commit, unit tests with coverage, `fraud-core` wheel build) ahead of Phase 6. The repo is on GitHub; DISCOVERY Q140 allows GitHub Actions with a GitLab note in the README. Phase 6 is still planned for GitLab; decide later whether its stages move to GitHub Actions. `astral-sh/setup-uv` is pinned by commit SHA, because it publishes no major-version tag.
- 2026-10-07 · P1.T2 · Added a 16th setting, `REGION` (default `us-east-1`), used by `S3ObjectStore.from_settings`. Added `ObjectNotFound` and a `ModelVersion` dataclass to `fraud.ports`.
- 2026-10-07 · P1.T5 · The column spec uses neutral check dataclasses (`InRange`, `IsIn`, `Matches`) instead of pandera checks, so P8.T2 can translate the same spec to PySpark. `Matches` is a full match (`str_matches` plus `\Z`), because pandera 0.33 has no `str_fullmatch`. Added `pandas-stubs>=2.2,<3` as a dev dependency.
- 2026-10-07 · Order · P1.T5 was done before P1.T3 and P1.T4 because Docker is not installed yet. P1.T5 only depends on P1.T1.
- 2026-10-07 · Commits · The P1.T1 commit (ad8b8fa) also holds most of P1.T2, and its message overstates what it contains (no drift detection or promotion exists yet). The PR description corrects this.

## Session log
- 2026-10-07 · P1.T3 · Compose stack healthy in ~40 s; all ports on 127.0.0.1; buckets `lake`/`mlflow` with versioning; `local.transactions` 6 partitions; DBs mlflow/airflow/fraud_local; MLflow artifact round trip through the proxy; ~0.9 GiB total. **SeaweedFS honours `IfNoneMatch`** (write-once verified on the real store) · 44769eb
- 2026-10-07 · P1.T10 · gitleaks pre-commit hook and CI history scan (both proven to block a fake AWS key; real history clean, 10 commits), all Actions pinned by SHA, Dependabot, `uv audit` job (no known vulnerabilities in 66 packages) · 5035007
- 2026-10-07 · P1.T6 · `docs/data.md`, `scripts/make_fixture.py` (deterministic), `tests/fixtures/sparkov_sample.csv` (2,828 rows, 52 cards, 147 fraud rows), 6 fixture tests (43 total passing) · fcffd34
- 2026-10-07 · Docker unblocked: OrbStack opened for the first time; docker 29.4 client and server.
- 2026-10-07 · THREAT-MODEL.md created (TM-001–007, TM-101–105). Added P1.T10, P6.T10, P11.T8 to PLAN.md and here; P1.T9 gains the 127.0.0.1 port rule.
- 2026-10-07 · PROGRESS.md created from PLAN.md and the PRD story map.
- 2026-10-07 · P1.T5 · Transaction data contract and 17 tests (37 total passing) · 527e3b3
- 2026-10-07 · Dataset · Sparkov downloaded to `data/raw/`; row counts verified.
- 2026-10-07 · P1.T2 · psycopg connection factory; `REGION` in `.env.example`; P1.T2 complete · 3ecb912
- 2026-10-05 · CI · GitHub Actions lint, test and build · 862b82f, 627b594
- 2026-10-05 · P1.T1 + most of P1.T2 · uv workspace, settings, ports, S3 adapter, 20 tests · ad8b8fa
