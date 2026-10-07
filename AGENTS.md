# AGENTS.md

Fraud ML Platform: a local MLOps platform for real-time card-fraud detection (Kafka replay, LightGBM scoring, delayed labels, drift monitoring, shadow/canary promotion), built as an ML/MLOps interview portfolio.

## Project docs
Read these before large changes; don't duplicate them here.
- `PRD.md`: what we are building and why (user stories US-n)
- `ARCHITECTURE.md`: structure, codemap, invariants
- `THREAT-MODEL.md`: threats (TM-nnn) and mitigations; read before touching input handling, data storage, CI, ports or external services. **Kept out of git**: it exists only on the owner's machine. The rules below are the copy that travels with the repo.
- `PLAN.md`: phases and tasks (IDs like P2.T4)
- `PROGRESS.md`: current state of the build
- `CONTEXT.md`: glossary; use these terms in code, comments and commits (Transaction, Decision, Champion, Matured label, …)
- `docs/adr/`: decision records; don't reverse one without asking
- `DISCOVERY.md`, `RESEARCH.md`: original decisions and research (versions checked 2026-10-04)

## Implementation progress
This project is being built from PLAN.md. At the start of every session, and after any context reset, read PROGRESS.md and continue from its "Current state" using the `progress` skill. Update PROGRESS.md after every task.

The owner codes most of it by hand: guide step by step unless asked to write the code.

## Commands
- Install: `uv sync` (CI uses `uv sync --locked`)
- Test (all / one file / one test): `uv run pytest` / `uv run pytest tests/unit/test_contracts.py` / `uv run pytest tests/unit/test_contracts.py::test_valid_silver_passes`
- Unit tests only: `uv run pytest -m "not integration"`
- Coverage: `uv run pytest --cov=fraud --cov-report=term-missing`
- Lint + format + typecheck: `uv run pre-commit run --all-files` (or `make lint`)
- Typecheck only: `uv run mypy`
- Build the package: `uv build --package fraud-core`
- Dependency audit: `uv audit --locked --preview-features audit-command`
- Secret scan of the full history: `docker run --rm -v "$PWD:/repo" ghcr.io/gitleaks/gitleaks:v8.30.1 git /repo --redact`
- Regenerate the test fixture: `uv run python scripts/make_fixture.py`
- Add a dependency: `uv add --package fraud-core <pkg>`; dev tool: `uv add --dev <pkg>`
- Start / stop the stack: `make up` / `make down` (also `make ps`, `make logs`); `make up` returns once every service is healthy
- Run a command in a service: `docker compose --env-file .env -f deploy/compose/docker-compose.yml exec <service> …`
- Migrate the DB: `uv run fraud db migrate` (planned, P1.T4)
- Ingest the dataset: `make ingest` (planned, P1.T7)
- End-to-end demo: `make demo` (planned, P5.T9)

## Conventions
- Shared code lives in `packages/fraud-core/src/fraud/` (import name `fraud`). Services in `services/<name>/` are thin wrappers around it.
- Infrastructure goes behind a Protocol in `fraud.ports`; implementations go in `fraud.adapters`. Business code depends on the Protocol only.
- Config comes only from `fraud.config.get_settings()` (pydantic-settings, env vars). Never read `os.environ` elsewhere.
- Data rules live in `fraud.contracts` as neutral `ColumnSpec`s (`InRange`, `IsIn`, `Matches`), translated to pandera now and PySpark later (P8.T2). Don't put pandera objects in the spec.
- Dataclasses for internal records; pydantic for data from outside (env, HTTP, JSON); Protocols for behaviour.
- Translate library errors into domain exceptions at the adapter (`ObjectAlreadyExists`, `ObjectNotFound`, `SchemaSkew`); re-raise anything unexpected.
- mypy runs in strict mode: annotate every function, including tests.

## Rules
- `update_state` is the only feature implementation, used by training and serving. Reason: no train/serve skew (ADR-0002).
- Feature, job and service logic never reads the wall clock (`datetime.now`, `time.time`); "now" is the Transaction's event time or `--as-of`. Reason: determinism under the Simulated clock.
- Predictions, card state and consumer offsets commit in one Postgres transaction. Reason: exactly-once effects.
- Kafka producers to `{env}.transactions` use key `card_hash` and `partitioner=murmur2_random`; never set the record timestamp to event time. Reason: per-card ordering; retention would delete history.
- `/score` never writes state, predictions or offsets. Reason: load and contract tests must not corrupt live metrics.
- Oracle truth (`oracle_labels`, source `is_fraud`) is read only by `fraud.evaluation`, `fraud.jobs.report` and the labeler. Reason: the simulation must not cheat.
- Score thresholds are frozen per model version. Categorical codes come from the versioned vocabulary, never pandas `category` codes. Reason: action-rate monitoring and warm starts.
- Data snapshots are write-once, and every training run logs the snapshot SHA-256, git SHA, image digest and trigger reason. Reason: reproducibility.
- Every container and pod sets memory limits; images never use `:latest`. Reason: the 17 GiB host budget and GitOps reproducibility.
- Never branch on the runtime (Compose, kind, GKE). Reason: the same images run everywhere.

## Security
Copied from THREAT-MODEL.md. For security-sensitive work, also use the `security-and-hardening` skill.

### Always
- Validate external data at the boundary with the pandera contracts in `fraud.contracts` (`validate(df, RAW_COLUMNS | SILVER_COLUMNS, layer)`), and FastAPI request bodies with pydantic models.
- Hash card numbers at ingest with `HMAC-SHA256(cc_num, CARD_HASH_KEY)`. Only `card_hash` crosses into silver, Kafka, Postgres, logs, MLflow or prompts.
- Read every secret and endpoint from `fraud.config.Settings` (environment variables). Store secrets as `SecretStr` and call `get_secret_value()` only where the value is used.
- Parameterize every SQL query (psycopg placeholders, `COPY` for bulk), and pass subprocess arguments as lists, never through a shell string.
- Publish container and kind ports on `127.0.0.1` only.
- Pin dependencies through `uv.lock`, GitHub Actions by commit SHA, and third-party CI images by digest.
- Use write-once (`if_absent=True`) writes for snapshot and manifest objects.
- Put Transaction text in prompts only inside the delimited data block, and request structured output from vLLM.

### Ask first
- Adding a new external service, API key or token, or any network call outside the Compose/kind network.
- Changing runner configuration, CI/CD visibility, protected branches or tags, or the kubeconfig scope.
- Adding a new category of personal data to features, logs or prompts (names, addresses, gender and date of birth are excluded from features).
- Exposing any port beyond `127.0.0.1`, adding auth, or changing MLflow or Airflow access.
- Loading a model or artifact format that uses pickle.

### Never
- Commit `.env`, kubeconfigs, runner tokens, the Kaggle or HF token, `data/`, or real values in `.env.example`.
- Log, print or store a raw `cc_num`, or a whole raw DataFrame row.
- Run CI jobs privileged, mount the Docker socket, or run a fork MR's pipeline in the parent project.
- Use `eval`, `exec`, `pickle.loads` or `yaml.load` (unsafe loader) on data from files, Kafka, the database or the LLM.
- Treat LLM output as code, SQL or a Decision. It is advisory text for an analyst.
- Move the `champion` alias anywhere except the promotion job.

## Testing
- `tests/unit/` runs by default and in CI; `tests/integration/` is marked `@pytest.mark.integration` and needs `make up`.
- Every new module gets unit tests. Name tests after the behaviour they prove (`test_put_if_absent_twice_raises`).
- Fakes: use moto (`mock_aws`) for S3; isolate settings tests from the real `.env` with `monkeypatch.chdir(tmp_path)` and `get_settings.cache_clear()`.
- Tests that need the full dataset skip when `data/raw/fraudTrain.csv` is absent (CI has no data); use `tests/fixtures/sparkov_sample.csv` (P1.T6) instead where possible.
- Done means: the task's PLAN.md "Done when" passes, plus `uv run pytest` and `uv run pre-commit run --all-files` are green.

## Git workflow
- Work on a branch, never directly on `main`; open a PR into `main`. GitHub Actions (`.github/workflows/ci.yml`) runs lint, unit tests and the package build on every push.
- Commit after every task, with the task ID first: `P1.T6: Sparkov sample fixture and data guide`.
- Commit the code and its doc updates (ARCHITECTURE.md, AGENTS.md, PROGRESS.md) together.
- If a hook modifies files, the commit is aborted: re-stage and commit again.

## Gotchas
- pandas is pinned `<3` workspace-wide (PySpark and pandera support), and `pandas-stubs` is pinned `<3` to match.
- Import pandera as `import pandera.pandas as pa`. It has no `str_fullmatch`; `str_matches` anchors only at the start, so `Matches` appends `\Z`.
- pandas 2.3 with NumPy 2.5 emits a DeprecationWarning on every `pd.Timedelta(...)`; use `datetime.timedelta` for offsets.
- `tests/fixtures/sparkov_sample.csv` is generated: change `scripts/make_fixture.py` and rerun it, never edit the CSV by hand. The large-files hook allows up to 1 MiB for it.
- `Settings()` needs `# type: ignore[call-arg]`: mypy can't see that pydantic fills fields from the environment.
- ruff and ruff-format run only on `.py`/`.pyi` in pre-commit: ruff 0.16 also rewrites Python blocks inside Markdown (it once rewrote `research/*.md`).
- New workspace members must be listed explicitly in the root `[tool.uv.workspace] members`; a `services/*` glob fails on folders without a `pyproject.toml`.
- Every GitHub Action is pinned by commit SHA with a `# vX.Y.Z` comment (`astral-sh/setup-uv` has no major tags anyway); let Dependabot bump them.
- `docker` comes from OrbStack (`~/.orbstack/bin`, added to PATH by `~/.zprofile`); a shell opened before OrbStack's first launch won't find it.
- Postgres 18 images mount their volume at `/var/lib/postgresql`, not `/var/lib/postgresql/data`.
- Hostnames in `.env` (`postgres`, `s3`, `mlflow`, `fraud-kafka-kafka-bootstrap`) resolve on the host through an `/etc/hosts` line pointing them at `127.0.0.1`.
- On macOS, AirPlay Receiver also listens on port 5000, so `localhost:5000` (IPv6 first) hits AirPlay and returns 403. Use `127.0.0.1:5000` or `mlflow:5000`, or turn off AirPlay Receiver in System Settings.
- MLflow runs with `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`: job execution starts ~8 extra processes (~1.6 GiB) and was OOM-killed at 768m. Re-enable it only with a larger limit (Phase 9).
- `.env` must exist before `make up` (copy `.env.example`, replace every `change-me`). Postgres reads `POSTGRES_USER`/`POSTGRES_PASSWORD`; keep `DATABASE_URL` in sync. The init SQL only runs on an empty volume: after changing it, `docker compose … down -v`.
- `docker compose up --wait` fails at random when a one-shot job that nothing depends on exits while other services are still starting; `make up` waits only for the long-running services and runs `kafka-init` explicitly. Give future one-shot jobs a dependent (`service_completed_successfully`) or run them the same way.
- Use `docker exec -i` (or `compose exec -T`) to pipe a script into a container; `compose run` with a heredoc hangs waiting for stdin.
- `CLAUDE.md` is in `.gitignore`; this file is the project's agent instructions.
