## 04. Orchestration and data engineering

Researched 2026-10-04. Versions are given with release dates. Where a passage is in quotation marks, it was taken verbatim from the linked primary source: docs, source code, a chart `values.yaml`, or a README. Claims marked **(verified locally)** were checked on this machine with `uv run --python 3.13`. Claims marked **(estimate)** are not from a source and should be measured.

### Questions

1. What is the current Airflow 3.x version and official Helm chart? Which executor fits a laptop kind cluster? What is a minimal `values.yaml`, how should DAGs be delivered under GitOps, and how much memory does it take? Which Airflow 3 breaking changes affect DAG code?
2. How does KubernetesPodOperator work in Airflow 3 (provider version, versioned images, config in, outputs/XCom out)? How should the accelerated simulated clock relate to Airflow schedules?
3. How should a drift alert trigger retraining: the REST API, Assets/asset events, or a sensor polling Postgres?
4. Which PySpark version to use? It must cover Python 3.13, Java, local mode in a container on kind, Parquet on S3 via s3a (jar versions), and `applyInPandas` semantics (ordering, Arrow, memory). Is Delta Lake worth it?
5. Which pandera version to use? How do `pandera.pandas` and `pandera.pyspark` differ, and can one schema serve both?
6. What is MinIO's status in 2026 (licence, images, chart/operator), and what are the alternatives?

### Findings

#### Q1. Airflow 3 on Kubernetes: chart, executor, values, DAG delivery, footprint, breaking changes

**Versions (as of 2026-10-04):**

| Component | Version | Date | Source |
|---|---|---|---|
| `apache-airflow` | 3.3.2 (3.3.0 on 2026-07-06, 3.3.1 on 2026-08-12) | 2026-09-17 | https://pypi.org/project/apache-airflow/ , https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html |
| `apache-airflow-task-sdk` | 1.3.2 | 2026-09-17 | https://pypi.org/project/apache-airflow-task-sdk/ |
| Official Helm chart `apache-airflow/airflow` | 1.22.0 (appVersion 3.2.2) | 2026-06-01 | https://airflow.apache.org/index.yaml , https://airflow.apache.org/docs/helm-chart/stable/index.html |
| `apache-airflow-providers-cncf-kubernetes` | 10.22.0 (10.23.0rc2 on 2026-10-02) | 2026-09-14 | https://pypi.org/project/apache-airflow-providers-cncf-kubernetes/ |
| Airflow images | `apache/airflow:3.3.2-python3.13` and `slim-3.3.2-python3.13` exist; default is a different Python | — | https://hub.docker.com/r/apache/airflow/tags |

`apache-airflow` 3.3.2 has `requires_python: "!=3.15,>=3.10"`, so Python 3.13 is supported. The chart docs say "Supported Airflow version: `2.11+`, `3.0+`". The chart defaults to 3.2.2 (`defaultAirflowTag: "3.2.2"` in the 1.22.0 `values.yaml`). To get 3.3.2, set `airflowVersion` and `defaultAirflowTag` to 3.3.2 (see Risks).

**Executor.** The chart's `values.yaml` (1.22.0) says:
> "# One or multiple of: LocalExecutor, CeleryExecutor, KubernetesExecutor
> # For Airflow <3.0, LocalKubernetesExecutor and CeleryKubernetesExecutor are supported."

The default is `executor: "CeleryExecutor"`, which brings Redis and Celery workers. The hybrid `LocalKubernetesExecutor` and `CeleryKubernetesExecutor` are gone in 3.x. They are replaced by multiple executors (`executor: "LocalExecutor,KubernetesExecutor"`, then per task `executor=`) ([executor docs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html#using-multiple-executors-concurrently)).

| Executor | With KubernetesPodOperator (KPO) | Laptop fit |
|---|---|---|
| **LocalExecutor** | A task runs as a subprocess in the scheduler pod. KPO starts one pod per task (your versioned image). | **Best.** No Redis, no workers, no flower. One pod per task. |
| KubernetesExecutor | Each task gets an Airflow worker pod, and with KPO that pod starts a second pod: **2 pods per task**. | It works and is "k8s-native", but pod churn and startup latency double. Better when tasks are plain `@task` Python and not KPO. |
| CeleryExecutor (chart default) | Workers, Redis and flower are always on. | **Worst** for RAM. Avoid. |

The chart gives the scheduler service account pod-launch rights by default:
> "# If this is true and using LocalExecutor/KubernetesExecutor/CeleryKubernetesExecutor, the scheduler's
> # Service Account will have access to communicate with the api-server and launch pods/jobs."
> `allowPodLaunching: true`

**Argo CD-specific settings.** The [chart docs](https://airflow.apache.org/docs/helm-chart/stable/index.html) say you "**must** set the four following values, or your application will not start as the migrations will not be run": `createUserJob.useHelmHooks: false`, `createUserJob.applyCustomEnv: false`, `migrateDatabaseJob.useHelmHooks: false`, `migrateDatabaseJob.applyCustomEnv: false`. They also suggest `migrateDatabaseJob.jobAnnotations: "argocd.argoproj.io/hook": Sync`. The `values.yaml` comment is "# Disable this if you are e.g. using ArgoCD".

**Bundled Postgres.** It uses Bitnami legacy images:
> "# Uses bitnamilegacy images to avoid Bitnami licensing restrictions
> # Not recommended for production - use external database instead"
> `repository: bitnamilegacy/postgresql` / `tag: "16.1.0-debian-11-r15"`

Prefer the platform's own Postgres, with a separate `airflow` database.

**Auth manager.** The chart sets `auth_manager: "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"` and creates `admin/admin` through `createUserJob`. FAB issues JWTs at `POST /auth/token` ([FAB token docs](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/token.html)).

**Minimal `values.yaml` for kind + Argo CD.** This is assembled from the 1.22.0 keys above and has not been deployed:

```yaml
# airflow-values.yaml  (chart apache-airflow/airflow 1.22.0)
airflowVersion: "3.3.2"
defaultAirflowRepository: registry.local/fraud/airflow   # custom image: DAGs baked in (see below)
defaultAirflowTag: "git-<sha>"                           # set by CI, same commit as task image tags
images:
  airflow:
    pullPolicy: IfNotPresent
executor: "LocalExecutor"
allowPodLaunching: true

# Argo CD: helm hooks off (chart docs: "must set")
createUserJob:     { useHelmHooks: false, applyCustomEnv: false }
migrateDatabaseJob:
  useHelmHooks: false
  applyCustomEnv: false
  jobAnnotations: { "argocd.argoproj.io/hook": Sync }

# use the platform Postgres instead of the bitnamilegacy subchart
postgresql: { enabled: false }
data:
  metadataSecretName: airflow-metadata-db   # key "connection": postgresql://airflow:...@postgres.data:5432/airflow
pgbouncer: { enabled: false }
redis: { enabled: false }
statsd: { enabled: false }          # or keep if Prometheus scrapes it

apiSecretKeySecretName: airflow-api-secret
jwtSecretName: airflow-jwt-secret

scheduler:
  replicas: 1
  resources: { requests: { cpu: 500m, memory: 1Gi }, limits: { memory: 2Gi } }   # LocalExecutor runs tasks here
apiServer:
  resources: { requests: { cpu: 250m, memory: 768Mi }, limits: { memory: 1536Mi } }
dagProcessor:
  resources: { requests: { cpu: 200m, memory: 512Mi }, limits: { memory: 1Gi } }
triggerer:
  resources: { requests: { cpu: 100m, memory: 384Mi }, limits: { memory: 768Mi } }   # needed for deferrable KPO / AssetWatcher
logs:
  persistence: { enabled: false }   # kind: ephemeral logs are fine; or remote logs to the S3 store
config:
  core: { load_examples: "False", parallelism: "8" }
  scheduler: { max_active_runs_per_dag: "1" }
```

**DAG delivery under GitOps.** Airflow 3 adds DAG bundles. [DAG bundles docs (3.3.2)](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html):
> "Since Dag bundles support versioning, they also allow Airflow to run a task using a specific version of the Dag bundle, allowing for a Dag run to use the same code for the whole run, even if the Dag is updated mid-way through the run."

`GitDagBundle` supports versioning. `LocalDagBundle`, `S3DagBundle` and `GCSDagBundle` do not. The chart configures bundles through `dagProcessor.dagBundleConfigList` (default `LocalDagBundle`, commented `GitDagBundle` example with `git_conn_id`, `subdir`, `tracking_ref`, `refresh_interval`). The older `dags.gitSync` sidecar (`registry.k8s.io/git-sync/git-sync:v4.4.2`) still exists.

| Option | GitOps fit | Notes |
|---|---|---|
| **Baked into a custom Airflow image** (LocalDagBundle) | Best fit for Q33. The Airflow image tag and every task image tag live in one Helm values commit, so Argo CD rolls them atomically. | Needs no git credentials in kind and has no sidecar. Any DAG change needs an image rebuild, which CI does anyway. No per-run bundle versioning. |
| GitDagBundle | Good. DAG runs are pinned to a git commit. | Needs a git connection (token) reachable from kind. Image tags must still be pinned in the repo, e.g. a `dags/images.json` that CI updates in the same commit. |
| git-sync sidecar | Legacy pattern. | Extra container in every pod. Superseded by bundles. |

**Memory footprint (estimate).** Neither the chart nor the docs publish one, and every component has `resources: {}`. With LocalExecutor and no Celery, Redis or Postgres subchart, expect the scheduler (about 0.7 to 1.5 GiB with in-process task supervisors), api-server (about 0.5 to 1 GiB), dag-processor (about 0.3 to 0.6 GiB) and triggerer (about 0.3 GiB): **roughly 2 to 3.5 GiB steady state**, plus the KPO task pods (Spark pods 3 to 6 GiB each, see Q4). Measure with `kubectl top pods -n airflow` before you lock the resource budget (research item 11).

**Breaking changes in Airflow 3 that affect DAG code.** From the [Upgrading to Airflow 3 docs](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html):
- Imports move to the Task SDK: `airflow.decorators.dag` becomes `airflow.sdk.dag` (likewise `DAG`, `task`, `Asset`, `Variable`, `Param`, `chain`). Ruff rules **AIR301 and AIR302** flag breaking imports.
- "Task code can no longer directly import and use Airflow database sessions or models." Workers talk to the API server.
- Removed: SubDAGs ("Replaced by TaskGroups, Assets, and Data Aware Scheduling"), SLAs ("replaced with Deadline Alerts"), SequentialExecutor, and context keys such as `execution_date`, `tomorrow_ds`, `yesterday_ds`, `prev_execution_date`. `schedule_interval` became `schedule`.
- `airflow.datasets.Dataset` became `airflow.sdk.Asset`. `catchup_by_default` is now `False`. REST API v1 was removed in favour of `/api/v2`. The webserver became `airflow api-server`, and the DAG processor runs separately.
- 3.3.1 adds pandas 3 XCom-serialisation compatibility ([release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)).

#### Q2. KubernetesPodOperator, versioned images, config and outputs, simulated clock

**Provider.** `apache-airflow-providers-cncf-kubernetes` 10.22.0 (2026-09-14). The import is `from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator` ([operator docs](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)).

**XCom.** From the same page: with `do_xcom_push=True` a sidecar reads `/airflow/xcom/return.json`. "An invalid json content will fail, example `echo 'hello' > /airflow/xcom/return.json` fail and `echo '\"hello\"' > /airflow/xcom/return.json` work". XCom is pushed only on success.

Other parameters: `on_finish_action` is one of `"delete_pod"`, `"delete_succeeded_pod"`, `"delete_active_pod"` or `"keep_pod"`; `get_logs=True`; `in_cluster=True` inside the cluster. There is also a `@task.kubernetes_cmd` decorator. 3.3+ adds durable execution: "Durable execution requires Airflow 3.3 or newer, since it relies on the task state store." The `durable` parameter defaults to `True`.

**Pattern.** Pass config as CLI args or env (rendered from `dag_run.conf` or params). Pass large outputs through the object store (snapshot URI + hash). Pass small metadata (URI, hash, MLflow run id) back through XCom.

```python
# dags/train.py  — Airflow 3.3, provider cncf-kubernetes 10.22
import json, pathlib
from airflow.sdk import DAG, Asset, Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGES = json.loads((pathlib.Path(__file__).parent / "images.json").read_text())  # {"spark": "fraud/spark-jobs:git-abc123", "train": "..."}
S3_ENV = [k8s.V1EnvVar(name="AWS_ENDPOINT_URL", value="http://s3.storage:8333"),
          k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="s3-creds"))]

GOLD = Asset("s3://lake/gold/training")          # emitted by build_dataset
DRIFT = Asset("fraud://alerts/drift")            # emitted by monitoring job via REST
RETRAIN_DUE = Asset("fraud://clock/retrain-due") # emitted by sim driver every 30 sim days

def pod(task_id, image_key, args, mem="4Gi", xcom=True, **kw):
    return KubernetesPodOperator(
        task_id=task_id, name=task_id.replace("_", "-"), namespace="ml-jobs",
        image=IMAGES[image_key], image_pull_policy="IfNotPresent",   # images preloaded with `kind load docker-image`
        cmds=["python", "-m"], arguments=args,
        env_vars=[k8s.V1EnvVar(name="SIM_DATE", value="{{ dag_run.conf.get('sim_date', '') }}")],
        container_resources=k8s.V1ResourceRequirements(requests={"memory": mem, "cpu": "2"}, limits={"memory": mem}),
        do_xcom_push=xcom, get_logs=True, in_cluster=True,
        on_finish_action="delete_succeeded_pod", deferrable=True, **kw)

with DAG(
    dag_id="train",
    schedule=(DRIFT | RETRAIN_DUE),          # asset-triggered, never wall-clock
    max_active_runs=1, catchup=False,
    params={"window_days": Param(365, type="integer")},
):
    validate = pod("validate_schema", "spark", ["jobs.validate", "--layer", "silver"])        # pandera first (Q35)
    build = pod("build_dataset", "spark",
                ["jobs.build_training_set", "--as-of", "{{ dag_run.conf.get('sim_date') }}"],
                mem="6Gi", outlets=[GOLD])        # writes /airflow/xcom/return.json {"uri":..., "sha256":...}
    train = pod("train_challengers", "train",
                ["jobs.train", "--dataset-uri",
                 "{{ ti.xcom_pull(task_ids='build_dataset')['uri'] }}"])
    validate >> build >> train
```

`namespace="ml-jobs"` needs a Role/RoleBinding for the scheduler service account there, or keep the pods in the `airflow` namespace. `allowPodLaunching` only grants rights in the release namespace unless `multiNamespaceMode: true`.

**Simulated clock vs Airflow schedules.** The simulation runs about 1 sim day per 30 s (DISCOVERY Q10), so a cron or timetable `schedule` in wall-clock time is meaningless. The sim clock's owner (the replayer) should drive Airflow:

| Option | How | Pros | Cons |
|---|---|---|---|
| A. REST trigger per period | `POST /api/v2/dags/{dag_id}/dagRuns` with `{"logical_date": null, "conf": {"sim_date": "2020-03-14"}}` | Explicit. `sim_date` is visible in the UI and in conf. | The replayer must know the DAG ids. Backpressure is up to you (`max_active_runs=1` + run-id dedupe). |
| **B. Asset events (recommended)** | The replayer posts asset events (`POST /api/v2/assets/events`, `extra={"sim_date": ...}`, optional `partition_key`) for `sim-day-closed` and `retrain-due`. DAGs use `schedule=Asset(...)`. | Decoupled: producers don't know consumers, and this is the Airflow 3 idiom. Queued events **coalesce** into the next run, so a slow DAG doesn't build a backlog. `triggering_asset_events` carries the sim dates. | Needs the asset id (look it up once with `GET /api/v2/assets?uri_pattern=...`). |
| C. Partitioned assets (3.2+) | `partition_key=sim_date` with `PartitionedAssetTimetable` and mappers | One run per sim partition, with partition lineage ([3.2 blog, 2026-04-07](https://airflow.apache.org/blog/airflow-3.2.0/)). | Newer and more complex, and you don't want one run per 30-s sim day anyway. |
| D. Wall-clock cron | `schedule="*/5 * * * *"`, and the job reads the current sim time from Postgres | Simple. | Breaks reproducibility. Sim time and run identity are decoupled. |

A REST body must include `logical_date` (the 3.3.2 OpenAPI marks `TriggerDAGRunPostBody.logical_date` as required but nullable). Send `null` to avoid uniqueness clashes ([OpenAPI spec @3.3.2](https://github.com/apache/airflow/blob/3.3.2/airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml)).

#### Q3. Triggering retraining from a drift alert

These are all Airflow 3.3.2 options.

**(a) REST: asset event (recommended) or DAG-run trigger.** Authenticate by getting a JWT from `POST /auth/token` ([API auth docs](https://airflow.apache.org/docs/apache-airflow/stable/security/api.html)), then send `Authorization: Bearer <JWT>`. With the chart's FAB auth manager, create a dedicated `monitoring` user with role `Op`, store its password in a k8s Secret, and call the in-cluster service `http://airflow-api-server.airflow:8080`.

```python
# monitoring/airflow_client.py — called after the alert row is committed to Postgres (Q23)
import os, httpx

AF = os.environ.get("AIRFLOW_URL", "http://airflow-api-server.airflow:8080")

def _token(c: httpx.Client) -> str:
    r = c.post(f"{AF}/auth/token", json={"username": os.environ["AF_USER"], "password": os.environ["AF_PASSWORD"]})
    r.raise_for_status()
    return r.json()["access_token"]

def emit_drift_event(alert_id: int, sim_date: str, features: list[str]) -> None:
    with httpx.Client(timeout=10) as c:
        h = {"Authorization": f"Bearer {_token(c)}"}
        asset = c.get(f"{AF}/api/v2/assets", params={"uri_pattern": "fraud://alerts/drift"}, headers=h).json()["assets"][0]
        r = c.post(f"{AF}/api/v2/assets/events", headers=h,
                   json={"asset_id": asset["id"], "extra": {"alert_id": alert_id, "sim_date": sim_date, "features": features}})
        r.raise_for_status()
```

The `CreateAssetEventsBody` fields in the 3.3.2 OpenAPI spec are `asset_id` (required), `extra`, `partition_key` and `access_control`. In the DAG, read `triggering_asset_events` ([asset scheduling docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)):

```python
from airflow.sdk import task
@task
def collect_trigger(triggering_asset_events=None) -> dict:
    evs = [e for _, events in (triggering_asset_events or {}).items() for e in events]
    return {"alert_ids": [e.extra.get("alert_id") for e in evs if e.extra.get("alert_id")],
            "sim_date": max(e.extra["sim_date"] for e in evs)}
```

The docs describe the conditional schedule this way: `schedule=(dag1_asset | dag2_asset)` "Triggers when either asset updates". That is exactly "any drift alert OR every 30 sim days" (Q20).

**(b) Event-driven AssetWatcher on Kafka.** Kafka is already in the stack. `MessageQueueTrigger(scheme="kafka", topics=[...])` from `apache-airflow-providers-common-messaging` ([triggers docs, provider 2.1.1](https://airflow.apache.org/docs/apache-airflow-providers-common-messaging/stable/triggers.html)) runs in the triggerer. The monitoring job publishes to `drift-alerts` and no Airflow credentials are needed. Its example is `asset = Asset("kafka_queue_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)])`. It also needs the `apache-kafka` provider and a connection in the Airflow image. Only `BaseEventTrigger` subclasses work ([event scheduling docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html)). Those docs warn against triggers whose condition stays true ("likely to remain true for an extended period"), which is why a naive "Postgres has an open alert" watcher is wrong.

**(c) Sensor polling Postgres.** `SqlSensor` (common.sql provider) in a DAG scheduled every N minutes, with `mode="reschedule"`, querying `SELECT ... FROM drift_alerts WHERE handled_at IS NULL`. You then mark rows handled in a final task.

| | Pros | Cons |
|---|---|---|
| REST asset event | Push-based, low latency, idiomatic Airflow 3, alert payload in `extra`, lineage in the UI. Postgres stays the system of record. | The monitoring job holds Airflow credentials and depends on the API being up (retry and outbox). |
| Kafka AssetWatcher | No credentials, durable queue, nice streaming story. | Extra providers and connection in the image. The triggerer must be up. Newer API. |
| SqlSensor polling | Simple, and Postgres is the only contract. | Polling latency and wasted runs. A wall-clock schedule fights the sim clock. Needs "handled" bookkeeping. Least idiomatic. |

#### Q4. PySpark: versions, Java, local mode on kind, s3a, applyInPandas, Delta

**Versions (as of 2026-10-04):**
- `pyspark` **4.2.0** (2026-07-14), with patch lines 4.1.3, 4.0.4 and 3.5.9 (July 2026) ([PyPI](https://pypi.org/project/pyspark/)).
- The wheel classifiers list Python 3.10 to 3.14, so **Python 3.13 is supported**.
- `requires_dist` for `[sql]` is `pandas>=2.2.0`, `pyarrow>=18.0.0`, `numpy>=1.21`.

From the [Spark 4.2.0 docs](https://spark.apache.org/docs/latest/): "Spark runs on Java 17/21/25, Scala 2.13, Python 3.10+, and R 4.0+ (Deprecated)." Also: "Java 25 prior to version 25.0.3 support is deprecated as of Spark 4.2.0."

The [install docs](https://spark.apache.org/docs/latest/api/python/getting_started/install.html) list "pandas: >=2.2.0,<3.0.0" for Connect and pandas-on-Spark. **(Verified locally)** PySpark 4.2.0 with pandas 3.0.6 emits: "FutureWarning: PySpark does not yet fully support pandas >= 3.0.0. Some features may not work correctly. It is recommended to use pandas < 3.0.0 for now." **Pin `pandas>=2.2,<3` in the Spark job image.**

The machine's host Java is `openjdk 1.8.0_504`. PySpark 4.2 failed locally with `[JAVA_GATEWAY_EXITED]` **(verified locally)**. Run Spark only inside the container image, or install JDK 17+ on the host for notebooks and tests.

The `apache/spark` Docker Hub image has **no 4.2.0 tag yet**. The latest are `4.1.3-scala2.13-java17-python3-ubuntu` and others, pushed 2026-07-24 ([Docker Hub](https://hub.docker.com/r/apache/spark/tags)). Those images use Ubuntu's system Python, not 3.13. Build your own instead:

```dockerfile
# images/spark-jobs/Dockerfile — same image for notebooks (Q33)
FROM python:3.13-slim-trixie
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev            # pyspark[sql]==4.2.0, pandas>=2.2,<3, pyarrow, pandera[pandas,pyspark]
# s3a: hadoop-aws must match Spark's Hadoop (3.5.0) + AWS SDK v2 bundle 2.35.4 — bake, don't --packages at runtime
ARG M=https://repo1.maven.org/maven2
RUN SP=$(uv run python -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))") \
 && curl -fsSLo $SP/jars/hadoop-aws-3.5.0.jar $M/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
 && curl -fsSLo $SP/jars/bundle-2.35.4.jar   $M/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
 && curl -fsSLo $SP/jars/analyticsaccelerator-s3-1.3.1.jar $M/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar
COPY src/ ./src/
ENV PATH=/app/.venv/bin:$PATH PYSPARK_PYTHON=/app/.venv/bin/python
```

Where the jar versions come from:
- The Spark v4.2.0 `pom.xml` has `<hadoop.version>3.5.0</hadoop.version>` and `<aws.java.sdk.v2.version>2.35.4</aws.java.sdk.v2.version>` ([pom](https://github.com/apache/spark/blob/v4.2.0/pom.xml)).
- The bundled PySpark jars are `hadoop-client-api-3.5.0.jar` and `hadoop-client-runtime-3.5.0.jar` **(verified locally)**.
- `hadoop-aws` 3.5.0 depends on `software.amazon.awssdk:bundle` (2.35.4 per `hadoop-project` 3.5.0) and `analyticsaccelerator-s3` 1.3.1 at compile scope ([hadoop-aws 3.5.0 pom](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.pom)).
- **The bundle jar is 686,119,507 bytes** (Content-Length header from Maven Central). Bake it into the image once.

**SparkSession for local mode on kind against the S3 store.** The keys come from the [Hadoop 3.5.0 third-party stores doc](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/third_party_stores.html): `fs.s3a.endpoint`, `fs.s3a.path.style.access=true`, and `fs.s3a.endpoint.region` ("anything except: sdk, auto, ec2"). The same doc lists checksum knobs that "may need tuning for specific stores": `fs.s3a.request.md5.header`, `fs.s3a.checksum.generation`, `fs.s3a.checksum.validation`.

```python
# src/fraud/spark.py
import os
from pyspark.sql import SparkSession

def spark(app: str) -> SparkSession:
    return (SparkSession.builder.master(f"local[{os.environ.get('SPARK_CORES', '4')}]").appName(app)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", "/tmp/spark")                      # emptyDir volume
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["AWS_ENDPOINT_URL"])   # http://s3.storage:8333
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.checksum.generation", "false")
        .config("spark.hadoop.fs.s3a.checksum.validation", "false")
        .getOrCreate())
```

**Driver memory.** In local mode the executor lives inside the driver JVM. [Spark config docs](https://spark.apache.org/docs/latest/configuration.html), on `spark.driver.memory` (default `1g`): "In client mode, this config must not be set through the `SparkConf` directly in your application, because the driver JVM has already started at that point. Instead, please set this through the `--driver-memory` command line option or in your default properties file."

Set it in the pod with `env PYSPARK_SUBMIT_ARGS="--driver-memory 3g pyspark-shell"` or with `spark-defaults.conf` in the image. Python workers (Arrow plus pandas per group) live **outside** the JVM heap, so set the pod memory limit to about 1.5 to 2 times the driver memory (e.g. `--driver-memory 3g` with a 5 to 6 GiB limit).

**`groupBy().applyInPandas` semantics.** From the Spark v4.2.0 `group_ops.py` docstring ([source](https://github.com/apache/spark/blob/v4.2.0/python/pyspark/sql/pandas/group_ops.py)):
> "This function requires a full shuffle. If using the `pandas.DataFrame` API, all data of a group will be loaded into memory, so the user should be aware of the potential OOM risk if data is skewed and certain groups are too large to fit in memory, and can use the iterator of `pandas.DataFrame` API to mitigate this."

The iterator form was added in 4.1.0 ("Added support for an iterator of `pandas.DataFrame` API."). The function can take `(key, pdf)`.

**Ordering: the docstring and the [Arrow guide](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html) give no row-order guarantee within a group.** After a shuffle, order is arbitrary. `sortWithinPartitions` before `groupBy` is not a contract. **Sort inside the function** on `(unix_time, trans_num)`, with a deterministic tiebreak. The Arrow guide adds: "The configuration for maxRecordsPerBatch is not applied on groups and it is up to the user to ensure that the grouped data will fit into the available memory." It also gives the minimum versions: "the minimum supported versions of Pandas is 2.2.0 and PyArrow is 18.0.0."

Sizing: Sparkov has about 1.85M rows over about 1k cards, so roughly 2k rows per card. Groups are tiny and OOM is not a concern.

```python
# src/fraud/jobs/build_training_set.py — replay ADR-0002 state function per card
import pandas as pd
from fraud.features import initial_state, step          # pure: (state, txn) -> (state, features)
from fraud.features.schema import FEATURE_DDL           # Spark DDL string of the output

def replay_card(key: tuple, pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(["unix_time", "trans_num"], kind="stable")   # REQUIRED: no order guarantee
    state, rows = initial_state(key[0]), []
    for txn in pdf.itertuples(index=False):
        state, feats = step(state, txn)
        rows.append(feats)
    return pd.DataFrame.from_records(rows)

silver = spark.read.parquet(f"s3a://lake/silver/transactions/snapshot={snap}")
gold = silver.groupBy("cc_num").applyInPandas(replay_card, schema=FEATURE_DDL)
gold.write.mode("errorifexists").parquet(f"s3a://lake/gold/training/snapshot={new_snap}")
```

Use the same `step` function in serving. The parity test then compares `replay_card(pdf)` against the streaming consumer for the same card (ADR-0002).

**Delta Lake vs plain Parquet.** `delta-spark` 4.4.0 (2026-08-20) "adds Apache Spark 4.2 support" ([release](https://github.com/delta-io/delta/releases/tag/v4.4.0)). Before it, the [compat table](https://docs.delta.io/latest/releases.html) only went to Spark 4.1.

Delta gives time travel, ACID overwrite and schema enforcement. Here, though, snapshots are already immutable, write-once prefixes whose content hash goes to MLflow (Q30). Delta's log adds jars and S3 consistency concerns, and it overlaps with the snapshot and hash design.

**Recommendation: plain Parquet** in write-once `snapshot=<id>/` prefixes, with a `_MANIFEST.json` (file list plus sha256) and `mode("errorifexists")`. Mention Delta and Iceberg as the "real platform" choice in the build-vs-buy write-up.

#### Q5. pandera: versions, pandas vs PySpark APIs, one schema for both?

**Version.** `pandera` **0.33.1** (2026-09-01). 0.33.0 (2026-08-30) added a CLI and first-class pyarrow. 0.32.0 (2026-06-19) added the opt-in Narwhals backend. 0.30.0 added pandas 3 support ([PyPI](https://pypi.org/project/pandera/), [releases](https://github.com/unionai-oss/pandera/releases)).

The pandas API lives at `import pandera.pandas as pa`. The docs say using the top-level `pandera` module "will be deprecated in version `0.29.0`" ([index.md @v0.33.1](https://github.com/unionai-oss/pandera/blob/v0.33.1/docs/source/index.md)). Spark uses `import pandera.pyspark as pa` with `pandera[pyspark]`.

**Differences in PySpark** ([pyspark_sql docs](https://pandera.readthedocs.io/en/stable/pyspark_sql.html)):
- "the output of `schema.validate` will produce a dataframe in pyspark SQL even in case of errors during validation. Instead of raising the error, the errors are collected and can be accessed via the `dataframe.pandera.errors` attribute"
- Validation is lazy by default.
- "There is no support for lambda based vectorized checks since in spark lambda checks needs UDFs, which is inefficient."
- Custom checks return a scalar bool.
- dtypes should be `pyspark.sql.types`.
- `PANDERA_VALIDATION_DEPTH=SCHEMA_ONLY|DATA_ONLY|SCHEMA_AND_DATA`.

**One schema for both?** **Not as a single schema object.** Schema classes are per backend: `pandera.api.pandas...DataFrameSchema` vs `pandera.api.pyspark.container.DataFrameSchema`, and the dtype systems differ. The Narwhals backend ([narwhals_backend.md @v0.33.1](https://github.com/unionai-oss/pandera/blob/v0.33.1/docs/source/narwhals_backend.md)) unifies the check implementations:
> "Built-in checks (`isin`, `in_range`, `str_matches`, etc.) are implemented as Narwhals expressions and run unchanged on Polars LazyFrames, Ibis tables, PySpark SQL DataFrames, and pandas DataFrames when the Narwhals backend is enabled."

But you still import a backend-specific module, and "The public API (`import pandera.polars as pa`, ... `import pandera.pyspark as pa`, `import pandera.pandas as pa`) is unchanged." It is opt-in, has caveats for PySpark ("`coerce=True` is a no-op"), and raises `SchemaErrors` instead of filling `df.pandera.errors`.

**Practical pattern: one neutral column spec in `features/` (or `contracts/`) that builds both schemas.** **(Verified locally**, pandas validation and pyspark schema construction with pandera 0.33.1 and pyspark 4.2.0 on Python 3.13. Spark-side `.validate` was not run because the host has no JDK 17.)

```python
# src/fraud/contracts/transactions.py — single source of truth (Q35)
import pyspark.sql.types as T
import pandera.pandas as pap
import pandera.pyspark as pas

SPEC = {  # name: (pandas dtype, spark dtype, checks, nullable)
    "trans_num": ("str",     T.StringType(), {},                           False),
    "cc_num":    ("int64",   T.LongType(),   {"gt": 0},                    False),
    "unix_time": ("int64",   T.LongType(),   {"gt": 0},                    False),
    "amt":       ("float64", T.DoubleType(), {"ge": 0, "le": 50_000},      False),
    "category":  ("str",     T.StringType(), {"isin": CATEGORIES},         False),
    "is_fraud":  ("int64",   T.IntegerType(),{"isin": [0, 1]},             False),
}
_CHECK = {"gt": "gt", "ge": "ge", "le": "le", "isin": "isin"}

def _checks(mod, spec):  # same built-in check names exist in both modules
    return [getattr(mod.Check, _CHECK[k])(v) for k, v in spec.items()]

def pandas_schema() -> pap.DataFrameSchema:     # serving / unit tests / training-run gate
    return pap.DataFrameSchema({c: pap.Column(pd_t, _checks(pap, chk), nullable=n)
                                for c, (pd_t, _, chk, n) in SPEC.items()}, strict=True, name="txn")

def spark_schema() -> pas.DataFrameSchema:      # silver layer
    return pas.DataFrameSchema({c: pas.Column(sp_t, _checks(pas, chk), nullable=n)
                                for c, (_, sp_t, chk, n) in SPEC.items()}, strict=True, name="txn")

# silver job
def validate_silver(df):
    out = spark_schema().validate(df)
    errors = dict(out.pandera.errors)            # read immediately; "further pyspark operations may reset the attribute"
    if errors:
        raise SchemaSkew(errors)                 # Q35: abort + alert (write alert row, emit asset event)
    return out
```

A test asserts that both schemas have identical column sets and check names. That parity test catches drift between the pandas and Spark contracts.

An alternative for cross-row-free checks is to run the **pandas** schema inside `mapInPandas` / `applyInPandas`. That works per partition only, so `unique` and dataset-level checks are lost. Use it only if the Spark-native checks are missing something.

#### Q6. MinIO in 2026: status and alternatives

**Status (verified 2026-10-04):**
- `github.com/minio/minio` is **archived** (`"archived": true`, last push 2026-04-24T17:54:39Z, licence AGPL-3.0) per the GitHub API.
- The last GitHub release is `RELEASE.2025-10-15T17-29-55Z` (2025-10-16).
- `minio/operator` is also **archived** (last push 2026-03-20).

From the README ([repo](https://github.com/minio/minio)):
> "THIS REPOSITORY IS NO LONGER MAINTAINED."
> "The MinIO community edition is now distributed as source code only. We will no longer provide pre-compiled binary releases."

It points users to "AIStor Free" (standalone, free licence) and "AIStor Enterprise".

**Images:**
- `hub.docker.com/v2/repositories/minio/minio/` now returns `{"message":"object not found"}` (checked 2026-10-04).
- A third-party write-up dated 2026-09-18 says: "September 11, 2026, sometime between 18:31 and 19:36 UTC, MinIO flat-out deleted the minio/minio and minio/mc repositories on Docker Hub" ([vonng.com](https://vonng.com/en/db/silo-is-coming/), not a primary source; the Hub 404 is primary).
- Anonymous pulls from `quay.io/v2/minio/minio/tags/list` return `UNAUTHORIZED` (checked 2026-10-04).

Timeline per [vonng.com 2026-02-14](https://blog.vonng.com/en/db/minio-resurrect/) (secondary): admin console removed from CE (May 2025), binary and Docker distribution stopped (Oct 2025), maintenance mode (Dec 2025), archived (Feb to Apr 2026).

**The plain `minio/minio` Helm/compose setup will not pull.** **This contradicts Q30 ("MinIO as local GCS stand-in").**

**Alternatives (checked 2026-10-04):**

| Store | Latest | Licence | Versioning / object lock | K8s install | Fit |
|---|---|---|---|---|---|
| **SeaweedFS** | 4.48 (2026-09-28), chart 4.48.0 | Apache-2.0 | Versioning yes; Object Lock config supported ([wiki](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API)) | Helm repo `https://seaweedfs.github.io/seaweedfs/helm`. `allInOne.enabled` + `allInOne.s3.enabled`, `createBuckets` with `versioning: Enabled` / `objectLock: true` ([values.yaml](https://github.com/seaweedfs/seaweedfs/blob/master/k8s/charts/seaweedfs/values.yaml)) | **Recommended**: mature since 2015, active, versioning matches "immutable versioned snapshots" |
| RustFS | 1.0.1 (2026-10-03), chart `charts.rustfs.com` 1.0.1 | Apache-2.0 | README marks Versioning and Object Lock "Available" ([repo](https://github.com/rustfs/rustfs)) | Helm chart. MinIO-like ports 9000/9001, default `rustfsadmin/rustfsadmin` | Closest MinIO drop-in, but only just reached 1.0 |
| Garage | `dxflrs/garage` images pushed daily | AGPL-3.0 | "Garage does not (yet) support object versioning."; object lock missing ([S3 compat](https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/)) | Helm chart in repo | Light, but no versioning |
| `pgsty/silo` (MinIO fork, formerly `pgsty/minio`) | `RELEASE.2026-09-16T00-00-00Z` | AGPL-3.0 | MinIO feature set | Docker image | Keeps the "MinIO" story, but a one-maintainer fork |
| Ceph RGW (Rook) | — | LGPL | Full | Rook operator | Too heavy for a laptop |
| LocalStack | 2026.x | Proprietary single image | — | — | "Starting March 23, 2026, LocalStack for AWS will ship as a single, unified container image" that requires an auth token ([LocalStack blog](https://blog.localstack.cloud/localstack-single-image-next-steps/)). **Not suitable.** |

Minimal SeaweedFS values (keys from chart 4.48 `values.yaml`):

```yaml
# seaweedfs-values.yaml (chart seaweedfs/seaweedfs 4.48.0)
master: { enabled: false }
volume: { enabled: false }
filer:  { enabled: false }
allInOne:
  enabled: true
  updateStrategy: { type: Recreate }
  s3:
    enabled: true
    enableAuth: true
    createBuckets:
      - { name: lake,   versioning: Enabled }      # bronze/silver/gold snapshots
      - { name: mlflow, versioning: Enabled }      # MLflow artifacts
s3:
  enableAuth: true
  # credentials via s3.credentials.admin.{accessKey,secretKey} or existingConfigSecret
```

S3 port 8333 (`s3.port: 8333`). The `createBucketsHook` is a Helm hook, which Argo CD maps to a sync hook, so check that buckets appear after the first sync. Spark (s3a), MLflow (`MLFLOW_S3_ENDPOINT_URL`) and boto3/pyarrow all just take an endpoint URL. Keep the code store-agnostic: the port/adapter is "object store", and the README says "S3-compatible store standing in for GCS".

### Recommendation

1. **Airflow 3.3.2** on chart **1.22.0**, overriding `airflowVersion` and `defaultAirflowTag` to 3.3.2, with **LocalExecutor**, no Redis, Celery or Postgres subchart, the platform Postgres, a triggerer, and helm hooks off for Argo CD. Use a custom Airflow image (`apache/airflow:slim-3.3.2-python3.13` + `cncf-kubernetes`, plus the `common-messaging`/`apache-kafka` providers if used) with **DAGs baked in**. CI writes the image tags into one Helm values commit (`dags/images.json` + the chart tag), and Argo CD ships DAGs and images together (Q33). Move to `GitDagBundle` later if per-run DAG versioning is worth showing.
2. Write every task as a **KubernetesPodOperator** (`deferrable=True`, `image_pull_policy="IfNotPresent"`, immutable `git-<sha>` tags loaded with `kind load docker-image`). Large outputs go to the object store, and URI + sha256 + MLflow run id go through XCom.
3. **No wall-clock schedules.** The sim replayer is the clock. It emits Airflow **asset events** (`sim-day-closed` every K sim days, `retrain-due` every 30 sim days) via `POST /api/v2/assets/events`. The monitoring job writes the alert row to Postgres (Q23) and then emits a `drift` asset event. The `train` DAG uses `schedule=(DRIFT | RETRAIN_DUE)` with `max_active_runs=1`, so events coalesce. A Kafka `AssetWatcher` is a stretch alternative. Avoid SqlSensor polling.
4. **PySpark 4.2.0**, local mode, in a custom `python:3.13` + **OpenJDK 21** image with pandas pinned `<3`. Bake in `hadoop-aws-3.5.0` + `bundle-2.35.4` + `analyticsaccelerator-s3-1.3.1`. Use `--driver-memory 3g` in a 5 to 6 GiB pod. `applyInPandas` must **sort inside the function**. Use plain Parquet in write-once snapshot prefixes with a manifest hash, not Delta.
5. **pandera 0.33.1**. One neutral column spec generates a `pandera.pandas` schema (training gate, serving, tests) and a `pandera.pyspark` schema (silver), with a parity test between them. Read `df.pandera.errors` immediately. Any error raises `SchemaSkew`, which writes an alert and aborts the DAG (Q35).
6. **Replace MinIO with SeaweedFS** (Apache-2.0, versioning, Helm `allInOne`). Keep "MinIO/AIStor, RustFS and Garage considered" in the ADR and build-vs-buy section. Update Q30 and the glossary wording from "MinIO" to "S3-compatible object store".

### Risks and gotchas

- **Contradicts Q30.** MinIO community is archived, Docker Hub `minio/minio` is gone (404 on 2026-10-04), and quay.io needs auth. Any tutorial `image: minio/minio` breaks. Write a new ADR (object store = SeaweedFS).
- **Sim-clock speed vs Airflow latency.** 1 sim day is about 30 s, but a KPO and Spark task takes about 30 to 90 s on its own (pod start plus JVM start, an estimate) and a training run takes minutes. **Nightly per-account batch scoring cannot run every sim day** (Q29 says "nightly"). Either batch-score every K sim days (e.g. 7, which is about 3.5 wall-min), or have the replayer pause while gated jobs run. Retraining every 30 sim days is about every 15 wall-min, which is feasible only if training finishes well within that. Asset-event coalescing prevents backlog but merges triggers. Record the chosen cadence in DISCOVERY.
- Chart 1.22.0 is tested with Airflow 3.2.2, and running 3.3.2 on it is an override. If something breaks (e.g. new 3.3 config keys), fall back to 3.2.2. KPO `durable` needs 3.3. Watch for chart 1.23.
- Under Argo CD you **must** disable helm hooks for `createUserJob` and `migrateDatabaseJob`, or migrations never run.
- The chart's default FAB auth with `admin/admin` is for kind only. Use a separate `Op` user for the monitoring client, kept in a Secret. JWTs expire, so fetch a token per call or cache it with a refresh.
- `TriggerDAGRunPostBody.logical_date` is required, so pass `null`. Old `execution_date` code and v1 REST calls fail in Airflow 3.
- `allowPodLaunching` covers only the release namespace. Launching KPO pods in another namespace needs a Role/RoleBinding.
- PySpark 4.2 with pandas 3 gives a FutureWarning and partial support. Pin `pandas<3` in Spark images. Serving or training images may use pandas 3, but then the train/serve parity test must run under both, or you pin `<3` everywhere for consistency (simplest).
- The host Java is 1.8, which can't run Spark 4. Spark runs only in the container, or you install JDK 17/21 for local tests.
- The AWS SDK v2 `bundle` jar is about 686 MB, giving Spark images of about 1.5 GB. Bake it, never use `--packages` at runtime, and `kind load` it once.
- With third-party S3 stores, the AWS SDK v2 checksum defaults can fail uploads. Set the `fs.s3a.checksum.*` options and test writes early.
- `applyInPandas` has no order guarantee. If you forget the in-function sort, training features silently differ from serving, and the parity test must cover it. Groups load fully into memory (fine at about 2k rows per card).
- Setting `spark.driver.memory` in `SparkConf` after the JVM has started is ignored. Use `PYSPARK_SUBMIT_ARGS` or `spark-defaults.conf`. Python worker memory is outside the heap, so size pod limits accordingly.
- pandera-on-Spark does not raise by default. Errors sit in `df.pandera.errors` and may reset after further ops. Lambda checks are unsupported. The Narwhals backend changes this behaviour (raises, `coerce` no-op), so don't mix it in by accident (`PANDERA_USE_NARWHALS_BACKEND`).
- The SeaweedFS `createBucketsHook` is a Helm hook, and its behaviour under Argo CD differs from `helm install`. RustFS 1.0 is only a day old, and Garage lacks versioning.
- The memory figures for Airflow are estimates. Measure them for research item 11.

### Sources

- Airflow PyPI (3.3.2, 2026-09-17): https://pypi.org/project/apache-airflow/
- Airflow release notes 3.3.x: https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html
- Airflow 3.2.0 blog (2026-04-07): https://airflow.apache.org/blog/airflow-3.2.0/
- Helm chart docs (1.22.0): https://airflow.apache.org/docs/helm-chart/stable/index.html
- Helm chart index and tarball: https://airflow.apache.org/index.yaml , https://archive.apache.org/dist/airflow/helm-chart/1.22.0/airflow-1.22.0.tgz
- Executors: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html
- Upgrading to Airflow 3: https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html
- DAG bundles: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html
- KubernetesPodOperator: https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html
- cncf-kubernetes provider PyPI: https://pypi.org/project/apache-airflow-providers-cncf-kubernetes/
- Asset scheduling: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html
- Event-driven scheduling: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html
- MessageQueueTrigger: https://airflow.apache.org/docs/apache-airflow-providers-common-messaging/stable/triggers.html
- REST API auth: https://airflow.apache.org/docs/apache-airflow/stable/security/api.html
- FAB token: https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/token.html
- Simple auth manager: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html
- REST OpenAPI spec @3.3.2: https://github.com/apache/airflow/blob/3.3.2/airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml
- Airflow image tags: https://hub.docker.com/r/apache/airflow/tags
- Spark 4.2.0 overview: https://spark.apache.org/docs/latest/
- PySpark install: https://spark.apache.org/docs/latest/api/python/getting_started/install.html
- PySpark PyPI: https://pypi.org/project/pyspark/
- Spark pom v4.2.0: https://github.com/apache/spark/blob/v4.2.0/pom.xml
- applyInPandas source v4.2.0: https://github.com/apache/spark/blob/v4.2.0/python/pyspark/sql/pandas/group_ops.py
- applyInPandas API page: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html
- Arrow in PySpark: https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html
- Spark configuration: https://spark.apache.org/docs/latest/configuration.html
- apache/spark images: https://hub.docker.com/r/apache/spark/tags
- Hadoop S3A third-party stores (3.5.0): https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/third_party_stores.html
- hadoop-aws 3.5.0 pom: https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.pom
- AWS SDK bundle 2.35.4: https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/
- Delta releases / compat: https://github.com/delta-io/delta/releases/tag/v4.4.0 , https://docs.delta.io/latest/releases.html
- pandera PyPI: https://pypi.org/project/pandera/
- pandera releases: https://github.com/unionai-oss/pandera/releases
- pandera PySpark SQL: https://pandera.readthedocs.io/en/stable/pyspark_sql.html
- pandera Narwhals backend @v0.33.1: https://github.com/unionai-oss/pandera/blob/v0.33.1/docs/source/narwhals_backend.md
- pandera index (pandas import) @v0.33.1: https://github.com/unionai-oss/pandera/blob/v0.33.1/docs/source/index.md
- MinIO repo (archived): https://github.com/minio/minio
- MinIO operator (archived): https://github.com/minio/operator
- Docker Hub minio/minio (404): https://hub.docker.com/r/minio/minio
- vonng.com, Docker Hub deletion (2026-09-18, secondary): https://vonng.com/en/db/silo-is-coming/
- vonng.com, MinIO timeline (2026-02-14, secondary): https://blog.vonng.com/en/db/minio-resurrect/
- SeaweedFS S3 API: https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API
- SeaweedFS Helm chart values: https://github.com/seaweedfs/seaweedfs/blob/master/k8s/charts/seaweedfs/values.yaml , https://seaweedfs.github.io/seaweedfs/helm/index.yaml
- RustFS: https://github.com/rustfs/rustfs , https://charts.rustfs.com/index.yaml
- Garage S3 compatibility: https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/
- pgsty/silo: https://hub.docker.com/r/pgsty/silo
- LocalStack single image: https://blog.localstack.cloud/localstack-single-image-next-steps/
