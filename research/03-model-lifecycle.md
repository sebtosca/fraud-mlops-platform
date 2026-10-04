## 03. Model lifecycle

Researched 2026-10-04. Versions checked against PyPI on that date. Claims marked **[verified locally]** were run in a throwaway Python 3.13.12 venv (uv) with lightgbm 4.7.0, onnxmltools 1.16.0, onnx 1.23.1, onnxruntime 1.30.0, mlflow 3.16.1 and scikit-learn, on a 16-core Linux x86_64 machine. The scripts were not added to the repo.

### Questions

1. LightGBM continued training: exact semantics of `init_model` in `lgb.train` (adds trees on top; learning rate; categorical features must match; Dataset binning/`reference`; `keep_training_booster`), known pitfalls, whether it is a fair "stateful" analogue to Huyen's DMLS ch. 9, and the current LightGBM version.
2. ONNX export: onnxmltools vs skl2onnx, categorical support, opset, probability output format (ZipMap), onnxruntime version and Python 3.13 wheels, numeric parity, a fair latency benchmark method, and the expected speed-up for GBDTs.
3. MLflow 3.x: registry aliases, signatures, custom metadata (thresholds) on a version, `mlflow.data` with hashes, LightGBM flavor + pyfunc, and the Prompt Registry API.
4. Running an MLflow tracking server on Kubernetes with Postgres as the backend store and MinIO for artifacts: Helm charts, proxied artifacts, S3 env vars, and auth.
5. Shadow and canary routing on top of MLflow aliases, and recording the promotion decision for audit.

### Findings

#### Q1. LightGBM continued training (`init_model`)

**Version.** The latest is `lightgbm` **4.7.0**, uploaded to PyPI on 2026-07-18. It requires Python >=3.10 and ships `py3-none` manylinux wheels, so it installs on 3.13 ([PyPI JSON](https://pypi.org/pypi/lightgbm/json)). The 4.7.0 release notes say it raised the minimum Python to 3.10 and added polars inputs ([releases](https://github.com/lightgbm-org/LightGBM/releases)). The repo has **moved**: `github.com/microsoft/LightGBM` now 301-redirects to `github.com/lightgbm-org/LightGBM` (checked with `curl -I` on 2026-10-04).

**Documented semantics.** The docs describe `init_model` only as the "Filename of LightGBM model or Booster instance used for continue training" ([lgb.train](https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.train.html)). The CLI equivalent, `input_model`, says that for a train task "training will be continued from this model" ([Parameters](https://lightgbm.readthedocs.io/en/stable/Parameters.html)). The source (`engine.py` in 4.7.0) shows the actual mechanism:

```python
# lightgbm/engine.py (4.7.0), lines 232-243
if isinstance(init_model, (str, Path)):
    predictor = _InnerPredictor.from_model_file(model_file=init_model, pred_parameter=params)
elif isinstance(init_model, Booster):
    predictor = _InnerPredictor.from_booster(booster=init_model, pred_parameter=dict(init_model.params, **params))
...
train_set._update_params(params)._set_predictor(predictor)
```

`Dataset._set_predictor` then calls `_set_init_score_by_predictor`. That means continued training is **boosting from an init_score**: the old model's raw scores on the *new* data become the starting margin, new trees are fit to the remaining gradient, and the old trees are appended unchanged in front of them. If the raw data has already been freed, it raises: `"Cannot set predictor after freed raw data, set free_raw_data=False when construct Dataset to avoid this."` (`basic.py`, 4.7.0).

**Verified locally:**
- Training 200 rounds and then `lgb.train(p, d2, num_boost_round=50, init_model=b1)` gives `num_trees()==250` and `current_iteration()==250`. `b2.predict(X, num_iteration=200)` equals `b1.predict(X)` exactly, so the old trees are **frozen**.
- `init_model="b1.txt"` (a file path) and `init_model=b1` (a Booster) produce identical models.
- **Learning rate**: the new `params` apply only to the new trees. Continuing with `learning_rate=0.2` on top of a 0.05 model works without warnings. Nothing rescales the old trees.
- **Feature count mismatch** fails loudly with `LightGBMError: The number of features in data (9) is not the same as it was in training data (10)`.
- **Categorical mismatch is silent.** Continuing a model trained with `categorical_feature=[9]` on a Dataset that does not declare feature 9 as categorical **succeeds with no error**. The new trees treat it as numeric.
- **pandas `category` dtype is a silent-corruption trap.** Old window categories were `[food, gas, grocery, travel]`. In the new window `atm` appears, so pandas renumbers the codes. The continued booster stores the *new* `pandas_categorical` mapping. Under that mapping the old 30 trees score `travel` at 0.012 instead of 0.965 (`b2.predict(t, num_iteration=30)`), and the 5 new trees just learn to compensate. Related reports: [#1920](https://github.com/Microsoft/LightGBM/issues/1920) (categoricals in incremental training) and [#4634](https://github.com/Microsoft/LightGBM/issues/4634) (init_score error when continuing with categoricals).
- **Binning**: the new Dataset builds its own bin mappers from the new window. Old trees still predict correctly because the saved model stores real-valued thresholds, not bin indices. A validation Dataset must still use `reference=train_set`; the docs say "If this is Dataset for validation, training data should be used as reference." ([Dataset](https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.Dataset.html)).
- **`keep_training_booster`** is documented as "Whether the returned Booster will be used to keep training." ([lgb.train](https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.train.html)). When it is False, 4.7.0 runs `booster.model_from_string(booster.model_to_string()).free_dataset()` (engine.py line 349). That only frees the training data and still returns a `Booster`, which works as a later `init_model`. Leave it at the default for our case, because each cycle is a separate Airflow task that reloads from MLflow.

**An alternative closer to fine-tuning: `Booster.refit`.** It refits the leaf values of the *existing* trees on new data with decay: `leaf_output = refit_decay_rate * old_leaf_output + (1.0 - refit_decay_rate) * new_leaf_output` ([Parameters](https://lightgbm.readthedocs.io/en/stable/Parameters.html), `refit_decay_rate`). The Python signature is `Booster.refit(data, label, decay_rate=0.9, ..., weight=None, ...)` (`basic.py` 4.7.0, line 4905). The tree structure stays fixed and the tree count does not grow.

**Is it a fair "stateful" analogue to Huyen?** In Huyen's framing, stateful training means the model "continues training on new data (fine-tuning)". It applies to *data iteration* only; *model iteration* (new features or architecture) still needs training from scratch. She cites Grubhub cutting training cost about 45x after moving from stateless to stateful daily retraining ([Huyen, Real-time ML, 2022](https://huyenchip.com/2022/01/02/real-time-machine-learning-challenges-and-solutions.html)). LightGBM's `init_model` is a **reasonable but imperfect** analogue:
- It matches on cost and data: it only needs data since the last train, so compute is much lower. That fits Q20.
- It differs on mechanics: NN fine-tuning updates every weight, whereas `init_model` **freezes** old trees and adds corrective trees. The model grows each cycle, and old, stale splits are never revised, only counterweighted. Use `Booster.refit` if the interviewer pushes on "fine-tuning existing parameters".
- It shares Huyen's limitation: a feature-set change forces stateless training, since a feature-count mismatch errors.

Because the model grows, inference latency and ONNX size rise after each stateful cycle. The Q16 p99 gate has to catch this.

#### Q2. ONNX export

**Versions (PyPI, 2026-10-04):**

| Package | Version | Uploaded | Python 3.13 |
|---|---|---|---|
| onnxmltools | 1.16.0 | 2026-01-30 | py3-none wheel |
| skl2onnx | 1.20.0 | 2026-01-30 | py3-none wheel |
| onnx | 1.23.1 | 2026-09-29 | cp312-abi3 wheel (covers 3.13) |
| onnxruntime | 1.30.0 | 2026-09-10 | requires >=3.11; cp313 manylinux_2_28 x86_64/aarch64, macOS arm64, Windows wheels |

Source: `https://pypi.org/pypi/<pkg>/json` for each. All four installed cleanly on 3.13.12 **[verified locally]**.

**onnxmltools vs skl2onnx.**
- **onnxmltools** converts a raw `lightgbm.Booster` or `LGBMClassifier` directly with `onnxmltools.convert_lightgbm`. Its README lists LightGBM among supported toolkits ([onnxmltools](https://github.com/onnx/onnxmltools)).
- **skl2onnx** does *not* know LightGBM natively. You only need it when the LightGBM model sits inside a sklearn `Pipeline`, and then you register onnxmltools' converter with `update_registered_converter(LGBMClassifier, "LightGbmLGBMClassifier", calculate_linear_classifier_output_shapes, convert_lightgbm, options={"nocl": [True, False], "zipmap": [True, False, "columns"]})` ([skl2onnx LightGBM tutorial](https://onnx.ai/sklearn-onnx/auto_tutorial/plot_gexternal_lightgbm.html)).
- We train with `lgb.train`, which gives a Booster, so **onnxmltools alone is correct**, consistent with Q14.

**Opset.** `target_opset=15` produced a model importing `('', 9)` and `('ai.onnx.ml', 1)`; the converter emits the minimal opset it needs. Asking for `target_opset=21` fails with `target_opset 21 is higher than ... the converter support (15)`. Passing a dict such as `{'': 21, 'ai.onnx.ml': 5}` fails with a TypeError. The tree op is `TreeEnsembleClassifier` from ai.onnx.ml v1 **[verified locally]**.

**Categoricals.** Conversion works and parity holds **[verified locally]**. However, ai.onnx.ml v1 has no set-membership split, so each LightGBM categorical split becomes a chain of `BRANCH_EQ` nodes. One categorical with 500 levels in a 100-tree model produced **30,431 BRANCH_EQ nodes**, against 1,989 BRANCH_LEQ and 3,100 leaves, for a 1.26 MB model. onnxmltools issue #647 (opened 2023-08-26, still open) reports about 2x latency for high-cardinality categoricals and attributes it to "not supporting `set contains` split natively in onnxruntime" ([#647](https://github.com/onnx/onnxmltools/issues/647)). Sparkov's `merchant`, `city` and `job` columns are high-cardinality; `category` (about 14 levels) and `state` are fine.

**Output format (ZipMap).**
- With the default `zipmap=True`, the outputs are `label: tensor(int64)` and `probabilities: seq(map(int64, tensor(float)))`, a list of dicts that is slow and awkward.
- With `zipmap=False`, `probabilities: tensor(float) [None, 2]`; take `[:, 1]` as the fraud score **[verified locally]**.
- Gotcha: the `label` output is declared with shape `[1]`, so ORT logs `VerifyOutputSizes ... Expected shape from model of {1} does not match actual shape of {N}` on every batch call. It is harmless. Silence it with `SessionOptions.log_severity_level=3`, or request only `["probabilities"]` in `run()`.

**Numeric parity.** ONNX evaluates in float32 and LightGBM in float64. Max absolute probability difference was 3.1e-7 with numeric features only and 2.6e-7 with a categorical, over 5,000 rows. The top 0.3% sets were identical (15/15) **[verified locally]**. In CI, assert both `max|p_onnx - p_native| < 1e-5` and **identical decisions at the stored block/review thresholds**. Decision equality is the property that matters.

**Benchmark (measured locally).** Model: 300 trees, 63 leaves, 30 features, one categorical, `zipmap=False`. Method: `time.perf_counter_ns`, 200 warm-up calls, 3,000 timed calls for single rows and 300 for batches.

| Path | p50 | p99 |
|---|---|---|
| native `Booster.predict(row, num_threads=1)` | 32.9 µs | 57.2 µs |
| native `Booster.predict(row)` (default OpenMP threads) | 42.0 µs | **3,022 µs** |
| ORT 1 row, `intra_op_num_threads=1` | **11.6 µs** | **18.8 µs** |
| native batch 1024, 1 thread | 10.2 ms | 11.5 ms |
| ORT batch 1024, 1 thread | 6.8 ms | 7.6 ms |

So expect about **2.5-3x on single-row p50 and p99**, and about 1.5x on batches. Most of the single-row gain is avoided Python and LightGBM per-call overhead, not faster tree traversal. The LightGBM issue on per-call config re-creation describes that overhead ([lightgbm-org #2935](https://github.com/lightgbm-org/LightGBM/issues/2935)). Microsoft's lightgbm-benchmark measures with "batch size of 1 and single thread". For 100 trees, 31 leaves and 100 columns it reports 23.5 µs for the LightGBM Python API against 7.4 µs for Treelite, and labels its numbers work-in-progress ([lightgbm-benchmark inferencing](https://microsoft.github.io/lightgbm-benchmark/results/inferencing/)).

Fair-benchmark rules:
1. Same model and same float32 input, in-process, excluding feature computation.
2. Warm-up of at least 100 calls, and the session created once.
3. Pin threads to 1 on both sides. The ORT default is "Number of physical CPU Cores" intra-op threads, and spinning is on by default, trading CPU for latency ([ORT threading](https://onnxruntime.ai/docs/performance/tune-performance/threading.html)). The native OpenMP default produced a 3 ms p99 tail.
4. Report p50, p99 and p99.9 over at least 10k single-row calls, plus batch throughput separately.
5. Record CPU model, library versions and tree count in the MLflow run.
6. Re-run after each stateful cycle, because tree count grows.

#### Q3. MLflow 3.x APIs

**Version.** `mlflow` **3.16.1** was uploaded 2026-09-16. It requires Python >=3.10 and is pure-Python ([PyPI](https://pypi.org/pypi/mlflow/json)). 3.0.0 shipped 2025-06-10 and minors arrive about monthly (3.15.0 on 2026-07-31, 3.16.0 on 2026-09-04), so **pin the exact version** in both the server image and the clients.

**Aliases.** The docs define them as "a mutable, named reference to a particular version of a registered model", addressed as `models:/MyModel@champion` ([Model Registry](https://mlflow.org/docs/latest/ml/model-registry/)). The API is `set_registered_model_alias`, `get_model_version_by_alias` and `delete_registered_model_alias`. Stages are deprecated: use aliases for deployment pointers and tags for status ([registry workflow](https://mlflow.org/docs/latest/ml/model-registry/workflow/)). `MlflowClient` 3.16.1 has **no alias-history API**, only set, get and delete, so audit has to be recorded separately (see Q5).

**Thresholds with the version: three places, use the first two.**
1. `log_model(..., metadata={...})` writes into the `MLmodel` file. It is **immutable** with the artifact and readable as `pyfunc_model.metadata.metadata` **[verified locally]**.
2. A `thresholds.json` artifact, for humans and other tools.
3. Model-version tags (`set_model_version_tag`). These are mutable and queryable, so use them only as a searchable mirror.

**Datasets.** `mlflow.data.from_pandas(df, source=..., name=..., targets=...)` plus `mlflow.log_input(ds, context="training")` ([dataset docs](https://mlflow.org/docs/latest/ml/dataset/)). **Caveat:** the digest is *not* a full data hash. In 3.16.1, `compute_pandas_digest` does `df.head(MAX_ROWS)` with `MAX_ROWS = 10000`, keeps only string and numeric columns, and MD5s the result into 8 hex characters (`mlflow/data/digest_utils.py`; observed digest `9a2d9d05`). For Q30, compute your own SHA-256 of the MinIO snapshot object(s) and log it as a tag or param. Use `source=` to point at the immutable `s3://` snapshot URI.

**Verified end-to-end snippet** (works against `sqlite:///` locally and identically against the server):

```python
import mlflow, lightgbm as lgb
from mlflow import MlflowClient
from mlflow.models import infer_signature

mlflow.set_tracking_uri("http://mlflow.mlops.svc.cluster.local:5000")
mlflow.set_experiment("fraud")
ds = mlflow.data.from_pandas(train_df, source=f"s3://snapshots/{snap_id}/train.parquet",
                             name="train", targets="is_fraud")
with mlflow.start_run(run_name=f"challenger-{mode}") as run:     # mode = stateless|stateful
    mlflow.log_input(ds, context="training")
    mlflow.set_tags({"git_sha": GIT_SHA, "data_sha256": SNAP_SHA256,
                     "retrain_mode": mode, "parent_model_version": parent_v or ""})
    mlflow.log_params(params); mlflow.log_dict(config, "config.json")
    booster = lgb.train(params, dtrain, num_boost_round=n, init_model=parent_booster)  # None if stateless
    sig = infer_signature(X_val, booster.predict(X_val))
    info = mlflow.lightgbm.log_model(
        booster, name="model", signature=sig, input_example=X_val.head(3),
        registered_model_name="fraud-lgbm",
        metadata={"thresholds": {"block": t_block, "review": t_review},
                  "alert_budget": {"block": 0.003, "review": 0.007}})
    mlflow.log_dict({"block": t_block, "review": t_review}, "thresholds.json")
    mlflow.onnx.log_model(onnx_model, name="model_onnx")          # optional sibling artifact

c = MlflowClient()
c.set_registered_model_alias("fraud-lgbm", "challenger", info.registered_model_version)
m = mlflow.pyfunc.load_model("models:/fraud-lgbm@champion")       # pyfunc -> probabilities
b = mlflow.lightgbm.load_model("models:/fraud-lgbm@champion")     # native lightgbm.Booster
thr = m.metadata.metadata["thresholds"]
```

In MLflow 3, `log_model` takes `name=`, and the result carries a LoggedModel id and URI such as `models:/m-948b...`, in addition to `registered_model_version` **[verified locally]**. The parent booster for a stateful run is loaded with `mlflow.lightgbm.load_model("models:/fraud-lgbm@champion")`.

**Prompt Registry.** `mlflow.genai.register_prompt`, `load_prompt` and `set_prompt_alias`. Templates use `{{ var }}`. URIs are `prompts:/name/version` or `prompts:/name@alias`. The docs state "Prompt versions in MLflow are immutable" ([Prompt Registry](https://mlflow.org/docs/latest/genai/prompt-registry/)). **[verified locally]**:

```python
p = mlflow.genai.register_prompt(
    name="case-summary",
    template="Summarise transaction {{txn_id}} flagged with score {{score}}.",
    commit_message="v1", tags={"task": "summary"})
mlflow.genai.set_prompt_alias("case-summary", alias="production", version=p.version)
prompt = mlflow.genai.load_prompt("prompts:/case-summary@production")
text = prompt.format(txn_id=..., score=...)
```

Gotcha (from the `load_prompt` docstring, 3.16.1): alias-based loads are cached for 60 s by default (`MLFLOW_ALIAS_PROMPT_CACHE_TTL_SECONDS`), and version-based loads have no TTL. After moving an alias, the summary service picks it up within about 60 s. Pass `cache_ttl_seconds=0` in tests.

#### Q4. MLflow server on Kubernetes (Postgres + MinIO)

**Server flags (from `mlflow server --help`, 3.16.1):**
- `--backend-store-uri postgresql://...`
- `--serve-artifacts` (default **True**) proxies uploads and downloads "to the storage location that is specified by '--artifacts-destination'".
- `--artifacts-destination s3://bucket` "only applies when ... the experiment's artifact root location is http or mlflow-artifacts URI".
- With serving on, the default artifact root is `mlflow-artifacts:/`.
- `--app-name basic-auth` enables auth, and `--allowed-hosts` sets Host-header checks.

**Use proxied mode.** Clients and training pods then need only the tracking URI, and only the server holds MinIO credentials.

**S3 env vars on the server pod:** `MLFLOW_S3_ENDPOINT_URL=http://minio.<ns>.svc:9000`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `MLFLOW_S3_IGNORE_TLS=true` ([artifact store](https://mlflow.org/docs/latest/self-hosting/architecture/artifact-store/)).

**Helm.** MLflow has no official Helm chart. The maintained community chart is `community-charts/mlflow`, chart **1.11.7 / appVersion 3.16.0** as of 2026-10-04 ([chart](https://github.com/community-charts/helm-charts/tree/main/charts/mlflow)). Values excerpt:

```bash
helm repo add community-charts https://community-charts.github.io/helm-charts
```
```yaml
backendStore:
  postgres: {enabled: true, host: postgres.data.svc, port: 5432, database: mlflow, user: mlflow, password: ...}  # prefer existingSecret
artifactRoot:
  proxiedArtifactStorage: true
  s3: {enabled: true, bucket: mlflow, awsAccessKeyId: ..., awsSecretAccessKey: ...}
extraEnvVars:
  MLFLOW_S3_ENDPOINT_URL: "http://minio.data.svc:9000"
auth: {enabled: true, adminUsername: admin, adminPassword: ...}
```

The chart pins appVersion 3.16.0 while the latest client is 3.16.1, so pin client and server to the same version. If you build your own image instead, the official `ghcr.io/mlflow/mlflow` image is reported to **lack `psycopg2` and `boto3`** (a secondary source, [Vultr guide](https://docs.vultr.com/how-to-deploy-mlflow-open-source-machine-learning-experiment-tracking); I did not verify it). Building `FROM ghcr.io/mlflow/mlflow:v3.16.1` with `pip install psycopg2-binary boto3 'mlflow[auth]'` is the usual fix.

**Auth options.**
1. **Basic auth**: `mlflow server --app-name basic-auth` with `MLFLOW_FLASK_SERVER_SECRET_KEY` and `MLFLOW_AUTH_ADMIN_PASSWORD` set before first start ("at least 12 characters"). The old default `password1234` "is no longer accepted". Clients use `MLFLOW_TRACKING_USERNAME` and `MLFLOW_TRACKING_PASSWORD`. Permissions are READ, USE, EDIT, MANAGE and NO_PERMISSIONS, with RBAC from 3.13.0. Point `database_uri` in `basic_auth.ini` (via `MLFLOW_AUTH_CONFIG_PATH`) at Postgres, not the default SQLite ([basic auth](https://mlflow.org/docs/latest/self-hosting/security/basic-http-auth/)).
2. LDAP, through the chart's `ldapAuth` values.
3. For a local kind cluster: no auth and ClusterIP-only, or basic auth to show the pattern.

**Gotcha [verified locally]:** in 3.16.1, `--allowed-hosts` defaults to "localhost + private IPs". A request with `Host: mlflow.mlops.svc.cluster.local:5000` gets **HTTP 403 "Invalid Host header - possible DNS rebinding attack detected"**. Pass `--allowed-hosts mlflow,mlflow.mlops,mlflow.mlops.svc.cluster.local,mlflow.localhost` (or the chart's equivalent extra arg), or in-cluster clients that use the service DNS name will fail.

#### Q5. Shadow and canary on MLflow aliases

MLflow aliases are only **pointers**. Nothing in MLflow splits traffic.

- **KServe** offers `canaryTrafficPercent`, but "Canary rollout strategy is only supported in serverless deployment mode", which means Knative ([KServe canary](https://kserve.github.io/website/docs/model-serving/predictive-inference/rollout-strategies/canary)). That is too heavy for this kind cluster.
- **Recommended: routing inside the scorer**, where the streaming consumer decides.

How the routing works:
- **Aliases:** `champion` (serves), `challenger` (shadow or canary candidate), plus `previous_champion` for one-step rollback.
- **Scorer:** loads `models:/fraud-lgbm@champion` and `@challenger` and refreshes them on a poll of `get_model_version_by_alias`, for example every 60 s or when a version number changes.
- **Shadow:** score every transaction with both models. Only the champion's decision is applied. Write both scores, versions and decisions to the predictions table.
- **Canary:** route deterministically with `hash(card_id) % 100 < canary_pct`. Hash on card, not transaction, so a card's experience stays consistent and per-card features do not mix. The challenger's decision is applied for that slice; still log both scores.
- **Who decides `canary_pct`:** config in Git (a Helm value or ConfigMap synced by Argo CD), so the split itself is audited. The `champion` alias is moved by the **promotion job**, an Airflow task that evaluates the Q16 gate, not by a person.
- **Exploration (Q12):** the 2% exploration sample must be drawn *before* routing and applied to both arms so the IPW weights stay valid.

The promotion job, in pseudocode:

```python
gate = evaluate(champion_v, challenger_v, window)   # fraud-$ recall @ budget, PR-AUC, p99
decision = {"from": champion_v, "to": challenger_v, "phase": "shadow->canary|canary->champion",
            "metrics": gate.metrics, "passed": gate.passed, "rule": "Q16 v1",
            "window": window, "git_sha": GIT_SHA, "decided_at": now_iso()}
with mlflow.start_run(run_name=f"promotion-{challenger_v}", experiment_id=PROMOTION_EXP):
    mlflow.log_metrics(gate.flat_metrics); mlflow.log_dict(decision, "decision.json")
    mlflow.set_tags({"decision": "promote" if gate.passed else "reject",
                     "champion_version": champion_v, "challenger_version": challenger_v})
c.set_model_version_tag("fraud-lgbm", challenger_v, "promotion_decision", "promote" if gate.passed else "reject")
c.set_model_version_tag("fraud-lgbm", challenger_v, "promotion_run_id", run.info.run_id)
if gate.passed and phase == "canary->champion":
    c.set_registered_model_alias("fraud-lgbm", "previous_champion", champion_v)
    c.set_registered_model_alias("fraud-lgbm", "champion", challenger_v)
# also INSERT decision row into Postgres `model_promotions` (append-only) — aliases keep no history
```

Audit trail:
- An **immutable** record: the promotion run, whose artifact cannot be edited after logging.
- **Queryable** tags on the version.
- An **append-only Postgres table**, the only place that captures alias history, because MLflow keeps only the current alias target.
- **Git** for the canary percentage.

### Recommendation

- **Stateful challenger:** `lgb.train(params, dtrain_new, num_boost_round=k, init_model=champion_booster)` with `free_raw_data=False`, a fixed `categorical_feature` index list, and IPW `weight=` on the new Dataset.
  - Owners of the feature code must encode categoricals as **stable integer codes from a frozen vocabulary**, never pandas `category` dtype.
  - Cap cumulative trees, for example by forcing a stateless reset when trees exceed 2x baseline or every N cycles.
  - Log `retrain_mode`, `parent_model_version`, tree count, training wall-clock and CPU-seconds.
  - Optionally add `Booster.refit` as a third variant to discuss "true" fine-tuning.
- **ONNX:** `onnxmltools.convert_lightgbm(booster, initial_types=[("input", FloatTensorType([None, F]))], zipmap=False, target_opset=15)`.
  - CI parity test: max abs diff under 1e-5 and identical decisions at the stored thresholds.
  - Keep high-cardinality columns (merchant, city, job) out of LightGBM categoricals; use frequency or target-rate features computed in the shared feature function instead. Low-cardinality `category` and `state` are fine as categoricals.
  - Benchmark single-threaded on both sides with warm-up and p50/p99/p99.9. Expect about 2.5-3x single-row.
- **MLflow:** pin 3.16.x in both server and clients.
  - Thresholds go in `log_model(metadata=...)` plus a `thresholds.json` artifact.
  - Log your own SHA-256 of the snapshot; do not rely on `mlflow.data` digest.
  - Aliases: `champion`, `challenger`, `previous_champion`.
  - Prompts for the case-summary service live in the prompt registry with a `production` alias.
- **Server:** the `community-charts/mlflow` chart with Postgres backend, proxied artifacts to MinIO (`MLFLOW_S3_ENDPOINT_URL`), basic auth with its DB in Postgres, and `--allowed-hosts` set for service DNS names. Credentials come from Kubernetes Secrets.
- **Routing:** shadow and canary live in the scorer, keyed by card hash, with the percentage in Git. An Airflow promotion task moves aliases and writes an MLflow promotion run, version tags and a Postgres audit row.

### Risks and gotchas

- **Nuance on Q14/Q20 (not a contradiction):** `init_model` is additive boosting from an init_score, not parameter fine-tuning. Old trees are frozen and stale splits are never revised. Say this explicitly in the interview. `Booster.refit` is the closer analogue.
- **Silent warm-start corruption:** an undeclared categorical, or pandas category codes renumbered when a new level appears, gives no error. The old trees silently mis-route **[verified locally]**. Guard with a test that asserts `b_new.predict(X, num_iteration=old_iters) == b_old.predict(X)` on a fixed sample.
- **Tree-count growth** across stateful cycles raises latency and ONNX size. That can fail the Q16 p99 gate and skew the Q20 compute comparison over time.
- **Possible conflict with Q6/Q14 (ONNX benchmark):** onnxmltools caps at opset 15 and ai.onnx.ml v1, so there is no BRANCH_MEMBER. High-cardinality categoricals expand into tens of thousands of BRANCH_EQ nodes and can **erase the ONNX speed-up** (#647, about 2x slower).
- **Native LightGBM default threading** gave a 3 ms p99 on single rows. Always pass `num_threads=1` for per-event scoring, or the champion's latency baseline is unfair.
- **ONNX float32 vs native float64:** scores near a threshold can flip. Test decisions, not only scores.
- **Conflict with Q30 ("data hash"):** the MLflow dataset digest is an 8-character MD5 of the first 10,000 rows of string and numeric columns only. It is not a reproducibility hash, so log a full SHA-256.
- **Aliases keep no history** in MLflow. Without the Postgres table and promotion runs there is no audit of who was champion when.
- **`--allowed-hosts`** blocks in-cluster DNS names with a 403 by default in 3.16.x **[verified locally]**.
- **Fast MLflow release cadence** (about monthly minors): Helm appVersion 3.16.0 vs client 3.16.1. Mismatched client and server can break registry or prompt APIs, so pin both.
- **Prompt alias cache TTL is 60 s:** a new prompt version is not live immediately. Account for it in eval and rollback tests.
- **KServe canary needs Knative serverless mode.** Do not adopt it for kind, and keep routing in-app (consistent with Q16's "canary if time allows").
- **Shadow plus censored labels:** challenger blocks in shadow are never actioned, so the challenger gets labels for its would-be blocks that the champion does not. The comparison is only fair on matured labels with the exploration sample.

### Sources

- LightGBM `lgb.train` docs: https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.train.html
- LightGBM Parameters (`input_model`, `categorical_feature`, `refit_decay_rate`): https://lightgbm.readthedocs.io/en/stable/Parameters.html
- LightGBM Dataset (`reference`): https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.Dataset.html
- LightGBM releases (repo moved to lightgbm-org): https://github.com/lightgbm-org/LightGBM/releases
- LightGBM PyPI: https://pypi.org/pypi/lightgbm/json
- LightGBM issues: https://github.com/Microsoft/LightGBM/issues/1920 , https://github.com/Microsoft/LightGBM/issues/4634 , https://github.com/Microsoft/LightGBM/issues/1352 , https://github.com/lightgbm-org/LightGBM/issues/2935
- Huyen, "Real-time machine learning: challenges and solutions" (2022): https://huyenchip.com/2022/01/02/real-time-machine-learning-challenges-and-solutions.html
- onnxmltools: https://github.com/onnx/onnxmltools ; categorical perf issue #647: https://github.com/onnx/onnxmltools/issues/647
- skl2onnx LightGBM tutorial: https://onnx.ai/sklearn-onnx/auto_tutorial/plot_gexternal_lightgbm.html
- ONNX Runtime threading: https://onnxruntime.ai/docs/performance/tune-performance/threading.html
- PyPI metadata: https://pypi.org/pypi/onnxruntime/json , https://pypi.org/pypi/onnx/json , https://pypi.org/pypi/onnxmltools/json , https://pypi.org/pypi/skl2onnx/json , https://pypi.org/pypi/mlflow/json
- LightGBM inference benchmark: https://microsoft.github.io/lightgbm-benchmark/results/inferencing/
- MLflow Model Registry: https://mlflow.org/docs/latest/ml/model-registry/ ; workflow: https://mlflow.org/docs/latest/ml/model-registry/workflow/
- MLflow datasets: https://mlflow.org/docs/latest/ml/dataset/
- MLflow Prompt Registry: https://mlflow.org/docs/latest/genai/prompt-registry/
- MLflow artifact store: https://mlflow.org/docs/latest/self-hosting/architecture/artifact-store/
- MLflow basic auth: https://mlflow.org/docs/latest/self-hosting/security/basic-http-auth/
- community-charts MLflow Helm chart: https://github.com/community-charts/helm-charts/tree/main/charts/mlflow
- Secondary source on the MLflow image missing psycopg2/boto3: https://docs.vultr.com/how-to-deploy-mlflow-open-source-machine-learning-experiment-tracking
- KServe canary: https://kserve.github.io/website/docs/model-serving/predictive-inference/rollout-strategies/canary
- Local source inspection (installed 2026-10-04): `lightgbm/engine.py` and `lightgbm/basic.py` (4.7.0); `mlflow/data/digest_utils.py`, `mlflow/tracking/client.py`, `mlflow/genai/prompts/__init__.py` and `mlflow server --help` (3.16.1)
