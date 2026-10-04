## 08. Resource budget and MLOps Level 2 mapping

Researched 2026-10-04. Versions and dates are recorded inline because most of this changes over time. Figures marked **(doc)** come from a vendor manifest or document. Figures marked **(est.)** are planning estimates: almost none of these Helm charts set default requests (`resources: {}`), so an estimate is the only number available until we measure with `kubectl top` on the real cluster.

### Questions

1. What memory and CPU requests do the vendors document for each component? Sum them for (a) the full stack with `staging` and `prod` namespaces and (b) a slimmed configuration. Does either fit in ~17 GiB free RAM? Which services should be shared across namespaces and which duplicated?
2. kind specifics: current version and node image, single-node vs multi-node, reaching services (extraPortMappings / ingress), `kind load` vs a local registry and how each interacts with Argo CD, Docker resource considerations, known overheads. Lighter alternatives (k3d, minikube).
3. Map every named Level 1 and Level 2 component, the six stages, and the CI and CD test lists in Google Cloud's *MLOps: Continuous delivery and automation pipelines in machine learning* to this project's components, and mark the gaps.
4. In what order should things be brought up so Phase 1 runs on the lightest footprint (Compose) and Phase 2 moves to kind without rewriting services? Which abstractions keep that move cheap?

### Findings

#### Q1. Resource budget

**Host state measured on 2026-10-04** (`free -g`, `docker info`): 31 GiB total, 20 GiB "available" at the time of measurement (DISCOVERY.md says ~17 GiB is typical), 19 GiB swap, 16 CPUs. Docker uses cgroup v2 with the systemd driver. **Docker's only runtimes are `runc` and `io.containerd.runc.v2`, so the NVIDIA runtime is not configured yet.** On Linux, kind nodes are plain containers with no VM memory cap, so every pod draws directly on host RAM.

##### Per-component numbers

| Component (version, date) | What the vendor documents | Planning request: memory / CPU | Notes |
|---|---|---|---|
| kind node: control plane (etcd, apiserver, controller-manager, scheduler), kubelet, containerd, CoreDNS ×2, kindnet, kube-proxy, local-path-provisioner (kind v0.33.0, 2026-08-26) | kind documents no figure. kubeadm asks for "2 GB or more of RAM per machine (any less will leave little room for your apps)" and "2 CPUs or more for control plane machines" ([kubeadm](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/install-kubeadm/)). For comparison, a k3s "server with a workload" measured 1596–1606 M ([k3s resource profiling](https://docs.k3s.io/reference/resource-profiling)) | **1.5 GiB / 1 CPU (est.)** | Fixed cost of every kind node. A multi-node cluster pays it again per node (kubelet, containerd, kube-proxy, kindnet). |
| Strimzi Cluster Operator (1.2.0, 2026-08-20) | **(doc)** requests `cpu: 200m, memory: 384Mi`; limits `cpu: 1000m, memory: 384Mi` ([060-Deployment](https://github.com/strimzi/strimzi-kafka-operator/blob/1.2.0/install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml), same in the [Helm values](https://github.com/strimzi/strimzi-kafka-operator/blob/1.2.0/helm-charts/helm3/strimzi-kafka-operator/values.yaml)) | 384 Mi / 0.2 | One operator can serve several namespaces. |
| Kafka, single dual-role KRaft node (Kafka 4.3.1 in the Strimzi 1.2.0 `kafka-single-node.yaml` example) | The example sets no resources. Strimzi: "If a memory limit (and request) is not specified, a JVM's minimum heap size is set to `128M`. The JVM's maximum heap size is not defined…" When a limit is set, the operator gives Kafka 50% of it as heap, capped at 5 GB ([Strimzi docs source](https://github.com/strimzi/strimzi-kafka-operator/blob/1.2.0/documentation/modules/con-common-configuration-properties.adoc)) | **1.25 GiB request = limit, `-Xms/-Xmx 512m` / 0.25 (est.)** | Without a limit the heap can grow without bound. Always set one on a shared host. The example also asks for a **100Gi** PVC; shrink it to about 10Gi. |
| Strimzi Entity Operator (Topic + User Operator, two JVM containers) | The example sets no resources | 2 × 256 Mi (est.). With `userOperator` dropped: 256 Mi | We have no Kafka auth, so the User Operator is not needed. |
| PostgreSQL | `shared_buffers`: "The default is typically 128 megabytes (128MB)" ([PG docs, runtime-config-resource](https://www.postgresql.org/docs/current/runtime-config-resource.html)) | 512 Mi / 0.25 (est.) | One instance holding several databases. |
| MLflow tracking server (3.16.1, 2026-09-17) | No official Helm chart. The server starts uvicorn or gunicorn with `workers or 4`, so the default is **4 worker processes** ([mlflow/server/__init__.py L440/L466](https://github.com/mlflow/mlflow/blob/v3.16.1/mlflow/server/__init__.py)) | **512 Mi / 0.25 with `--workers 1` (est.)**. About 1.5 GiB with the default 4 workers (est.) | Pass `--workers 1` (or 2). |
| Object store ("MinIO", see the risk below) | The MinIO repo is **archived** and no longer publishes images. See Risks. | 256–512 Mi / 0.1 (est.) for SeaweedFS or similar | |
| Airflow, official chart **1.22.0** (2026-06-13, `appVersion: 3.2.2`). Airflow core is at 3.3.2 (2026-09-17) | **(doc)** Every component has `resources: {}` ([values.yaml](https://github.com/apache/airflow/blob/helm-chart/1.22.0/chart/values.yaml)). The chart defaults to `executor: "CeleryExecutor"`, which adds Redis and a Celery worker. Airflow core defaults to `LocalExecutor`; `[api] workers` defaults to 1 and `[dag_processor] parsing_processes` to 2 ([config.yml 3.3.2](https://github.com/apache/airflow/blob/3.3.2/airflow-core/src/airflow/config_templates/config.yml)). For the whole Compose stack, Airflow says "allocate at least 4GB memory for the Docker Engine (ideally 8GB)" ([docker-compose howto](https://github.com/apache/airflow/blob/3.3.2/airflow-core/docs/howto/docker-compose/index.rst)) | api-server 768 Mi, scheduler 768 Mi, dag-processor 512 Mi, triggerer 384 Mi. **Slim total ≈ 2.4 GiB / 1.1 CPU (est.)**. Chart defaults (Celery worker about 1.5 Gi, Redis 128 Mi, statsd 64 Mi, embedded Postgres 256 Mi) **≈ 4.4 GiB (est.)** | The embedded Postgres image is `bitnamilegacy/postgresql:16.1.0-debian-11-r15`, which no longer gets updates. Disable it (`postgresql.enabled: false`) and use the shared Postgres. |
| Argo CD (3.5.3, 2026-09-14; argo-helm `argo-cd` chart 10.9.6) | **(doc)** The upstream `install.yaml` and `core-install.yaml` set **no resource requests**. Full install has 7 workloads: application-controller, repo-server, server, redis, dex, applicationset, notifications. Core has 4: application-controller, repo-server, redis, applicationset ([manifests v3.5.3](https://github.com/argoproj/argo-cd/tree/v3.5.3/manifests); [Argo CD Core](https://github.com/argoproj/argo-cd/blob/v3.5.3/docs/operator-manual/core.md)) | Full ≈ 1.0 GiB, core ≈ 0.6 GiB (est.). Full with dex and notifications off ≈ 0.8 GiB (est.) | Core has no API server, no RBAC and no notifications. The UI runs locally through `argocd admin dashboard`. |
| vLLM (0.30.0, 2026-09-22), host-side RAM | **(doc)** The vLLM Kubernetes guide's example uses `requests: cpu: "2", memory: 6G` and `limits: cpu: "10", memory: 20G`, plus a `/dev/shm` `emptyDir` with `medium: Memory, sizeLimit: "2Gi"` ([k8s.md v0.30.0](https://github.com/vllm-project/vllm/blob/v0.30.0/docs/deployment/k8s.md)). `--swap-space` no longer appears in `vllm/config`. The image is **8.7 GB compressed** (amd64, [Docker Hub](https://hub.docker.com/r/vllm/vllm-openai/tags)) | **6 GiB request, 10 GiB limit / 2 CPU (est.)** | Weights live in VRAM. Host RAM goes to the Python/torch/CUDA runtime, page cache while weights load, and shm. With TP=1, a smaller shm (about 1Gi) is enough. |
| Spark job pod, PySpark local mode (Spark 4.2.0 is the newest on archive.apache.org) | **(doc)** `spark.driver.memory` default `1g`. In client mode it has to be set with `--driver-memory`, not through `SparkConf` ([configuration.md](https://github.com/apache/spark/blob/master/docs/configuration.md)) | **2.5 GiB limit / 4 CPU with `local[4]` (est.)**: about 1.5g heap, plus JVM overhead, plus Python workers for `applyInPandas` | Transient, runs only while the task runs. `local[*]` would take all 16 cores. |
| LightGBM training pod | none | 1.5 GiB / 2 CPU (est.) | Transient. |
| App services, per env: scoring API, monitoring job, case-summary service | none | 384 + 384 + 192 Mi ≈ 1 GiB / 0.6 CPU per env (est.). Replayer 256 Mi | |
| kube-prometheus-stack (91.9.0, 2026-10-02) | **(doc)** Defaults are `resources: {}`. The commented example for Prometheus is `memory: 400Mi` ([values.yaml](https://github.com/prometheus-community/helm-charts/blob/kube-prometheus-stack-91.9.0/charts/kube-prometheus-stack/values.yaml)) | ≈ 2 GiB for the whole stack (est.) | Too heavy for this budget. |
| Grafana alone | **(doc)** commented example `memory: 128Mi` ([grafana-community values](https://github.com/grafana-community/helm-charts/blob/main/charts/grafana/values.yaml)) | 256 Mi / 0.1 | A Postgres datasource is enough. Alerts already land in Postgres (Q6/Q23). |
| Local registry container (`registry:3`) | none | 64 Mi | Runs outside the cluster. |
| Outside the cluster: self-hosted GitLab runner and its Docker builds | none | 1–3 GiB bursts (est.) | Counts against the same 17 GiB. |

##### Sums

**(a) Full stack, every service duplicated in `staging` and `prod`, chart defaults:**

| Item | GiB |
|---|---|
| kind node | 1.5 |
| Strimzi operator (shared) | 0.375 |
| Kafka + Entity Operator ×2 | 3.5 |
| Postgres ×2 | 1.0 |
| MLflow, 4 workers, ×2 | 3.0 |
| Object store ×2 | 1.0 |
| Airflow chart defaults (Celery, own Postgres) ×2 | 8.8 |
| App services ×2 + replayer ×2 | 2.5 |
| vLLM (only one GPU, so it cannot be duplicated) | 6.0 |
| Argo CD full | 1.0 |
| kube-prometheus-stack | 2.0 |
| registry | 0.06 |
| **Steady state** | **≈ 30.7** |
| + Spark pod + training pod running together | +4.0 → **≈ 34.7 peak** |

This **does not fit** in 17 GiB, and not even in the full 31 GiB.

**(b) Slim configuration, platform shared and only the apps duplicated:**

| Item | GiB |
|---|---|
| kind node (single node) | 1.5 |
| Strimzi operator | 0.375 |
| One Kafka broker (1.25) + Topic Operator only (0.25) | 1.5 |
| One Postgres (DBs: `airflow`, `mlflow`, `fraud_staging`, `fraud_prod`) | 0.5 |
| One MLflow, `--workers 1` | 0.5 |
| One object store | 0.4 |
| One Airflow: LocalExecutor + KubernetesPodOperator, no statsd, external DB | 2.4 |
| App services ×2 + one replayer | 2.25 |
| vLLM | 6.0 |
| Argo CD core | 0.6 |
| Grafana only | 0.25 |
| registry | 0.06 |
| **Steady state, vLLM on** | **≈ 16.3** |
| + one batch pod at a time (Spark 2.5) | **≈ 18.8 peak**: does **not** fit in 17 GiB |
| **Steady state, vLLM scaled to 0** | **≈ 10.3** |
| + one batch pod | **≈ 12.8 peak**: fits, with about 4 GiB headroom |

Verdict: the slim layout fits in about 17 GiB only if **(1)** vLLM runs at `replicas: 0` by default and is scaled up for the LLM part of the demo (or the staging apps scale to 0 while it runs), and **(2)** Airflow runs one heavy pod at a time (a pool with 1 slot for Spark and training). CPU is not the limit: summed slim requests are about 6–7 of 16 cores. **Memory is the binding constraint.**

##### What to share and what to duplicate

| Share once, in a `platform` (or `kafka` / `airflow` / `argocd`) namespace | Duplicate per env (`staging`, `prod`) |
|---|---|
| Strimzi operator + **one** Kafka cluster. Env isolation uses topic prefixes (`staging.transactions`, `prod.transactions`) and per-env consumer groups. | Scoring service, monitoring job, case-summary service |
| **One** Postgres with one database (or schema) per env, plus `airflow` and `mlflow` | Their ConfigMaps and Secrets (DB name, topic prefix, MLflow alias name) |
| **One** MLflow tracking server and registry. A single registry is the normal setup: promotion moves aliases (`challenger`, `champion`) or per-env registered-model names. The 3.16 CLI also lists `--enable-workspaces`, not evaluated here ([cli](https://github.com/mlflow/mlflow/blob/v3.16.1/mlflow/cli/__init__.py)). | Optional: the replayer, if staging needs its own lower-rate stream |
| **One** object store with per-env buckets or prefixes | |
| **One** Airflow. Pipeline CD per env uses two **DAG bundles** pinned to different git refs (see Q3/Q4) | |
| **One** vLLM (one GPU, `nvidia.com/gpu: 1`) | |
| Argo CD, Grafana | |

Sharing matches how the GKE target would usually look: one Cloud SQL instance, one GCS project and one Kafka cluster serving several namespaces.

#### Q2. kind specifics

**Version.** kind **v0.33.0** (2026-08-26). "The default node image is now `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5`". The release notes say: "You *must* use the `@sha256` digest to guarantee an image built for this release" ([release v0.33.0](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0)).

**Single-node vs multi-node.** Use **single-node**. Each extra node is another container with its own kubelet and containerd, so the per-node overhead in Q1 repeats. `kind load` also copies every image into every node. Multi-node only shows scheduling (node labels or taints), and that adds nothing for this demo. The exception is GPU: NVIDIA's `nvkind` builds a cluster whose GPU-bearing **worker** nodes come from its own config template. If we go that way, the extraPortMappings and registry config have to move into that template ([nvkind README](https://github.com/NVIDIA/nvkind)). NVIDIA's own wording: "running `kind` with access to GPUs is not very straightforward. There is no standard way to inject GPUs support into a `kind` worker node". nvkind also needs `nvidia-container-toolkit` configured for Docker, which is **not configured on this host** (see Q1).

**Reaching services.** There are two documented options.

- `extraPortMappings` to NodePorts ([configuration](https://kind.sigs.k8s.io/docs/user/configuration/#extra-port-mappings)). The docs map `containerPort: 30950` to `hostPort: 80` and then set the Service `nodePort` to 30950.
- Ingress. The current guide uses **cloud-provider-kind v0.9.0+**, which "natively supports Ingress. No third-party ingress controllers are required by default". Gateway API is supported too ([ingress](https://kind.sigs.k8s.io/docs/user/ingress/)). cloud-provider-kind runs as a separate host process.

For a single-user demo, NodePorts bound to 127.0.0.1 are the simplest option and need no extra process:

```yaml
# kind-config.yaml  (kind v0.33.0)
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: fraud
nodes:
- role: control-plane
  image: kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
  extraPortMappings:
  - {containerPort: 30080, hostPort: 8080, listenAddress: "127.0.0.1"}  # Argo CD UI (full install) / not needed for core
  - {containerPort: 30500, hostPort: 5000, listenAddress: "127.0.0.1"}  # MLflow
  - {containerPort: 30808, hostPort: 8081, listenAddress: "127.0.0.1"}  # Airflow api-server UI
  - {containerPort: 30300, hostPort: 3000, listenAddress: "127.0.0.1"}  # Grafana
  - {containerPort: 30001, hostPort: 8001, listenAddress: "127.0.0.1"}  # scoring API, staging (k6 target)
  - {containerPort: 30002, hostPort: 8002, listenAddress: "127.0.0.1"}  # scoring API, prod
  extraMounts:
  - hostPath: /home/professorx/projects/fraud-ml-platform/data   # Sparkov CSVs, read by Spark pods via hostPath PV
    containerPath: /data
```

**Images: `kind load` vs a local registry.**

- `kind load docker-image my-app:tag` copies an image into the node's containerd. kind warns that the default pull policy becomes `Always` for `:latest` or an omitted tag, so "don't use a `:latest` tag" and/or set `imagePullPolicy: IfNotPresent` or `Never` ([quick-start, Loading an Image](https://kind.sigs.k8s.io/docs/user/quick-start/#loading-an-image-into-your-cluster)).
- A local registry (`registry:3` on `127.0.0.1:5001`, plus a containerd `hosts.toml` that aliases `localhost:5001` to `kind-registry:5000`). Per the docs, "Pod manifests / pod specs / pod YAML should use `localhost:5001`". The script notes that "the containerd config patch is not necessary with images from kind v0.27.0+" ([local-registry](https://kind.sigs.k8s.io/docs/user/local-registry/), [kind-with-registry.sh](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/site/static/examples/kind-with-registry.sh)).

How this interacts with Argo CD: **Argo CD never pulls container images.** It renders the Helm charts and applies them. The kubelet inside the kind node does the pulling.

- With `kind load`, a GitOps tag bump in git can sync before anyone has loaded the image. The pod then sits in `ErrImagePull` because the image name points at no registry. Every bump needs a manual or CI `kind load`, which breaks the "git is the only input" story.
- With a **local registry**, the self-hosted GitLab runner on the same machine pushes `localhost:5001/fraud/<svc>:<git-sha>`, bumps `deploy/values-staging.yaml`, and Argo CD syncs. The kubelet pulls from the registry, so the flow is the same shape as GKE pulling from Artifact Registry. Only `image.registry` changes per environment.
- A third option is pulling straight from `registry.gitlab.com`. It needs no extra container but costs bandwidth and an imagePullSecret if the project is private.

**Recommendation: local registry** for anything Argo CD deploys. Use `kind load` only for one-off experiments.

**Docker and host considerations.**

- *inotify*: kind documents "too many open files" pod errors and suggests `fs.inotify.max_user_watches=524288` and `fs.inotify.max_user_instances=512` ([known issues](https://kind.sigs.k8s.io/docs/user/known-issues/#pod-errors-due-to-too-many-open-files)). This host has `max_user_watches = 65536` (instances 1024 is fine), so **raise the watches limit**.
- *Disk eviction*: kind documents kubelet "must evict pod(s) to reclaim ephemeral-storage" and recommends `docker system prune` ([known issues](https://kind.sigs.k8s.io/docs/user/known-issues/#failing-to-properly-start-cluster)). Images get stored twice: once in the host Docker and once in node containerd, and a third time in the registry. vLLM alone is 8.7 GB compressed. With about 600 GB free this is fine, but prune regularly.
- *No memory cap*: on Linux there is no Docker Desktop VM, so the node container can use all host RAM. The only protection is setting **requests and limits on every pod**. A runaway JVM (Kafka with no limit) or `local[*]` Spark will squeeze the user's desktop apps first.

**Lighter alternatives (brief).**

- **k3d v5.9.0** (2026-06-02) runs k3s in Docker. k3s lists a 2 core / 2 GB minimum, and a measured server with a workload used about 1.6 GB ([k3s requirements](https://docs.k3s.io/installation/requirements), [profiling](https://docs.k3s.io/reference/resource-profiling)). It ships Traefik and a local-path provisioner, and has built-in `k3d registry create`. GPU needs a custom-built k3s image, because "The native K3s image is based on Alpine but the NVIDIA container runtime is not supported on Alpine yet" ([k3d CUDA](https://github.com/k3d-io/k3d/blob/v5.9.0/docs/usage/advanced/cuda.md)). The saving over kind is a few hundred MiB at most.
- **minikube v1.39.0** (2026-09-02) lists minimums of "2 CPUs or more", "2GB of free memory" and "20GB of free disk space" ([start](https://minikube.sigs.k8s.io/docs/start/)). It has **first-class GPU support** with the docker driver: `minikube start --driver docker --container-runtime docker --gpus all` ([NVIDIA tutorial](https://minikube.sigs.k8s.io/docs/tutorials/nvidia/)). That is materially easier than nvkind, though it still needs the NVIDIA Container Toolkit.
- Neither one changes the memory verdict. What dominates is the workloads, not the distribution.

#### Q3. Google Cloud MLOps Level 1 / Level 2 mapping

Source: [MLOps: Continuous delivery and automation pipelines in machine learning](https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning). The page says "Last reviewed 2024-08-28 UTC" and was fetched 2026-10-04. Content is CC BY 4.0. Quotes are exact and anchors point to the article's sections. Legend: ✅ covered by a decision · 🟡 partial or needs an explicit task · ❌ gap · ⛔ deliberately not adopted.

**Level definitions.**

- Level 1 ([#mlops_level_1](https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning#mlops_level_1_ml_pipeline_automation)): "The goal of level 1 is to perform continuous training of the model by automating the ML pipeline". It requires "automated data and model validation steps to the pipeline, as well as pipeline triggers and metadata management."
- Level 2 ([#mlops_level_2](https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning#mlops_level_2_cicd_pipeline_automation)): "For a rapid and reliable update of the pipelines in production, you need a robust automated CI/CD system."

**Level 1 characteristics**

| Article characteristic (quoted) | Project component | Status |
|---|---|---|
| Rapid experiment: "The steps of the ML experiment are orchestrated." | Airflow DAG drives validate → features → train → evaluate. Notebooks use the same image (Q33) | ✅ |
| CT of the model in production: "automatically trained in production using fresh data based on live pipeline triggers" | Drift or schedule triggers the retraining DAG (Q20) | ✅ |
| Experimental-operational symmetry: "The pipeline implementation that is used in the development or experiment environment is used in the preproduction and production environment" | The same images and DAGs run in staging and prod, via DAG bundles pinned to `main` and to tags | 🟡 Needs the DAG-bundle setup in Q4. With one Airflow and no bundles, staging and prod pipelines are not separate. |
| Modularized code for components and pipelines, "components should ideally be containerized" | One image per task, run with KubernetesPodOperator (Q33). Ports-and-adapters (Q30) | ✅ |
| Continuous delivery of models: "The model deployment step… is automated." | Shadow → canary → `champion` alias move (Q16). Scoring service hot-reloads by alias | ✅ (canary is "if time allows") |
| Pipeline deployment: "you deploy a whole training pipeline" | CD ships DAGs and images together (Q33) | ✅ |

**Level 1 additional components**

| Article component and definition (quoted) | Project component | Status |
|---|---|---|
| **Data validation**: "required before model training to decide whether you should retrain the model or stop the execution of the pipeline." | pandera first in every training run and in the silver layer (Q35) | ✅ |
| Data schema skews: "you should stop the pipeline so the data science team can investigate." | pandera schema failure aborts retraining and writes an alert (Q35) | ✅ |
| Data values skews: "significant changes in the statistical properties of data… you need to trigger a retraining" | PSI/KS drift rules (Q19) trigger retraining (Q20) | ✅ |
| **Model validation**: "You evaluate and validate the model before it's promoted to production." | Q16 gate | ✅ |
| Producing evaluation metrics on a test dataset | PR-AUC and fraud-dollar recall at the Alert budget, on a time-split holdout | ✅ |
| Comparing against the current model: "You make sure that the new model produces better performance than the current model" | Challenger must beat the champion by ≥1 pt, with no PR-AUC loss (Q16) | ✅ |
| "Making sure that the performance of the model is consistent on various segments of the data." | Not decided | ❌ **Gap.** Add per-segment metrics (merchant category, amount band, night vs day) and a "no segment regresses by more than X" rule. |
| "test your model for deployment, including infrastructure compatibility and consistency with the prediction service API." | ONNX export plus contract tests in staging (Q34) | 🟡 Add a check that the registered model loads inside the *serving image* and answers the API schema. |
| Online validation: "in a canary deployment or an A/B testing setup" | Shadow then 10% canary (Q16) | 🟡 Canary is "if time allows". Shadow alone does not apply live decisions. |
| **Feature store** (optional): "a centralized repository where you standardize the definition, storage, and access of features for training and serving." | ADR-0002: one pure state function shared by training replay and serving. Postgres snapshots | ⛔ Deliberate. The README explains when Feast or Vertex Feature Store would be adopted (Q35). The goal it serves, avoiding training-serving skew, is met structurally by the parity test. |
| **Metadata management**: "Information about each execution of the ML pipeline is recorded in order to help with data and artifacts lineage, reproducibility, and comparisons." | MLflow runs plus the Airflow metadata DB | see rows below |
| "The pipeline and component versions that were executed." | git SHA logged (Q30) | 🟡 Also log each task's **image digest** and the DAG bundle version as MLflow tags. |
| "The start and end date, time, and how long the pipeline took" | Airflow task instances | ✅ |
| "The executor of the pipeline." | Airflow run_id, run type and triggering source (drift vs schedule) | 🟡 Record the trigger reason (which drift alert) as a DAG run conf and as an MLflow tag. |
| "The parameter arguments that were passed to the pipeline." | MLflow params + config (Q30) | ✅ |
| "The pointers to the artifacts produced by each step… resume the pipeline from the most recent step" | Data snapshot paths and hashes (Q30), MLflow artifacts. Airflow retry and clear-from-task | ✅ |
| "A pointer to the previous trained model if you need to roll back" | MLflow registry versions and aliases | ✅ |
| "The model evaluation metrics… for both the training and the testing sets." | Not explicit | 🟡 Log train and test metrics both. |
| **Triggers**: On demand | Manual Airflow trigger / REST | ✅ |
| On a schedule | "every 30 sim days" (Q20) | 🟡 The schedule is in *simulated* time, so it is an event-driven trigger rather than an Airflow cron. Say so in the README. |
| On availability of new training data | Not decided | 🟡 Could use an Airflow Asset on new matured-label snapshots. Optional. |
| On model performance degradation | Matured-label performance check (Q19) | ✅ |
| On significant changes in the data distributions | PSI/KS feature and score drift (Q19/Q20) | ✅ |

**Level 2 components** ("This MLOps setup includes the following components")

| Component | Project | Status |
|---|---|---|
| Source control | gitlab.com (Q27) | ✅ |
| Test and build services | GitLab CI: ruff/mypy, tests, SonarCloud, UBI image build, Trivy, model gate, prompt-eval gate (Q27) | ✅ |
| Deployment services | Argo CD + Helm, staging and prod namespaces (Q25, Q34) | ✅ |
| Model registry | MLflow registry with aliases (Q16). Prompts in the MLflow prompt registry (Q28) | ✅ |
| Feature store | none (ADR-0002) | ⛔ deliberate |
| ML metadata store | MLflow tracking (on Postgres) + Airflow metadata DB | ✅ (two stores. The README should say which question each one answers.) |
| ML pipeline orchestrator | Airflow 3 | ✅ |

**The six stages** (from the article's "Characteristics" list)

| Stage: quoted output | Project | Status |
|---|---|---|
| 1. Development and experimentation: "The output of this stage is the source code of the ML pipeline steps that are then pushed to a source repository." | Notebooks on the task image + `features/` package + DS guide (Q30, Q33), pushed to GitLab | ✅ |
| 2. Pipeline continuous integration: "The outputs of this stage are pipeline components (packages, executables, and artifacts) to be deployed in a later stage." | GitLab CI builds versioned task images and runs the tests below (Q27, Q33) | ✅ |
| 3. Pipeline continuous delivery: "The output of this stage is a deployed pipeline with the new implementation of the model." | CI bumps image tags in `deploy/` and Argo CD syncs. DAGs ship with their images (Q33) | 🟡 With one shared Airflow, "deployed to staging" vs "deployed to prod" needs the per-env DAG bundles (Q4). |
| 4. Automated triggering: "The output of this stage is a trained model that is pushed to the model registry." | Drift or schedule triggers the DAG, which registers a challenger in MLflow (Q20) | ✅ |
| 5. Model continuous delivery: "The output of this stage is a deployed model prediction service." | Shadow → canary → alias move. Scoring service picks up `champion` (Q16) | ✅ |
| 6. Monitoring: "The output of this stage is a trigger to execute the pipeline or to execute a new experiment cycle." | Monitoring job: drift and performance alerts in Postgres trigger the DAG (Q19, Q20) | ✅ |
| (Note) "The data analysis step is still a manual process… The model analysis step is also a manual process." | `reports/results.md`, MLflow comparisons, notebooks | ✅ (say this is intentional) |

**CI test list** ([#continuous_integration](https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning#continuous_integration))

| Article test (quoted) | Project | Status |
|---|---|---|
| "Unit testing your feature engineering logic." | Feature unit tests + train/serve parity test (Q21) | ✅ |
| "Unit testing the different methods implemented in your model." | Decision-policy, PSI/KS and encoding tests (Q21) | ✅ |
| "Testing that your model training converges" (loss goes down, overfits a few records) | Convergence test (Q33) | ✅ |
| "Testing that your model training doesn't produce NaN values" | Not decided | ❌ **Gap, cheap.** Assert there are no NaN/inf in features, predictions or the LightGBM eval history on the fixture sample. |
| "Testing that each component in the pipeline produces the expected artifacts." | Component tests (Q33) | ✅ |
| "Testing integration between pipeline components." | Integration tests (Q33). `make demo` as the local integration test (Q21) | ✅ |

**CD list** ([#continuous_delivery](https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning#continuous_delivery))

| Article item (quoted) | Project | Status |
|---|---|---|
| "Verifying the compatibility of the model with the target infrastructure… required memory, compute, and accelerator resources are available." | Not decided | 🟡 Add a staging smoke job: load the model in the serving image, check resource requests fit, and for vLLM check `nvidia.com/gpu` is allocatable. |
| "Testing the prediction service by calling the service API with the expected inputs" | API contract tests in staging (Q34) | ✅ |
| "Testing prediction service performance… (QPS) and model latency." | k6 load test, p99 vs budget (Q34). ONNX benchmark (Q6) | ✅ |
| "Validating the data either for retraining or batch prediction." | Retraining: pandera (Q35) | 🟡 Run pandera before **nightly batch scoring** as well. |
| "Verifying that models meet the predictive performance targets before they are deployed." | Model quality gate in CI (PR-AUC floor) + Q16 promotion rule | ✅ |
| "Automated deployment to a test environment, for example, a deployment that is triggered by pushing code to the development branch." | No separate test env. Feature-branch CI runs component and integration tests without deploying | 🟡 State the mapping: "test environment" = CI job containers. Optionally a short-lived `test` namespace, but there is no RAM for it. |
| "Semi-automated deployment to a pre-production environment… merging code to the main branch after reviewers approve" | Merge to `main` auto-syncs `staging` (Q34) | ✅ |
| "Manual deployment to a production environment after several successful runs of the pipeline on the pre-production environment." | git tag → prod (Q34) | 🟡 Make "several successful staging runs" explicit: the tag job checks the last N staging DAG runs succeeded. |

**Gap summary, to add to PLAN:**

1. Segment-level model validation.
2. NaN/inf test.
3. Infrastructure-compatibility smoke test.
4. pandera before batch prediction.
5. Image digest, DAG-bundle version and trigger reason logged to MLflow.
6. Train and test metrics both logged.
7. "N green staging runs" precondition on the prod tag.
8. Per-env DAG bundles.
9. Optional: new-data trigger via Airflow Assets.

Feature store stays a documented ⛔.

#### Q4. Phased bring-up and keeping the Compose → kind move cheap

**Phase 1 on Compose (planning ≈ 5–6 GiB steady, est.).**

- Postgres, one container with the `mlflow`, `airflow` and `fraud` DBs.
- Kafka in single-node KRaft, using the official `apache/kafka` image ([Docker Hub](https://hub.docker.com/r/apache/kafka)).
- The object store.
- MLflow with `--workers 1`.
- Airflow as **one container running `airflow standalone`**. "The ``airflow standalone`` command initializes the database, creates a user, and starts all components" ([start.rst 3.3.2](https://github.com/apache/airflow/blob/3.3.2/airflow-core/docs/start.rst)). Point it at the Compose Postgres with `LocalExecutor`.
- Replayer, scoring and monitoring.
- Training as `docker compose run trainer …`. vLLM is not part of Phase 1.

**Phase 2 on kind.** Stop the Compose stack first, so the two never run at the same time.

1. Raise inotify. Create the registry and the kind cluster (config in Q2). Install Argo CD, core or trimmed: about +2.2 GiB.
2. `platform` namespace: Postgres, object store, MLflow (Helm, Argo CD Applications). Re-run Phase 1 training against them.
3. Strimzi operator + one Kafka (`KafkaNodePool` dual-role, `replicas: 1`, 10Gi, limits set) + `KafkaTopic` CRs for both envs.
4. App Helm chart. `staging` auto-syncs from `main`, `prod` syncs from tags. Then k6 and contract tests.
5. Airflow chart with `executor: LocalExecutor`, `postgresql.enabled: false`, `statsd.enabled: false`, and KubernetesPodOperator tasks. Two Git DAG bundles: `staging` with `tracking_ref: main` and `prod` with `tracking_ref: <latest tag>`. A bundle is configured by `name`, `classpath: airflow.providers.git.bundles.git.GitDagBundle`, and kwargs that include `tracking_ref` ("Branch or tag for this DAG bundle") ([config.yml](https://github.com/apache/airflow/blob/3.3.2/airflow-core/src/airflow/config_templates/config.yml), [GitDagBundle](https://github.com/apache/airflow/blob/3.3.2/providers/git/src/airflow/providers/git/bundles/git.py)). Inject the env into each DAG through bundle-specific Variables or by the DAG id prefix.
6. Spark tasks, as KubernetesPodOperator pods with `local[4]` and a 2.5Gi limit, in an Airflow pool of size 1.
7. GPU last: NVIDIA Container Toolkit → nvkind (or the minikube fallback) → vLLM at `replicas: 0` by default → case-summary service.
8. Grafana, if time allows.

Each step leaves a demoable system, which matches "Phase 2 can stop at any point".

**Abstractions that keep the move cheap.**

- **12-factor config.** Each service reads everything from env vars through one settings class (e.g. pydantic-settings): `KAFKA_BOOTSTRAP`, `TOPIC_PREFIX`, `DATABASE_URL`, `MLFLOW_TRACKING_URI`, `S3_ENDPOINT_URL`, `MODEL_ALIAS`, `LLM_BASE_URL`. Compose gets these from `.env`; Helm renders them into a ConfigMap and Secret from values. No code branches on "am I in k8s".
- **Same images, immutable tags.** Compose references the same `localhost:5001/fraud/<svc>:<git-sha>` images that Helm deploys. Never use `:latest`, which also avoids kind's `Always` pull-policy trap.
- **DNS parity.** Give Compose services `networks.aliases` equal to the Kubernetes Service names, e.g. Strimzi's `<cluster>-kafka-bootstrap`, `mlflow`, `postgres`. Cross-namespace FQDNs (`postgres.platform.svc.cluster.local`) only ever live in Helm values.
- **Task logic as CLI entrypoints in the image** (`fraud validate|build-features|train|evaluate|score-batch`). The Airflow DAG is a thin factory that picks `KubernetesPodOperator` on kind or `DockerOperator` / `BashOperator` on Compose from one env flag. The images and the code inside them do not change.
- **Helm values per env.** One chart per service (or one umbrella `fraud-apps` chart) with `values.yaml`, `values-staging.yaml` and `values-prod.yaml`. They differ only in image tag, topic prefix, DB name, MLflow alias, replicas and resources. The same chart plus a `values-gke.yaml` is the GKE story (ADR-0003).
- **Mirror resource limits.** Compose `deploy.resources.limits.memory` should match the Helm limits ([Compose deploy spec](https://docs.docker.com/reference/compose-file/deploy/#resources)), so OOMs show up in Phase 1 rather than on kind.
- **Identical health and readiness endpoints** (`/healthz`, `/readyz`) for Compose `healthcheck` and k8s probes. Services retry on connect instead of relying on Compose `depends_on` ordering, because Kubernetes has none.
- **Object storage through the S3 API** (boto3 / s3fs, with an `S3_ENDPOINT_URL` override), so the backing store can change (see Risks) and the GCS target becomes a config swap: GCS supports S3-interoperable access, or use an adapter.

### Recommendation

- **Use the slim, shared topology.** One kind node with one each of Strimzi/Kafka, Postgres (DBs per env), MLflow (`--workers 1`), object store, Airflow (LocalExecutor + KubernetesPodOperator, no embedded Postgres, no statsd) and vLLM. Use Argo CD core (or full with dex and notifications off) and Grafana only, without kube-prometheus-stack. Duplicate only the app services in `staging` and `prod`.
- **Budget**: about 10.3 GiB steady and 12.8 GiB at peak with vLLM scaled to 0, and about 16.3 / 18.8 GiB with vLLM on. **Keep vLLM at `replicas: 0` by default** and scale it up (and staging down) for the LLM demo. Put a 1-slot Airflow pool on Spark and training pods. Set requests **and** limits on every pod, Kafka especially (`-Xmx512m` inside a 1.25Gi limit).
- **kind v0.33.0** with the digest-pinned `kindest/node:v1.37.0`, single node, NodePorts on 127.0.0.1 through `extraPortMappings`, and a **local registry** (`localhost:5001`) that CI pushes to and Argo CD-managed pods pull from. Raise `fs.inotify.max_user_watches` to 524288 before creating the cluster.
- **Phase 1 on Compose** (about 5–6 GiB) with the same images, env-var config and service names. Phase 2 follows the 8-step order in Q4, stopping Compose first.
- **Level 2 claim**: honest once the 9 gap items in Q3 are added. The most visible are segment-level model validation, the NaN test, the infra-compatibility check, per-env DAG bundles and the "N green staging runs before the prod tag" rule. The feature store stays a documented, deliberate omission.
- **Replace MinIO**, because the community edition is archived and its images are gone. Pick SeaweedFS (Apache-2.0, release 4.48 on 2026-09-28, [repo](https://github.com/seaweedfs/seaweedfs)) behind the S3 API, or `fake-gcs-server` if GCS-API fidelity matters more than MLflow/Spark S3 support. Record this as an ADR amendment to Q30. (Detailed comparison belongs to the MLflow/storage research domain.)

### Risks and gotchas

- **Contradicts Q30 / ADR wording: MinIO.** `gh api repos/minio/minio` returns `archived: true`, last push 2026-04-24. The README says "THIS REPOSITORY IS NO LONGER MAINTAINED" and "The MinIO community edition is now distributed as source code only" ([README](https://github.com/minio/minio/blob/master/README.md)). The last GitHub release is RELEASE.2025-10-15. "Maintenance Mode" was raised 2025-12-04 ([issue #21714](https://github.com/minio/minio/issues/21714)). `docker manifest inspect minio/minio:RELEASE.2025-09-07T16-13-09Z` returned **denied** on 2026-10-04, so the image is no longer pullable from Docker Hub.
- **Contradicts Q34 if read as "a full stack per namespace".** Duplicating Kafka, Postgres, MLflow and Airflow per env is about 31–35 GiB. Only the app layer can be per-env.
- **ADR-0004 and the budget.** An always-on vLLM (about 6 GiB host RAM, vLLM's own example request) plus the full platform exceeds about 17 GiB at peak. Scale-to-zero is needed.
- **GPU in kind is non-trivial.** It needs nvkind ("not very straightforward", per NVIDIA) and the NVIDIA Container Toolkit, which is **not installed** here (Docker runtimes are `runc` only). Fallbacks: minikube `--gpus all` ([docs](https://minikube.sigs.k8s.io/docs/tutorials/nvidia/)), or run vLLM as a host `docker run --gpus all` and expose it to the cluster through a selector-less Service plus Endpoints. The second weakens the "GPU scheduling on Kubernetes" evidence from ADR-0004.
- **Bitnami catalog change.** From 2025-08-28 versioned Bitnami images moved to `docker.io/bitnamilegacy` with no further updates ([bitnami/containers#83267](https://github.com/bitnami/containers/issues/83267)). The Airflow chart 1.22.0's embedded Postgres still uses `bitnamilegacy/postgresql:16.1.0-debian-11-r15`. Avoid Bitnami charts for Postgres, MLflow and MinIO. Use the official `postgres` image, or CloudNativePG, and our own small MLflow chart.
- **Airflow chart version lag.** Chart 1.22.0 ships `appVersion: 3.2.2` while Airflow is at 3.3.2. Pin `airflowVersion` and `defaultAirflowTag` explicitly. The chart default `executor: CeleryExecutor` adds about 2 GiB (worker + Redis). Override it.
- **Strimzi 1.x.** It uses the `kafka.strimzi.io/v1` API only (v1beta2 was removed in 1.0.0), so older blog snippets fail. Since 1.0.1, Entity Operator cross-namespace watching is **disabled by default** ([CHANGELOG](https://github.com/strimzi/strimzi-kafka-operator/blob/1.2.0/CHANGELOG.md)). Keep every `KafkaTopic` CR, staging and prod, in the Kafka namespace. The single-node example asks for a 100Gi PVC. Without a memory limit the broker heap is unbounded.
- **Argo CD sets no requests.** Without explicit values its pods are BestEffort and get evicted first under memory pressure, which is exactly when you need GitOps to work.
- **`kind load` + GitOps race.** A tag bump synced before the image is loaded gives `ErrImagePull`. Use the registry.
- **Shared Airflow and Level 2's "pipeline CD per environment".** Without DAG bundles pinned to different refs, staging and prod run the same DAG code. That undermines experimental-operational symmetry and the test → pre-prod → prod promotion of *pipelines*, not just models.
- **Image disk usage.** vLLM is 8.7 GB compressed and Airflow 3.3.2 is 0.66 GB. Each image is stored in Docker, the registry and node containerd. kubelet ephemeral-storage eviction is a documented kind failure mode.
- **Estimates are estimates.** Validate them with `kubectl top pods -A` (install metrics-server; kind does not ship it) after step 4 of the bring-up, and update this budget.
- **Volatile facts recorded here:** kind v0.33.0 (2026-08-26), Strimzi 1.2.0 (2026-08-20), Argo CD 3.5.3 (2026-09-14, 3.6.0-rc1 out), argo-helm 10.9.6, Airflow 3.3.2 (2026-09-17), chart 1.22.0 (2026-06-13), MLflow 3.16.1 (2026-09-17), vLLM 0.30.0 (2026-09-22), kube-prometheus-stack 91.9.0 (2026-10-02), k3d 5.9.0, minikube 1.39.0, Google article last reviewed 2024-08-28.

### Sources

- Google Cloud, *MLOps: Continuous delivery and automation pipelines in machine learning* (last reviewed 2024-08-28, CC BY 4.0): https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning
- kind v0.33.0 release: https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0
- kind docs: quick start https://kind.sigs.k8s.io/docs/user/quick-start/ · configuration https://kind.sigs.k8s.io/docs/user/configuration/ · local registry https://kind.sigs.k8s.io/docs/user/local-registry/ · registry script https://github.com/kubernetes-sigs/kind/blob/v0.33.0/site/static/examples/kind-with-registry.sh · ingress https://kind.sigs.k8s.io/docs/user/ingress/ · known issues https://kind.sigs.k8s.io/docs/user/known-issues/
- nvkind: https://github.com/NVIDIA/nvkind
- kubeadm requirements: https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/install-kubeadm/
- k3s requirements and profiling: https://docs.k3s.io/installation/requirements · https://docs.k3s.io/reference/resource-profiling · k3d CUDA: https://github.com/k3d-io/k3d/blob/v5.9.0/docs/usage/advanced/cuda.md
- minikube: https://minikube.sigs.k8s.io/docs/start/ · https://minikube.sigs.k8s.io/docs/tutorials/nvidia/
- Strimzi 1.2.0: operator deployment https://github.com/strimzi/strimzi-kafka-operator/blob/1.2.0/install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml · Helm values https://github.com/strimzi/strimzi-kafka-operator/blob/1.2.0/helm-charts/helm3/strimzi-kafka-operator/values.yaml · single-node example https://github.com/strimzi/strimzi-kafka-operator/blob/1.2.0/examples/kafka/kafka-single-node.yaml · JVM heap docs https://github.com/strimzi/strimzi-kafka-operator/blob/1.2.0/documentation/modules/con-common-configuration-properties.adoc · CHANGELOG https://github.com/strimzi/strimzi-kafka-operator/blob/1.2.0/CHANGELOG.md
- Airflow: chart values 1.22.0 https://github.com/apache/airflow/blob/helm-chart/1.22.0/chart/values.yaml · production guide https://airflow.apache.org/docs/helm-chart/stable/production-guide.html · config.yml 3.3.2 https://github.com/apache/airflow/blob/3.3.2/airflow-core/src/airflow/config_templates/config.yml · docker-compose howto https://github.com/apache/airflow/blob/3.3.2/airflow-core/docs/howto/docker-compose/index.rst · quick start (standalone) https://github.com/apache/airflow/blob/3.3.2/airflow-core/docs/start.rst · GitDagBundle https://github.com/apache/airflow/blob/3.3.2/providers/git/src/airflow/providers/git/bundles/git.py
- Argo CD v3.5.3 manifests https://github.com/argoproj/argo-cd/tree/v3.5.3/manifests · Argo CD Core https://github.com/argoproj/argo-cd/blob/v3.5.3/docs/operator-manual/core.md · argo-helm chart https://github.com/argoproj/argo-helm/blob/main/charts/argo-cd/Chart.yaml
- MLflow v3.16.1 server: https://github.com/mlflow/mlflow/blob/v3.16.1/mlflow/server/__init__.py · CLI https://github.com/mlflow/mlflow/blob/v3.16.1/mlflow/cli/__init__.py
- vLLM v0.30.0 k8s guide: https://github.com/vllm-project/vllm/blob/v0.30.0/docs/deployment/k8s.md · Docker guide https://github.com/vllm-project/vllm/blob/v0.30.0/docs/deployment/docker.md · image tags https://hub.docker.com/r/vllm/vllm-openai/tags
- Spark configuration: https://github.com/apache/spark/blob/master/docs/configuration.md (https://spark.apache.org/docs/latest/configuration.html)
- PostgreSQL resource config: https://www.postgresql.org/docs/current/runtime-config-resource.html
- kube-prometheus-stack 91.9.0 values: https://github.com/prometheus-community/helm-charts/blob/kube-prometheus-stack-91.9.0/charts/kube-prometheus-stack/values.yaml · Grafana chart values: https://github.com/grafana-community/helm-charts/blob/main/charts/grafana/values.yaml
- MinIO: https://github.com/minio/minio (archived) · README https://github.com/minio/minio/blob/master/README.md · issue #21714 https://github.com/minio/minio/issues/21714 · SeaweedFS https://github.com/seaweedfs/seaweedfs · fake-gcs-server https://github.com/fsouza/fake-gcs-server
- Bitnami catalog changes: https://github.com/bitnami/containers/issues/83267 · https://github.com/bitnami/charts/blob/main/README.md
- Docker Compose deploy resources: https://docs.docker.com/reference/compose-file/deploy/#resources · Apache Kafka image: https://hub.docker.com/r/apache/kafka
