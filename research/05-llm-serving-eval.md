## 05. LLM serving and evaluation

Researched 2026-10-04. Every version and date below was checked on that day against the source given. Snapshot of the host it was checked from: RTX 3090, driver 580.173.02, compute capability 8.6, 24,576 MiB, of which about 1.9 GB is already used by the desktop and other apps. Docker default runtime is `runc`. **`nvidia-ctk` and `kind` are not installed yet** (`nvidia-smi`, `docker info`, `which` on this machine).

### Questions

1. **GPU inside kind.** How do you expose the NVIDIA GPU to a kind node (container toolkit config, `nvkind`, extraMounts, device plugin or GPU Operator)? What are the current versions, the exact steps and the known breakage? If it is too fragile, what is the fallback, and what does the fallback cost the story?
2. **vLLM.** Current version and image. Server flags for a 24 GB card. Which quantisation formats run on Ampere sm_86? The Prometheus metrics. Structured output. Status of the Helm chart and production-stack.
3. **Model choice.** Which current 7–9B open-weight instruct model fits English and French summaries with structured fields in 24 GB? Compare licence, French quality, quantised checkpoints and context length.
4. **Eval and CI gating.** Which approaches and tools (MLflow 3 GenAI evaluate, promptfoo, DeepEval, Ragas) fit best with the MLflow prompt registry decision? What can be reused from PromptGuard?
5. **LLMOps observability.** Tracing, token and latency metrics, and a self-hosted cost model for the README.

### Findings

#### Q1. GPU inside kind

**Status, 2026-10-04.** kind itself has no GPU support. The latest kind is v0.33.0 (2026-08-26), whose default node image is `kindest/node:v1.37.0`. NVIDIA's answer is `nvkind`, a wrapper around `kind create cluster`.

The nvkind README states the problem directly:

> "Unfortunately, running `kind` with access to GPUs is not very straightforward. There is no standard way to inject GPUs support into a `kind` worker node" — https://github.com/NVIDIA/nvkind

Component versions:

| Component | Version / date | Source |
|---|---|---|
| kind | v0.33.0, 2026-08-26 (default node `kindest/node:v1.37.0@sha256:a1ed56cf…`) | https://github.com/kubernetes-sigs/kind/releases |
| nvidia-container-toolkit | v1.20.1, 2026-09-19 | https://github.com/NVIDIA/nvidia-container-toolkit/releases |
| k8s-device-plugin | v0.20.1, 2026-09-22 | https://github.com/NVIDIA/k8s-device-plugin/releases |
| GPU Operator | v26.7.1, 2026-09-23 | https://github.com/NVIDIA/gpu-operator/releases |
| nvkind | **No tags or releases.** Latest main commit is `c5705049` (2026-06-30). The node post-processing was last changed in `9f700160` (2026-06-09). | https://github.com/NVIDIA/nvkind/commits/main |

**How nvkind works.** It has two parts, both read from source:

1. **The kind config template.** Each GPU worker gets an extraMount of `/dev/null` at `/var/run/nvidia-container-devices/<gpu>`, plus the label `nvidia.com/gpu.present: "true"` (`examples/one-worker-per-gpu.yaml`, `pkg/nvkind/default-config-template.yaml`). The host toolkit, with `accept-nvidia-visible-devices-as-volume-mounts=true` set, reads that mount as a request to inject the GPU into the node container.
2. **Post-processing on each node**, in `pkg/nvkind/node.go`:
   - Install `nvidia-container-toolkit` inside the node with `apt-get`, which needs internet access.
   - Run `nvidia-ctk runtime configure --runtime=containerd --config-source=file` and restart containerd.
   - Mask `/proc/driver/nvidia/params` and remove device nodes the worker should not see.

**Exact steps (host, one-off).** All of these need sudo, so the user runs them.

```bash
# 1. Install the NVIDIA Container Toolkit 1.20.1 (from NVIDIA's install guide)
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
  && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
  | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
sudo apt-get update
export NVIDIA_CONTAINER_TOOLKIT_VERSION=1.20.1-1
sudo apt-get install -y nvidia-container-toolkit=${NVIDIA_CONTAINER_TOOLKIT_VERSION} \
  nvidia-container-toolkit-base=${NVIDIA_CONTAINER_TOOLKIT_VERSION} \
  libnvidia-container-tools=${NVIDIA_CONTAINER_TOOLKIT_VERSION} libnvidia-container1=${NVIDIA_CONTAINER_TOOLKIT_VERSION}

# 2. Configure the toolkit for kind (from the nvkind README "Setup")
sudo nvidia-ctk runtime configure --runtime=docker --set-as-default --cdi.enabled
sudo nvidia-ctk config --set accept-nvidia-visible-devices-as-volume-mounts=true --in-place
sudo systemctl restart docker

# 3. Smoke tests (nvkind README)
docker run --runtime=nvidia -e NVIDIA_VISIBLE_DEVICES=all ubuntu:20.04 nvidia-smi -L
docker run -v /dev/null:/var/run/nvidia-container-devices/all ubuntu:20.04 nvidia-smi -L   # must list the 3090

# 4. Install nvkind, pinned to a commit because there are no releases
go install github.com/NVIDIA/nvkind/cmd/nvkind@c5705049
#   or, without Go on the host:
docker run --rm -v $PWD/bin/:/go/bin/ golang:1.23 go install github.com/NVIDIA/nvkind/cmd/nvkind@c5705049
```

Sources: https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html and https://github.com/NVIDIA/nvkind#setup

**The project's cluster.** The `--config-template` file is a normal kind config written as a Go template, so the project's own settings can go in it (port mappings for ingress, node image pin, a worker for the GPU):

```yaml
# deploy/kind/nvkind-cluster.yaml.tmpl  (use: nvkind cluster create --name fraud --config-template=deploy/kind/nvkind-cluster.yaml.tmpl)
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  image: kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
  extraPortMappings:
  - {containerPort: 80, hostPort: 80}
  - {containerPort: 443, hostPort: 443}
- role: worker                       # CPU worker: Kafka, Airflow, MLflow, ...
  image: kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
- role: worker                       # GPU worker: vLLM only
  image: kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
  labels:
    nvidia.com/gpu.present: "true"
  extraMounts:
    # requires accept-nvidia-visible-devices-as-volume-mounts = true (nvkind template comment)
    - hostPath: /dev/null
      containerPath: /var/run/nvidia-container-devices/0
```

**Device plugin vs GPU Operator.** Use the device plugin. It is one DaemonSet. The GPU Operator would need `driver.enabled=false`, `toolkit.enabled=false`, `cdi.enabled=true`, `operator.runtimeClass=nvidia` and a privileged namespace label in kind (nvkind README, "Install GPU Operator"). That is a lot of moving parts for one consumer card. The device plugin is still a Helm release, so Argo CD can manage it.

```bash
helm repo add nvdp https://nvidia.github.io/k8s-device-plugin && helm repo update
helm upgrade -i --kube-context=kind-fraud --namespace nvidia --create-namespace \
  nvidia-device-plugin nvdp/nvidia-device-plugin --version 0.20.1
kubectl get nodes -o json | jq -r '.items[] | {name: .metadata.name, gpu: .status.allocatable["nvidia.com/gpu"]}'
```

Sources: nvkind README "Install the k8s-device-plugin"; chart version from https://github.com/NVIDIA/k8s-device-plugin/releases

**Known breakage, from open and closed nvkind issues.**

- **#61, nvkind fails with toolkit ≥ 1.18 (`umount: /proc/driver/nvidia: not mounted`).** Users downgraded to 1.17.1 to work around it. It is fixed on main by `9a3061c7` (2026-04-20, "align with nvidia-container-toolkit v1.19"). The code now checks for `ModifyDeviceFiles: 0` and skips the umount. **Build nvkind from a commit after 2026-04-20; older binaries break with toolkit 1.20.1.** https://github.com/NVIDIA/nvkind/issues/61
- **#88, opened 2026-09-25 (toolkit 1.20.1).** The device plugin fails with `Failed to initialize NVML: ERROR_LIBRARY_NOT_FOUND`. In that report the cluster was created with plain `kind` from an nvkind-style config, so the in-node toolkit install and containerd configuration never ran. **Always create the cluster with `nvkind cluster create`, never `kind create cluster`.** https://github.com/NVIDIA/nvkind/issues/88
- **#85, device-plugin DaemonSet `DESIRED 0`.** The DaemonSet scheduled no pods because node labels or affinity did not match. Check that the GPU worker carries `nvidia.com/gpu.present=true`. https://github.com/NVIDIA/nvkind/issues/85
- **#74.** `--runtime=nvidia` did not work for one user, but `--gpus=all` did. https://github.com/NVIDIA/nvkind/issues/74
- **#20.** Device-plugin pods were not scheduled to workers (open since 2024-12). https://github.com/NVIDIA/nvkind/issues/20
- **Side effect.** `--set-as-default` makes `nvidia` the default Docker runtime for every container on the host, not only kind nodes. The post-install step needs internet access from inside the node (`apt-get` from `nvidia.github.io`).
- **Rebuilds.** Recreating the cluster re-runs the in-node `apt-get install`, which adds minutes to every `make cluster` and needs network access.

**Fallback: vLLM as a plain Docker container outside kind.** The official command:

```bash
docker run --runtime nvidia --gpus all \
    -v ~/.cache/huggingface:/root/.cache/huggingface \
    --env "HF_TOKEN=$HF_TOKEN" -p 8000:8000 --ipc=host \
    vllm/vllm-openai:latest --model Qwen/Qwen3-0.6B
```

(https://docs.vllm.ai/en/latest/deployment/docker/)

To make it reachable from pods, attach the container to kind's Docker network (`docker run --network kind --name vllm …`). kind's node entrypoint rewrites Docker's embedded-DNS rules so pods can use Docker DNS. Quoted from source: "we need to also apply these rules to non-local traffic (from pods)" (https://github.com/kubernetes-sigs/kind/blob/v0.33.0/images/base/files/usr/local/bin/entrypoint, `fix_network`). So an ExternalName Service pointing at the container name should resolve. This is inferred from source, not tested here. The robust alternative is a selector-less Service plus an EndpointSlice with the container's fixed IP (`docker run --network kind --ip 172.18.0.250`).

```yaml
apiVersion: v1
kind: Service
metadata: {name: vllm, namespace: llm}
spec:
  type: ExternalName
  externalName: vllm          # Docker container name on the "kind" network
  ports: [{port: 8000}]
```

What the fallback costs the story:

- vLLM is no longer deployed by Argo CD or Helm, so "GPU scheduling on Kubernetes" (one of the consequences listed in ADR-0004) is not shown on kind.
- The GKE path (ADR-0003) still uses the same Helm chart with a GPU node pool, but that chart would never have run anywhere. That is the "never applied" weakness again, this time for the LLM.
- What survives: the OpenAI-compatible boundary, the eval gate, prompt registry, tracing and metrics. Prometheus can still scrape the container through the Service.

**Recommendation for Q1.** Try nvkind first and timebox it to about 2 hours. If it is not working by then, switch to the Docker fallback. Keep the vLLM Helm chart in `deploy/` either way. On GKE, drivers and the device plugin are managed for you:

> "GKE automatically installs the default NVIDIA driver version for all GPU nodes" (1.32.2-gke.1297000+)

GPU nodes get the taint `nvidia.com/gpu=present:NoSchedule`, and pods select a GPU type with `cloud.google.com/gke-accelerator`. Source: https://docs.cloud.google.com/kubernetes-engine/docs/how-to/gpus. So the same chart only needs a values overlay (`nodeSelector`, `tolerations`).

#### Q2. vLLM, current version

**Version.** vLLM **v0.30.0**, released 2026-09-22 (PyPI `vllm 0.30.0`; https://github.com/vllm-project/vllm/releases/tag/v0.30.0). Images, quoted from the release notes:

> "CUDA 13.0 (Default) | `docker pull vllm/vllm-openai:v0.30.0`"
> "CUDA 12.9 | `docker pull vllm/vllm-openai:v0.30.0-cu129`"

The default Dockerfile builds `ARG CUDA_VERSION=13.0.3` with Python 3.12 (https://github.com/vllm-project/vllm/blob/v0.30.0/docker/Dockerfile). Host driver 580.173 belongs to the CUDA 13.0 branch, so the default image should run. If it fails with a CUDA/driver mismatch, use `-cu129`. Always pin the tag, never use `:latest`.

Breaking changes in v0.30.0 that matter here:

> "GPTQ activation ordering (`g_idx`) removed (#54809)"

This means **avoid GPTQ checkpoints made with `desc_act=True`**. AWQ, compressed-tensors W4A16 and FP8 are unaffected. https://github.com/vllm-project/vllm/releases/tag/v0.30.0

**Server flags for 24 GB.** Docstrings from `vllm/config/*.py` at tag v0.30.0:

- `--gpu-memory-utilization`: default **0.92** (not 0.9 as older guides say).

  > "The fraction of GPU memory to be used for the model executor … This is a per-instance limit"

  https://github.com/vllm-project/vllm/blob/v0.30.0/vllm/config/cache.py

  Startup fails if free memory is below the requested fraction:

  > "Free memory on device … is less than desired GPU memory utilization … Decrease GPU memory utilization or reduce GPU memory used by other processes."

  https://github.com/vllm-project/vllm/blob/v0.30.0/vllm/v1/worker/utils.py

  The desktop already uses about 1.9 GB of this card, so 0.92 × 24 GB = 22.1 GiB will not fit. **Use 0.85 or lower.**
- `--max-model-len`:

  > "Model context length (prompt and output). If unspecified, will be automatically derived from the model config."

  Candidate models default to 128K–262K, which wastes KV cache. Set 8192.
- `--kv-cache-dtype`:

  > "CUDA 11.8+ supports fp8 (=fp8_e4m3) and fp8_e5m2."

  This is optional. It doubles KV capacity, at a small quality risk.
- `--language-model-only`:

  > "If True, disables all multimodal inputs by setting all modality limits to 0."

  Useful because Ministral 3, Qwen3.5 and Gemma 4 all ship vision towers. https://github.com/vllm-project/vllm/blob/v0.30.0/vllm/config/multimodal.py
- `--default-chat-template-kwargs '{"enable_thinking": false}'`: turns off reasoning server-wide for models that think by default. https://github.com/vllm-project/vllm/blob/v0.30.0/docs/features/reasoning_outputs.md
- `--api-key`: the docs warn not to rely on it as real auth. https://github.com/vllm-project/vllm/blob/v0.30.0/docs/serving/online_serving/openai_compatible_server.md

**Quantisation on Ampere (sm_86).** Table from https://github.com/vllm-project/vllm/blob/main/docs/features/quantization/README.md, which notes "Ampere to SM 8.0/8.6":

| Implementation | Ampere |
|---|---|
| AWQ | ✅ |
| GPTQ | ✅ |
| Marlin (GPTQ/AWQ/FP8/FP4) | ✅ |
| llm-compressor INT8 (W8A8) | ✅ |
| llm-compressor FP8 (W8A8) | ❌ (Ada/Hopper only) |
| bitsandbytes, GGUF | ✅ |

FP8 checkpoints still load on Ampere as weight-only FP8 through Marlin. The warning text at v0.30.0:

> "Your GPU does not have native support for FP8 computation but FP8 quantization is being used. Weight-only FP8 compression will be used leveraging the Marlin kernel. This may degrade performance for compute-heavy workloads."

https://github.com/vllm-project/vllm/blob/v0.30.0/vllm/model_executor/layers/quantization/utils/marlin_utils_fp8.py

The result: an FP8 checkpoint halves weight memory on the 3090 but gets no FP8 compute speed-up. That is fine for low-QPS summaries.

**Prometheus metrics.** `/metrics` on the API server (https://docs.vllm.ai/en/latest/usage/metrics/; names from https://docs.vllm.ai/en/latest/design/metrics/). The ones to put on the Grafana board and the README:

- `vllm:num_requests_running`, `vllm:num_requests_waiting`
- `vllm:kv_cache_usage_perc` (0–1). It replaces the old `gpu_cache_usage_perc`.
- `vllm:prompt_tokens_total`, `vllm:generation_tokens_total` (counters, used for tokens/s and cost)
- `vllm:time_to_first_token_seconds`, `vllm:inter_token_latency_seconds`, `vllm:request_time_per_output_token_seconds`, `vllm:e2e_request_latency_seconds`, `vllm:request_queue_time_seconds` (histograms)
- `vllm:request_prompt_tokens`, `vllm:request_generation_tokens`, `vllm:request_success_total{finished_reason}`
- v0.30.0 also adds the `vllm:request_num_preemptions` histogram (release notes, #49984).
- Deprecation policy, quoted:

  > "when metrics are deprecated in version `X.Y`, they are hidden in version `X.Y+1` … and are then removed in version `X.Y+2`."

  Pin dashboards to the vLLM version.

**Structured output.** Per https://docs.vllm.ai/en/latest/features/structured_outputs/, the backends are xgrammar and guidance (`--structured-outputs-config.backend`, default `auto`). The standard OpenAI `response_format={"type":"json_schema",…}` works, and so does the vLLM-specific `extra_body={"structured_outputs": {"json": schema}}`. **`guided_json` / `guided_*` were removed in v0.12.0.** Older blog posts using them will fail. Structured output combined with reasoning needs a reasoning parser, and in some cases `--structured-outputs-config.enable_in_reasoning=True` (same doc). Disabling thinking avoids the issue.

**Helm chart and production-stack.**

- The vLLM docs list a basic Helm chart under `examples/deployment/chart-helm`. Its prerequisite is the "NVIDIA Kubernetes Device Plugin". It is oriented to S3 model download. https://github.com/vllm-project/vllm/blob/v0.30.0/docs/deployment/frameworks/helm.md
- **vLLM production-stack**: Helm chart `vllm-stack-0.1.13`, 2026-09-29, repo `https://vllm-project.github.io/production-stack`. It bundles a router, LMCache KV offload and a Prometheus/Grafana stack (https://github.com/vllm-project/production-stack). It is active (pushed 2026-10-02) but still 0.1.x. For one model on one GPU the router adds nothing.
- The same docs page lists KServe, llm-d, KubeAI, AIBrix and LWS as alternatives (https://github.com/vllm-project/vllm/blob/v0.30.0/docs/deployment/k8s.md).
- **Recommendation: a small in-repo chart** (Deployment, Service, PVC for the HF cache, ServiceMonitor), modelled on the docs' native manifest. That manifest uses `nvidia.com/gpu: "1"`, an `emptyDir {medium: Memory}` mounted at `/dev/shm`, a `/health` probe, and a PVC at `/root/.cache/huggingface`. A small chart is easier to explain in the interview than a vendored stack. Mention production-stack and llm-d in build-vs-buy.

Container spec for the chart (values for the recommended model, Q3):

```yaml
containers:
- name: vllm
  image: vllm/vllm-openai:v0.30.0
  args:
  - --model=mistralai/Ministral-3-8B-Instruct-2512
  - --served-model-name=case-summary-llm
  - --tokenizer-mode=mistral
  - --config-format=mistral
  - --load-format=mistral
  - --language-model-only
  - --max-model-len=8192
  - --gpu-memory-utilization=0.85
  - --max-num-seqs=32
  - --port=8000
  ports: [{containerPort: 8000, name: http}]
  resources: {limits: {nvidia.com/gpu: "1", memory: 16Gi}, requests: {cpu: "2", memory: 12Gi}}
  volumeMounts:
  - {name: hf-cache, mountPath: /root/.cache/huggingface}
  - {name: shm, mountPath: /dev/shm}
  startupProbe: {httpGet: {path: /health, port: 8000}, failureThreshold: 60, periodSeconds: 10}
  readinessProbe: {httpGet: {path: /health, port: 8000}, periodSeconds: 10}
volumes:
- {name: shm, emptyDir: {medium: Memory, sizeLimit: 2Gi}}
- {name: hf-cache, persistentVolumeClaim: {claimName: hf-cache}}
```

#### Q3. Model choice (late 2026)

Hugging Face API, 2026-10-04. Licence, languages and context come from each model card.

| Model (HF id) | Released | Params | Licence | French | Context | Quantised checkpoints | Notes |
|---|---|---|---|---|---|---|---|
| `mistralai/Ministral-3-8B-Instruct-2512` | 2025-10-31 (card updated 2026-07-15) | 8.9B | Apache-2.0 | Listed first after English: "English, French, Spanish…"; Mistral is a French lab | 256k | **Shipped by the vendor in FP8** ("instruct post-trained version in **FP8**"); `-BF16`, GGUF; community AWQ (`cyankiwi/…-AWQ-4bit`) | Instruct model, no thinking by default. "fitting in 12GB of VRAM in FP8". Needs `--tokenizer_mode mistral --config_format mistral --load_format mistral`. Has a vision tower (FP8 config excludes it). https://huggingface.co/mistralai/Ministral-3-8B-Instruct-2512 |
| `Qwen/Qwen3.5-9B` | 2026-02-27 | 9.65B | Apache-2.0 | "201 languages and dialects" | 262,144 native | No official AWQ/FP8 for 9B. RedHatAI `Qwen3.5-9B-quantized.w4a16` and `Qwen3.5-9B-FP8-dynamic`; `QuantTrio/Qwen3.5-9B-AWQ` | "operate in thinking mode by default", so pass `enable_thinking: false`. Card advises ≥128K context "to preserve thinking capabilities" (not needed without thinking). Hybrid Gated-DeltaNet architecture. Multimodal (`--language-model-only`). BF16 is about 19.3 GB, too tight. https://huggingface.co/Qwen/Qwen3.5-9B |
| `google/gemma-4-12B-it` | 2026-05-23 | 12.0B | **Apache-2.0** (Gemma 4 dropped the custom Gemma licence) | "Out-of-the-box support for 35+ languages, pre-trained on 140+" | 256K | **Official QAT** `google/gemma-4-12B-it-qat-w4a16-ct` (compressed-tensors, runs via Marlin); `RedHatAI/gemma-4-12B-it-FP8-Dynamic` | Larger than the ADR's "7–8B", but W4A16 is about 7–8 GB. Thinking off by default in vLLM. Audio and vision. https://huggingface.co/google/gemma-4-12B-it |
| `ibm-granite/granite-4.2-8b` | 2026-08-07 | 8.8B | Apache-2.0 | French in "Tested Languages" | 128K (512K extension) | **Official** `-fp8`, `-nvfp4`, `-mxfp4`, GGUF | Reasoning model, "full thinking (default)". Recommends a custom reasoning-parser plugin file. https://huggingface.co/ibm-granite/granite-4.2-8b |
| `google/gemma-4-E4B-it` | 2026-03-02 | 8.0B (effective 4B) | Apache-2.0 | as Gemma 4 | 128K | official QAT w4a16 | Cheap fallback if VRAM is tight |
| `utter-project/EuroLLM-9B-Instruct-2512` | 2026-01-26 | 9.2B | Apache-2.0 | EU languages; built for European languages | n/a | GGUF only (community) | No vLLM-ready AWQ/FP8. Weaker on structured output and tool use. Skip. |
| `meta-llama/*` | Newest small Llama is still Llama 3.x (2024); Llama 4 is 17B×16E MoE only | — | Llama Community Licence | — | — | — | Out: old, gated, restrictive licence |

Qwen3.8, the newest Qwen (2026-08), only ships at 27B and above. That does not fit 24 GB comfortably with KV cache, so it is out.

**VRAM math for Ministral 3 8B.** From its `config.json`: 34 layers, 8 KV heads, head_dim 128. KV per token in BF16 = 2 × 34 × 8 × 128 × 2 B ≈ 136 KiB.

At `--gpu-memory-utilization 0.85` (about 20.4 GiB): about 10 GiB of FP8 weights plus about 1–2 GiB of activations and CUDA graphs leaves about 8 GiB of KV. That is roughly 60k tokens of cache: about 7 concurrent requests at the full 8k context, or dozens of 1–2k-token summary requests. This is arithmetic, not measured. Confirm against vLLM's startup log line for KV cache size.

**French quality.** None of these cards publish a French-specific benchmark that is comparable across models. Mistral's card shows a "Multilingual MMLU" column but only against its own family and Qwen. The project's own golden set (Q4) needs about 10 French cases and is the real arbiter. This is an explicit gap.

#### Q4. LLM eval and CI gating

**Approach.** Use three layers, ordered so the cheap, deterministic checks can block on their own:

1. **Schema and format checks (deterministic).**
   - The output parses into the Pydantic `CaseSummary` model. Structured output makes this nearly guaranteed, so a failure means a server or config regression.
   - Language matches the request (en/fr).
   - Length is within bounds.
   - The required fields are present (e.g. `risk_factors`, `recommended_action ∈ {block, review, allow}` from the glossary's Decision).
2. **Faithfulness to Transaction fields (deterministic, the key check).**
   - Every number in the text must appear in the input: amount (with tolerance for formatting such as `1 234,56 €` vs `1234.56`), hour, distance, and the counts from the risk features.
   - Merchant, category and city named in the text must be in the input.
   - The summary's stated Decision must equal the platform's Decision. No card number may appear (PII).

   This is the "no hallucinated amounts" rule. Regex and normalisation make it cheap and exact.
3. **LLM-as-judge (graded, softer).**
   - Helpfulness, clarity and coverage against a hand-written reference summary, on a 1–5 scale. This is PromptGuard's `judge.py` rubric with the support-email wording swapped for fraud cases.
   - Built-in MLflow `Summarization` ("Is the summary faithful, comprehensive, concise, and clear?") and `Fluency` judges are available as is.

**What to reuse from PromptGuard** (`~/projects/Model_Regression_Detection_System`):

- **Hand-written golden cases** with `notes` explaining what each catches. The README says:

  > "Write cases by hand, not with an LLM — the entire point of the golden dataset is that its labels are trustworthy ground truth"

  Add flagged-Transaction cases: easy, edge (foreign amount, zero-history card), adversarial (merchant name containing instructions), and French.
- **Severity thresholds as env vars.** `WARNING_DELTA=3.0` and `CRITICAL_DELTA=8.0` pass-rate points, with exit code 2 blocking the merge.
- **The "all cases errored ⇒ critical" override**, so a dead vLLM can never pass the gate.
- **Per-case regression list** in addition to the aggregate. PromptGuard's case study shows the aggregate gate missed a real regression that per-case tracking caught.
- **Rolling-window drift check** (`EVALSYS_DRIFT_WINDOW=7`, `EVALSYS_DRIFT_THRESHOLD=5.0`).
- **Static HTML report as a CI artifact.**
- **Change:** PromptGuard's `scorer.py` prices tokens with the OpenAI table, which gives $0 for a local model. Replace it with the GPU-hour cost model (Q5).

**Tooling comparison.** Versions from PyPI and GitHub, 2026-10-04:

| Tool | Version | Fit |
|---|---|---|
| **MLflow GenAI evaluate** | mlflow 3.16.1 (2026-09-16) | **Best fit.** Same server as the prompt registry. Eval runs, traces and prompt versions live together, and the MLflow docs page "Evaluating Prompts" covers exactly this pairing. `mlflow.genai.evaluate(data=…, predict_fn=…, scorers=[…])`. Custom `@scorer` functions return `bool`, a number or a `Feedback(value, rationale)`. `make_judge(name, instructions with {{ inputs }}/{{ outputs }}/{{ expectations }}, model="<provider>:/<model>", feedback_value_type=…)`. Built-in judges include `Correctness`, `Guidelines`, `ExpectationsGuidelines`, `Summarization`, `Fluency`, `Safety`, `Equivalence`. The gate logic (thresholds, exit code) is still yours: write it as a small script over `results.metrics`. |
| promptfoo | 0.123.1 (2026-09-18) | Good YAML assertions (`is-json`, `javascript`, `llm-rubric`). Points at vLLM with `id: openai:chat:<served-name>` and `apiBaseUrl: http://…/v1` (https://www.promptfoo.dev/docs/providers/vllm/). Node-based, with its own results store, so it duplicates MLflow and does not read the MLflow prompt registry natively. |
| DeepEval | 4.2.8 (2026-10-02) | pytest-style; `GEval`. `FaithfulnessMetric` requires `retrieval_context` (RAG-shaped), though the Transaction JSON could be passed as context. Custom judge via `DeepEvalBaseLLM`. A third results store. |
| Ragas | 0.4.3 (2026-01-13) | RAG-specific, and this project has no retrieval. Skip. |

**Judge model choice.**

- **Default:** MLflow uses `"openai:/gpt-4o-mini"` outside Databricks.
- **Supported URIs:** `openai:/`, `anthropic:/`, `mistral:/`, `ollama:/`, `gateway:/<endpoint>`, and any LiteLLM provider after `pip install litellm`. https://github.com/mlflow/mlflow/blob/v3.16.1/docs/docs/genai/eval-monitor/scorers/llm-judge/custom-judges/supported-models.mdx
- **Pointing at self-hosted vLLM:** LiteLLM's provider is `hosted_vllm/<model>` with `HOSTED_VLLM_API_BASE` (https://docs.litellm.ai/docs/providers/vllm). In MLflow that is `model="hosted_vllm:/case-summary-llm"`. This is inferred from the MLflow LiteLLM rule and not tested here.
- **Caveat:** an 8B model judging itself is weak and circular. On one 24 GB card a second, different local judge model would compete for VRAM. Practical choice:
  - Deterministic layers 1–2 are the hard gate.
  - The judge is a soft signal that warns and is tracked for drift.
  - The judge is either a small hosted model (as in PromptGuard: tiny cost, needs a CI secret) or the same local model as a documented compromise.

  **Flag:** a hosted judge is not excluded by ADR-0004, but it means CI calls an external API. Decide explicitly.

**Code sketch** (MLflow 3.16, vLLM OpenAI API, PromptGuard-style gate):

```python
# services/case_summary/llm.py
import mlflow, openai
from pydantic import BaseModel, Field
from typing import Literal

class CaseSummary(BaseModel):
    summary: str = Field(max_length=600)
    risk_factors: list[str] = Field(max_length=5)
    recommended_action: Literal["block", "review", "allow"]
    language: Literal["en", "fr"]

client = openai.OpenAI(base_url="http://vllm.llm.svc:8000/v1", api_key="unused")
mlflow.openai.autolog()  # traces + token usage per call

@mlflow.trace
def summarise(txn: dict, decision: str, lang: str, prompt_uri="prompts:/case-summary@production") -> CaseSummary:
    prompt = mlflow.genai.load_prompt(prompt_uri)
    resp = client.chat.completions.create(
        model="case-summary-llm",
        temperature=0.2,
        messages=[{"role": "user", "content": prompt.format(transaction=txn, decision=decision, language=lang)}],
        response_format={"type": "json_schema",
                         "json_schema": {"name": "case_summary", "schema": CaseSummary.model_json_schema()}},
    )
    return CaseSummary.model_validate_json(resp.choices[0].message.content)
```

```python
# evals/run_eval.py  (CI job on the self-hosted GPU runner)
import re, sys, mlflow
from mlflow.genai import scorer
from mlflow.entities import Feedback
from mlflow.genai.judges import make_judge
from typing import Literal

NUM = re.compile(r"\d+(?:[.,\s]\d{3})*(?:[.,]\d+)?")
def _norm(s): return s.replace(" ", "").replace(" ", "").replace(",", ".")

@scorer
def no_hallucinated_numbers(inputs: dict, outputs: dict) -> Feedback:
    allowed = {_norm(str(v)) for v in inputs["txn"].values()} | {_norm(f'{inputs["txn"]["amt"]:.2f}')}
    found = {_norm(m) for m in NUM.findall(outputs["summary"])}
    bad = sorted(n for n in found if n not in allowed)
    return Feedback(value=not bad, rationale=f"unsupported numbers: {bad}" if bad else "all numbers grounded")

@scorer
def decision_matches(inputs: dict, outputs: dict) -> bool:
    return outputs["recommended_action"] == inputs["decision"]

@scorer
def language_matches(inputs: dict, outputs: dict) -> bool:
    return outputs["language"] == inputs["lang"]

analyst_usefulness = make_judge(
    name="analyst_usefulness",
    instructions=("Grade the fraud case summary in {{ outputs }} for a fraud analyst, given the transaction "
                  "{{ inputs }} and the reference {{ expectations }}. 5 = accurate, concise, names the real risk "
                  "factors; 1 = misleading or fabricates facts."),
    model="openai:/gpt-4o-mini",          # or "hosted_vllm:/case-summary-llm" (needs litellm), see caveat
    feedback_value_type=Literal["1", "2", "3", "4", "5"],
)

results = mlflow.genai.evaluate(
    data=load_golden("evals/golden/v1.json"),            # [{"inputs":{txn,decision,lang}, "expectations":{reference}}]
    predict_fn=lambda txn, decision, lang: summarise(txn, decision, lang, sys.argv[1]).model_dump(),
    scorers=[no_hallucinated_numbers, decision_matches, language_matches, analyst_usefulness],
)
sys.exit(gate(results.metrics, baseline=previous_run_metrics()))  # 0 ok, 1 warning, 2 critical (PromptGuard semantics)
```

**GitLab CI.** The GitLab Runner docker executor needs `gpus = "all"` under `[runners.docker]` plus the NVIDIA Container Toolkit. The shell executor needs no runner config. https://docs.gitlab.com/runner/configuration/gpus/

The job should call the already-running vLLM for prompt-only changes. For a model change it must run a candidate vLLM, and on one card that means scaling the in-cluster Deployment to 0 first (see Risks).

#### Q5. LLMOps observability and cost

**Tracing.** `mlflow.openai.autolog()` captures every chat call as a trace with inputs, outputs, latency and token usage. Quoted from the docs:

> "MLflow automatically tracks token usage and cost for OpenAI API calls"

For streaming, set `stream_options={"include_usage": True}`. https://github.com/mlflow/mlflow/blob/v3.16.1/docs/docs/genai/tracing/integrations/listing/openai.mdx

The automatic cost uses provider price tables, so for a self-hosted model rely on token counts and the formula below. MLflow traces are OTel-compatible:

- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` sends them to an OTel Collector.
- `MLFLOW_TRACE_ENABLE_OTLP_DUAL_EXPORT=true` sends to both MLflow and the collector.
- `MLFLOW_ENABLE_OTEL_GENAI_SEMCONV=true` emits `gen_ai.*` semantic-convention attributes.

https://github.com/mlflow/mlflow/blob/v3.16.1/docs/docs/genai/tracing/opentelemetry/export.mdx. Link the trace to the Transaction id and the prompt version (`prompts:/case-summary/7`) as trace tags.

**Metrics.** Prometheus scrapes vLLM `/metrics` (Q2 list). The case-summary service adds its own counters and histograms: `case_summary_requests_total{prompt_version,outcome}`, `case_summary_schema_failures_total`, `case_summary_latency_seconds`. Grafana panels:

- TTFT p50/p95 and e2e p95
- tokens/s from `rate(vllm:generation_tokens_total[5m])`
- `vllm:kv_cache_usage_perc`
- queue depth (`vllm:num_requests_waiting`)
- cost per 1k summaries (below)

**Cost model for the README.** Self-hosted cost is per GPU-hour, not per token:

```
cost_per_gpu_hour  = (P_avg_kW × price_kWh) + (hardware_cost / (lifetime_years × 8760 × duty_cycle))
summaries_per_hour = measured throughput at the target p95 (vllm bench serve / the eval run)
cost_per_1k        = 1000 × cost_per_gpu_hour / summaries_per_hour
cost_per_1M_output_tokens = 1e6 × cost_per_gpu_hour / (3600 × measured_output_tokens_per_s)
```

Inputs with sources:

- **RTX 3090 board power:** 350 W (https://www.nvidia.com/en-us/geforce/graphics-cards/30-series/rtx-3090-3090ti/). This machine reports `power.limit 350 W`.
- **French regulated electricity (EDF Tarif Bleu, Base, 6 kVA):** €0.2001/kWh from 2026-08-01, per CRE deliberation 2026-147. Secondary source: https://www.fournisseurs-electricite.com/fournisseurs/edf/tarifs/bleu-reglemente. Verify on cre.fr before quoting.

  Worst case at full power: 0.35 × 0.2001 ≈ **€0.07 per GPU-hour** of energy.
- **Hardware amortisation** is an assumption. Document it, for example "€X purchase / 3 years".
- **Cloud comparison:** GKE `g2-standard-8` (1× L4, 24 GB) is about $0.85/h on demand in us-central1. Third-party aggregator: https://gcloud-compute.com/g2-standard-8.html. Google's own pages did not render for extraction, so check the GCP pricing calculator before quoting.

The README story: local energy is about 10× cheaper per hour, but a dedicated GPU only pays off at sustained utilisation. Analyst summaries are bursty and low-QPS, so at real scale "scale-to-zero on GKE" or "hosted API" (ADR-0004's one-config-change swap) is a fair build-vs-buy point.

### Recommendation

1. **Serving.**
   - vLLM **v0.30.0**, pinned image `vllm/vllm-openai:v0.30.0`, falling back to `-cu129` if the driver complains.
   - Served from a small in-repo Helm chart on an **nvkind** cluster: toolkit 1.20.1, nvkind built from commit ≥ `9a3061c7` (pin `c5705049`), device plugin chart 0.20.1.
   - Timebox nvkind to about 2 h. On failure, run the same image as a Docker container on the `kind` network behind an ExternalName Service, and say so in the README.
   - Flags: `--max-model-len 8192 --gpu-memory-utilization 0.85 --language-model-only --max-num-seqs 32`.
2. **Model.**
   - **Primary: `mistralai/Ministral-3-8B-Instruct-2512`.**
     - Apache-2.0.
     - Vendor-released FP8 checkpoint (runs as Marlin W8A16 on the 3090, about 10 GB).
     - Non-thinking instruct model, so structured output is simple.
     - Native French from a French lab. That is a natural talking point for this employer.
     - Within the ADR's "7–8B" envelope.
   - **Fallback: `Qwen/Qwen3.5-9B` via `RedHatAI/Qwen3.5-9B-quantized.w4a16`**, with `--default-chat-template-kwargs '{"enable_thinking": false}'`.
   - **Stretch / quality option:** `google/gemma-4-12B-it-qat-w4a16-ct` (official QAT, Apache-2.0). Let the golden set decide between them. That bake-off is itself a good demo of the eval gate.
3. **Eval.**
   - MLflow 3.16 `mlflow.genai.evaluate` with:
     - deterministic `@scorer`s (schema, grounded numbers and entities, Decision match, language, no PAN) as the **hard gate**;
     - a `make_judge` usefulness score as a **soft, tracked** signal.
   - PromptGuard's thresholds, all-failed ⇒ critical, per-case regressions, drift window and HTML report wrap `results.metrics`.
   - Skip promptfoo, DeepEval and Ragas. Mention them in build-vs-buy.
4. **Observability.**
   - `mlflow.openai.autolog()` traces tagged with the prompt version and Transaction id, with optional OTLP dual export.
   - Prometheus scrapes vLLM `/metrics` and the service's own metrics.
   - The README reports TTFT/e2e p95, tokens/s, KV usage, and cost per 1k summaries from the GPU-hour formula.

**Contradictions and tensions with decisions:**

- **ADR-0004 says "~7–8B".** Ministral 3 8B fits. The best-quality small options (Qwen3.5-9B, Gemma 4 12B) are 9–12B. That is a minor wording change if one of them wins the bake-off.
- **ADR-0003 says "kind as single runtime".** It holds only if nvkind works. The fallback puts vLLM outside kind, which weakens the "GPU scheduling" evidence named in ADR-0004's consequences.
- **Staging and prod namespaces (glossary).** There is only one GPU, so there can be only one vLLM. Both namespaces must share a single `llm` namespace Service. Staging cannot run a different model at the same time as prod.
- **LLM judge.** Using a hosted judge in CI would add an external API dependency that ADR-0004 does not mention.

### Risks and gotchas

- **Desktop shares the GPU.** About 1.9 GB of VRAM is in use by the desktop and other apps (ALFRED python process 1.2 GB). vLLM's default `gpu_memory_utilization=0.92` will fail the startup free-memory check. Use ≤ 0.85, and stop other GPU apps before demos.
- **One GPU, many consumers.** Only one vLLM process fits. The CI eval job for a *model* change has to scale the in-cluster vLLM to 0, start the candidate, evaluate, then restore. Prompt-only changes can reuse the running server. vLLM `--enable-sleep-mode` exists but does not help with a different model.
- **nvkind has no releases.** Pin a commit. Builds from before 2026-04-20 fail with toolkit ≥ 1.18 (#61). Plain `kind create cluster` with the template skips the in-node setup and gives `ERROR_LIBRARY_NOT_FOUND` (#88). Node creation runs `apt-get` from `nvidia.github.io` inside each GPU node, so cluster rebuilds are slower and need internet.
- **`nvidia-ctk runtime configure --set-as-default`** changes Docker's default runtime host-wide. These are sudo steps the user must run (system configuration).
- **CUDA 13 default image** needs the 580+ driver. The host has 580.173. Re-check after any driver downgrade.
- **v0.30.0 dropped GPTQ `g_idx`.** Avoid `desc_act=True` GPTQ checkpoints. `guided_json` and friends were removed in 0.12.0, so use `response_format` / `structured_outputs`.
- **FP8 on Ampere is weight-only (Marlin).** You get the memory saving but no speed-up, and the log prints a warning. Do not read that warning as a failure.
- **Reasoning models** (Qwen3.5, Granite 4.2) think by default. This inflates latency and tokens and can conflict with structured output. Disable it server-wide.
- **Ministral needs mistral-format flags** (`--tokenizer_mode mistral --config_format mistral --load_format mistral`). Without them the FP8 checkpoint may not load correctly. Its vision tower stays BF16. `--language-model-only` avoids reserving memory for image inputs.
- **Prompt injection via Transaction fields** (merchant names are free text). Keep transaction data in a delimited data block, never in instructions, and add an adversarial golden case.
- **Judge circularity and non-determinism.** The judge's score must not be the only gate. Use temperature 0 and record the judge model and version on each eval run.
- **Metric names change between vLLM minors** under the X.Y+2 removal policy. Pin dashboards to the version.
- **French quality is unmeasured publicly** for these models. Only the project's golden set answers it.
- **Cost numbers.** Electricity and GCP prices above come from secondary sources. Re-verify, and label the hardware amortisation as an assumption.
- **The vLLM `--api-key` is not real auth.** Keep the Service ClusterIP-only (matches "local-only, no service auth").

### Sources

- nvkind README, templates, `pkg/nvkind/node.go`, commits, issues #20, #61, #74, #85, #88: https://github.com/NVIDIA/nvkind, https://github.com/NVIDIA/nvkind/issues (checked 2026-10-04)
- kind v0.33.0 release, node images: https://github.com/kubernetes-sigs/kind/releases ; entrypoint DNS fix: https://github.com/kubernetes-sigs/kind/blob/v0.33.0/images/base/files/usr/local/bin/entrypoint
- NVIDIA Container Toolkit install guide (1.20.1-1): https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html ; releases: https://github.com/NVIDIA/nvidia-container-toolkit/releases
- k8s-device-plugin v0.20.1: https://github.com/NVIDIA/k8s-device-plugin/releases ; GPU Operator v26.7.1: https://github.com/NVIDIA/gpu-operator/releases
- GKE GPUs: https://docs.cloud.google.com/kubernetes-engine/docs/how-to/gpus
- vLLM v0.30.0 release notes: https://github.com/vllm-project/vllm/releases/tag/v0.30.0
- vLLM config docstrings: https://github.com/vllm-project/vllm/blob/v0.30.0/vllm/config/cache.py, https://github.com/vllm-project/vllm/blob/v0.30.0/vllm/config/model.py, https://github.com/vllm-project/vllm/blob/v0.30.0/vllm/config/multimodal.py, https://github.com/vllm-project/vllm/blob/v0.30.0/vllm/v1/worker/utils.py
- vLLM quantisation table: https://github.com/vllm-project/vllm/blob/main/docs/features/quantization/README.md ; FP8 Marlin fallback: https://github.com/vllm-project/vllm/blob/v0.30.0/vllm/model_executor/layers/quantization/utils/marlin_utils_fp8.py
- vLLM structured outputs: https://docs.vllm.ai/en/latest/features/structured_outputs/ ; reasoning outputs: https://github.com/vllm-project/vllm/blob/v0.30.0/docs/features/reasoning_outputs.md
- vLLM metrics: https://docs.vllm.ai/en/latest/usage/metrics/, https://docs.vllm.ai/en/latest/design/metrics/
- vLLM Docker: https://docs.vllm.ai/en/latest/deployment/docker/ ; Kubernetes: https://github.com/vllm-project/vllm/blob/v0.30.0/docs/deployment/k8s.md ; Helm: https://github.com/vllm-project/vllm/blob/v0.30.0/docs/deployment/frameworks/helm.md
- vLLM OpenAI server: https://github.com/vllm-project/vllm/blob/v0.30.0/docs/serving/online_serving/openai_compatible_server.md
- vLLM production-stack (vllm-stack-0.1.13, 2026-09-29): https://github.com/vllm-project/production-stack
- Model cards: https://huggingface.co/mistralai/Ministral-3-8B-Instruct-2512, https://huggingface.co/Qwen/Qwen3.5-9B, https://huggingface.co/google/gemma-4-12B-it, https://huggingface.co/google/gemma-4-12B-it-qat-w4a16-ct, https://huggingface.co/ibm-granite/granite-4.2-8b, https://huggingface.co/utter-project/EuroLLM-9B-Instruct-2512, https://huggingface.co/RedHatAI/Qwen3.5-9B-quantized.w4a16 ; HF model API listings (https://huggingface.co/api/models?author=…) checked 2026-10-04
- MLflow 3.16.1 docs (from repo tag): supported judge models https://github.com/mlflow/mlflow/blob/v3.16.1/docs/docs/genai/eval-monitor/scorers/llm-judge/custom-judges/supported-models.mdx ; predefined judges …/llm-judge/predefined.mdx ; custom scorers …/scorers/custom/index.mdx ; evaluating prompts https://github.com/mlflow/mlflow/blob/v3.16.1/docs/docs/genai/prompt-registry/evaluate-prompts.mdx ; OpenAI tracing …/tracing/integrations/listing/openai.mdx ; OTel export …/tracing/opentelemetry/export.mdx ; prompt registry https://mlflow.org/docs/latest/genai/prompt-registry/
- LiteLLM vLLM provider: https://docs.litellm.ai/docs/providers/vllm
- promptfoo vLLM provider: https://www.promptfoo.dev/docs/providers/vllm/ ; DeepEval faithfulness: https://deepeval.com/docs/metrics-faithfulness ; versions from https://pypi.org (mlflow 3.16.1, deepeval 4.2.8, ragas 0.4.3) and https://github.com/promptfoo/promptfoo/releases (0.123.1)
- GitLab Runner GPUs: https://docs.gitlab.com/runner/configuration/gpus/
- RTX 3090 specs: https://www.nvidia.com/en-us/geforce/graphics-cards/30-series/rtx-3090-3090ti/
- EDF Tarif Bleu Aug 2026 (secondary): https://www.fournisseurs-electricite.com/fournisseurs/edf/tarifs/bleu-reglemente
- g2-standard-8 price (secondary): https://gcloud-compute.com/g2-standard-8.html
- PromptGuard: `~/projects/Model_Regression_Detection_System/README.md`, `src/evalsys/judge.py`, `src/evalsys/scorer.py`
