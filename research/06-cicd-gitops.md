## 06. CI/CD and GitOps

Researched 2026-10-04. Versions and dates come from GitHub/GitLab release feeds, registry listings and official docs, all fetched on that date. Some claims were also checked on this machine (Docker 29.8): the UBI Python packages, a Python 3.13 build on UBI 9 run with an arbitrary UID, unprivileged Buildah, and image digests. Those checks are marked **(verified locally)**.

Versions in play (as of 2026-10-04):

| Tool | Version | Date | Source |
|---|---|---|---|
| GitLab (gitlab.com) | 19.4.x | 19.4.0-ee tagged 2026-09-16 | [gitlab-org/gitlab tags](https://gitlab.com/gitlab-org/gitlab/-/tags) |
| GitLab Runner | v19.4.1 | 2026-09-24 | [gitlab-runner releases](https://gitlab.com/gitlab-org/gitlab-runner/-/releases) |
| Argo CD | v3.5.3 stable (v3.6.0-rc1 2026-09-16) | 2026-09-14 | [argo-cd releases](https://github.com/argoproj/argo-cd/releases) |
| Argo CD Image Updater | v1.3.0 | 2026-08-13 | [image-updater releases](https://github.com/argoproj-labs/argocd-image-updater/releases) |
| kind | v0.33.0 | 2026-09-16 | [kind releases](https://github.com/kubernetes-sigs/kind/releases) |
| Trivy | v0.75.0 | 2026-10-01 | [trivy releases](https://github.com/aquasecurity/trivy/releases) |
| k6 | v2.3.0 (v2.0.0 was 2026-05-11) | 2026-09-21 | [k6 releases](https://github.com/grafana/k6/releases) |
| Schemathesis | 4.29.1 | 2026-10-03 | [schemathesis releases](https://github.com/schemathesis/schemathesis/releases) |
| SonarScanner CLI (image `sonarsource/sonar-scanner-cli:latest`) | 8.1.0.6389 | pulled 2026-10-04 | (verified locally) |
| SonarQube Community Build | 26.9.0.129388 | — | [release notes](https://docs.sonarsource.com/sonarqube-community-build/server-update-and-maintenance/release-notes) |
| Buildah (`quay.io/buildah/stable`) | 1.43.4 | pulled 2026-10-04 | (verified locally) |
| uv (`ghcr.io/astral-sh/uv:latest`) | 0.12.23 (this machine has 0.11) | pulled 2026-10-04 | (verified locally), [uv GitLab guide](https://docs.astral.sh/uv/guides/integration/gitlab/) |
| GitLab CI component `components/sast` | 3.5.0 | 2026-09-25 | [components/sast](https://gitlab.com/components/sast) |
| GitLab CI component `components/container-scanning` | 5.2.0 | 2026-02-05 | [components/container-scanning](https://gitlab.com/components/container-scanning) |

Image digests resolved on 2026-10-04 (pin these or re-resolve them):
- `aquasec/trivy:0.75.0@sha256:af6acf9a6b85dfe389a1941505c0ce9efef52a4719635e1a962f022a3d855daa`
- `grafana/k6:2.3.0@sha256:9c2dee7f8ed74d317e4027c06a10f169b625638189de8d4555d0b3486a5aeb34`
- `quay.io/buildah/stable:latest@sha256:7f69b7665f1bdfb41747b695a3c89752e1a5fe54f764a48e130e5b19caac4cde`
- `sonarsource/sonar-scanner-cli:latest@sha256:a3f4215076706c95a17a68c19322ee916e40a3acd081a8c1a1e839e0194afa57`

### Questions

1. GitLab CI in 2026: how should `.gitlab-ci.yml` be structured for a Python uv monorepo (uv caching, `rules:changes` per service, a `needs` DAG)? Which CI/CD components or templates are worth using? How does GitLab's container registry work? What are the free-tier compute-minute limits on gitlab.com for a public versus a private project?
2. Self-hosted GitLab Runner on Linux with a GPU: how do we install it, register it with the new runner authentication token flow, configure the docker executor with `gpus = "all"`, and use tags? What are the security caveats of a runner on a personal machine serving a public repo? How can a CI job deploy to, or test against, a local kind cluster on the same host?
3. SonarCloud (SonarQube Cloud) with GitLab: is it free for public projects? How do we set it up with the `sonar-scanner-cli` image, import Python coverage XML, and wait for the quality gate (`sonar.qualitygate.wait`)? What about self-hosted SonarQube Community Build as an alternative?
4. UBI-based Python 3.13 images: which UBI image provides Python 3.13, or how do we install it? How do we do a multi-stage build with uv? What are the rootless/OpenShift-friendly practices? Which build tool should CI use (is Kaniko archived? Buildah? docker-in-docker)? How should Trivy scan in GitLab (container scanning template or Trivy directly), and with what fail thresholds?
5. GitOps image-tag bumps: should CI commit updated image tags to `deploy/` (same repo or a separate config repo, and how do we avoid CI loops with `[skip ci]`), or should Argo CD Image Updater do it, and what is its current status? Argo CD on kind: how do we install it, should staging/prod use an ApplicationSet or app-of-apps, how do we promote by git tag (pointing prod at a tag), and which Argo CD version?
6. k6 load testing in CI: how do we run k6 against a service in kind, set thresholds on p(99), and use exit codes to gate? What contract-testing options exist for FastAPI (Schemathesis)?

### Findings

#### Q1. GitLab CI structure, components, registry, compute minutes

**Compute minutes (gitlab.com).** A public project and a private project get the same quota. Only membership in the GitLab for Open Source program discounts a public project.
- "Free tier namespaces receive 400 compute minutes per month." ([compute minutes](https://docs.gitlab.com/ci/pipelines/compute_minutes/))
- Cost-factor table ([compute minutes](https://docs.gitlab.com/ci/pipelines/compute_minutes/)):
  - "Standard projects | Based on runner type"
  - "Public projects in the GitLab for Open Source program | `0.5` | 1 minute per 2 minutes of job time"
  - "Public forks of projects in the GitLab for Open Source program | `0.008` | 1 minute per 125 minutes of job time"
  - The default hosted runner is "Linux x86-64 (default) | `small` | `1`". It is "`saas-linux-small-amd64` (default) | 2 | 8 GB | 30 GB" (vCPU, memory, storage) ([hosted Linux runners](https://docs.gitlab.com/ci/runners/hosted_runners/linux/)). The GPU hosted runner has cost factor `7`.
- Self-hosted runners do not consume the quota: "Project and group runners are not affected by the compute quota and continue processing jobs." ([instance runner compute minutes](https://docs.gitlab.com/ci/pipelines/instance_runner_compute_minutes/))
- Storage: "Each project in a Free tier namespace on GitLab.com has 10 GiB of free storage." Also: "The container registry, package registry, and build artifacts are not included in the limit." ([storage quotas](https://docs.gitlab.com/user/storage_usage_quotas/))

Implication: 400 small-runner minutes will not cover image builds, Trivy, the model gate and the LLM eval. Route every job to the self-hosted runner and turn off instance runners for the project (Settings > CI/CD > Runners).

**uv in GitLab CI** ([uv GitLab integration](https://docs.astral.sh/uv/guides/integration/gitlab/)):

```yaml
variables:
  UV_VERSION: "0.12.23"
  PYTHON_VERSION: "3.12"
  BASE_LAYER: trixie-slim
  UV_LINK_MODE: copy
uv:
  image: ghcr.io/astral-sh/uv:$UV_VERSION-python$PYTHON_VERSION-$BASE_LAYER
```
- On `UV_LINK_MODE: copy`, the guide says it is needed because "GitLab CI creates a separate mountpoint for the build directory."
- The caching pattern: `UV_CACHE_DIR: .uv-cache`, `cache: key: files: [uv.lock]`, and `after_script: uv cache prune --ci`.
- The uv images come with Python 3.12, 3.13 and so on. Use `python3.13` to match the project.

**`rules:changes` semantics.** These matter for per-service jobs in a monorepo. Quoted from the [CI YAML reference](https://docs.gitlab.com/ci/yaml/#ruleschanges):
- "For new branch pipelines or when there is no Git `push` event, `rules: changes` always evaluates to true and the job always runs. Pipelines like tag pipelines, scheduled pipelines, and manual pipelines, all do not have a Git `push` event associated with them."
- "Merge request pipelines, `rules:changes` compares the changes with the target MR branch. Branch pipelines, `rules:changes` compares the changes with the previous commit on the branch."
- "A maximum of 50 patterns or file paths can be defined per `rules:changes` section."
- `rules:changes:compare_to` accepts "A branch name … A tag name … A commit SHA". It warns that it gives unexpected results with merged-results pipelines and in forks.

**`needs` limits.** "For GitLab.com, the limit is 50." Also: "Use `optional: true` in `needs` to depend on a job only if it exists in the pipeline." That second point is essential when `rules:changes` drops a service's build job ([CI YAML reference](https://docs.gitlab.com/ci/yaml/#needs); [needs](https://docs.gitlab.com/ci/yaml/needs/)).

**Duplicate pipelines.** "You should not include both push and merge request pipelines in the same job without `workflow:rules` that prevent duplicate pipelines." ([job rules](https://docs.gitlab.com/ci/jobs/job_rules/))

**CI/CD components.** The syntax is `include: - component: $CI_SERVER_FQDN/<project-path>/<component>@<version>`. The CI/CD Catalog was "Made generally available in GitLab 17.0" ([components](https://docs.gitlab.com/ci/components/)). GitLab-maintained components that are useful here:
- `gitlab.com/components/sast@3.5.0` (2026-09-25)
- `gitlab.com/components/secret-detection@2.4.0` (2026-08-12)
- `gitlab.com/components/dependency-scanning@2.1.1`
- `gitlab.com/components/opentofu@4.9.0`
- `gitlab.com/components/container-scanning@5.2.0`

Versions come from the GitLab API on 2026-10-04. On Free the jobs run, but "Presentation of Report data in Merge Request and Security tab" is Ultimate only ([container scanning](https://docs.gitlab.com/user/application_security/container_scanning/)). The useful ones are secret-detection and SAST, as extra evidence. For gating, use your own Trivy job (see Q4).

**Container registry.**
- `CI_REGISTRY_IMAGE` is the "Base address for the container registry to push, pull, or tag project's images, formatted as `<host>[:<port>]/<project_full_path>`" ([predefined variables](https://docs.gitlab.com/ci/variables/predefined_variables/)).
- `CI_REGISTRY_PASSWORD` "is the same as the `CI_JOB_TOKEN` and is valid only as long as the job is running."
- Naming: "`<registry server>/<namespace>/<project>[/<optional path>]`", with up to two extra levels, so `registry.gitlab.com/<user>/fraud-ml-platform/scoring:<sha>` works ([container registry](https://docs.gitlab.com/user/packages/container_registry/)).
- "If the project is public, the container registry is also public." kind nodes can therefore pull images anonymously, with no imagePullSecret.

**Skeleton** (a sketch assembled from the documented primitives above; not run against gitlab.com):

```yaml
workflow:
  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
    - if: $CI_COMMIT_BRANCH && $CI_OPEN_MERGE_REQUESTS
      when: never                       # no duplicate branch+MR pipelines
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
    - if: $CI_COMMIT_TAG =~ /^v\d+\.\d+\.\d+$/

stages: [lint, test, quality, build, scan, iac, gates, release, verify]

default:
  tags: [fraud-docker]                  # self-hosted; instance runners disabled

variables:
  UV_LINK_MODE: copy
  UV_CACHE_DIR: .uv-cache
  IMAGE_TAG: $CI_COMMIT_SHORT_SHA

.uv:
  image: ghcr.io/astral-sh/uv:0.12.23-python3.13-trixie-slim
  cache:
    - key: { files: [uv.lock] }
      paths: [$UV_CACHE_DIR]
  after_script: [uv cache prune --ci]

lint:
  extends: .uv
  stage: lint
  script:
    - uv sync --locked --all-packages
    - uv run ruff check . && uv run ruff format --check .
    - uv run mypy services packages

test:
  extends: .uv
  stage: test
  needs: [lint]
  script:
    - uv sync --locked --all-packages
    - uv run pytest --cov --cov-branch --cov-report=xml:coverage.xml --junitxml=report.xml
  artifacts:
    when: always
    reports: { junit: report.xml }
    paths: [coverage.xml]

# one build job per service, generated from a hidden template
.build:
  stage: build
  image: quay.io/buildah/stable@sha256:7f69b7665f1bdfb41747b695a3c89752e1a5fe54f764a48e130e5b19caac4cde
  variables: { STORAGE_DRIVER: vfs, BUILDAH_FORMAT: oci }
  script:
    - buildah build -f services/$SVC/Containerfile -t $CI_REGISTRY_IMAGE/$SVC:$IMAGE_TAG .
    - buildah push $CI_REGISTRY_IMAGE/$SVC:$IMAGE_TAG oci-archive:$SVC.tar
  artifacts: { paths: ["$SVC.tar"], expire_in: 1 day }

build:scoring:
  extends: .build
  needs: [test]
  variables: { SVC: scoring }
  rules:
    - if: $CI_COMMIT_TAG
      when: never                       # tag pipelines only promote (see Q5)
    - changes:
        paths: [services/scoring/**/*, packages/**/*, uv.lock]
```

With jobs like this:
- `scan:<svc>` needs `build:<svc>` (Trivy on `--input $SVC.tar`, see Q4).
- `push:<svc>` needs the scan and every gate.
- `bump-deploy` needs every `push:*` with `optional: true`.
- `verify:staging` needs `bump-deploy` (Q5, Q6).

#### Q2. Self-hosted runner with GPU, security, reaching kind

**Install (Debian/Ubuntu)** ([install](https://docs.gitlab.com/runner/install/linux-repository/)):

```bash
curl -L "https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.deb.sh" -o script.deb.sh
less script.deb.sh          # inspect first
sudo bash script.deb.sh
sudo apt install gitlab-runner
```
If you pin a version: "As of `gitlab-runner` version `v17.7.1`, when you install a specific version of `gitlab-runner` that is not the latest version, you must explicitly install the required `gitlab-runner-helper-packages` for that version." The example given is `sudo apt install gitlab-runner=17.7.1-1 gitlab-runner-helper-images=17.7.1-1`.

**Register with the new token flow** ([register](https://docs.gitlab.com/runner/register/)):
1. Create the runner in the UI (Project > Settings > CI/CD > Runners > New project runner). The tags, "Run untagged jobs" and **Protected** are set there, on the server side.
2. You get a `glrt-…` runner authentication token.
3. Register:
```bash
sudo gitlab-runner register \
  --non-interactive \
  --url "https://gitlab.com/" \
  --token "$RUNNER_TOKEN" \
  --executor "docker" \
  --docker-image alpine:latest \
  --docker-pull-policy "if-not-present" \
  --description "docker-runner"
```
"Runner registration tokens and several runner configuration arguments were deprecated. They are scheduled for removal in GitLab 20.0." Do not use `--registration-token` or the CLI tag flags.

**GPU** ([GPU config](https://docs.gitlab.com/runner/configuration/gpus/), introduced in Runner 13.9):
- Prerequisites: "Install NVIDIA Driver" and "Install NVIDIA Container Toolkit".
- In `[runners.docker]`, set `gpus = "all"` and `service_gpus = "all"`.
- Smoke test: a job with `script: - nvidia-smi`.
- The advanced config describes `gpus` as "GPU devices for Docker container. Uses the same format as the `docker` CLI" ([advanced configuration](https://docs.gitlab.com/runner/configuration/advanced-configuration/)).

**Tags.** "For runners with multiple tags like `[docker, shell, gpu]`, jobs require all specified tags to execute." Also: "Runners can be configured to accept untagged jobs by selecting a 'Run untagged jobs' checkbox." ([configure runners](https://docs.gitlab.com/ci/runners/configure_runners/))

**Security caveats** (personal machine, public repo):
- "Any user that has the Developer role for the project's repository could compromise the security of the environment hosting the runner." ([runner security](https://docs.gitlab.com/runner/security/))
- "When privileged mode is enabled, a user running a CI/CD job could gain full root access to the runner's host system". Also: "It is **not advised** to run containers in privileged mode". Use privileged mode only "on isolated and ephemeral virtual machines".
- On socket binding: "When you share the Docker daemon, you effectively disable the container's security mechanisms and expose your host to privilege escalation." ([Docker build](https://docs.gitlab.com/ci/docker/using_docker_build/))
- Forks: "A merge request from a fork that is submitted to the parent project triggers a pipeline that is created and runs in the fork (source) project, not the parent (target) project." So strangers' fork MRs do not reach your project runner unless you manually run them in the parent. The warning there reads: "Fork merge requests can contain malicious code that tries to steal secrets in the parent project" ([MR pipelines](https://docs.gitlab.com/ci/pipelines/merge_request_pipelines/)).
- Protected runners: "you can configure them to only run jobs on protected branches, or jobs that have protected tags." ([configure runners](https://docs.gitlab.com/ci/runners/configure_runners/))
- Masking: "Masking a CI/CD variable is not a guaranteed way to prevent malicious users from accessing variable values." ([CI/CD variables](https://docs.gitlab.com/ci/variables/))
- Public pipelines: in a public project, job logs are visible to non-members unless CI/CD visibility is set to "Only project members" ([pipeline settings](https://docs.gitlab.com/ci/pipelines/settings/)).

**Reaching kind from a job.** kind's API server is published on `127.0.0.1:<random>` on the host, so a job container on the default bridge cannot reach it. The runner's `network_mode` option ("Add container to a custom network") can attach jobs to kind's Docker network, named `kind`. `kind get kubeconfig --internal` ("use internal address instead of external", [kind source](https://github.com/kubernetes-sigs/kind/blob/main/pkg/cmd/kind/get/kubeconfig/kubeconfig.go)) produces a kubeconfig pointing at `https://<cluster>-control-plane:6443`, which resolves on that network. Mount the kubeconfig read-only from the host so it never becomes a GitLab variable.

Proposed `/etc/gitlab-runner/config.toml` (three runners created in the UI, each with its own `glrt-` token):

```toml
concurrent = 3

[[runners]]            # UI: tags [fraud-docker], run untagged, NOT protected
  name = "fraud-docker"
  url = "https://gitlab.com"
  token = "glrt-REDACTED"
  executor = "docker"
  [runners.docker]
    image = "registry.access.redhat.com/ubi9/ubi-minimal:latest"
    privileged = false
    # needed only for unprivileged Buildah (verified locally, see Q4)
    security_opt = ["seccomp:unconfined", "apparmor:unconfined"]
    allowed_images = ["ghcr.io/astral-sh/uv:*", "quay.io/buildah/stable*", "aquasec/trivy*", "sonarsource/*", "registry.access.redhat.com/*", "grafana/k6*"]
    volumes = ["/cache"]

[[runners]]            # UI: tags [gpu], PROTECTED
  name = "fraud-gpu"
  url = "https://gitlab.com"
  token = "glrt-REDACTED"
  executor = "docker"
  [runners.docker]
    image = "nvidia/cuda:12.8.0-base-ubi9"
    gpus = "all"
    privileged = false

[[runners]]            # UI: tags [kind], PROTECTED
  name = "fraud-kind"
  url = "https://gitlab.com"
  token = "glrt-REDACTED"
  executor = "docker"
  [runners.docker]
    image = "registry.access.redhat.com/ubi9/ubi-minimal:latest"
    network_mode = "kind"
    volumes = ["/home/professorx/.kube/kind-internal.yaml:/kube/config:ro", "/cache"]
    privileged = false
```
Generate the mounted file with `kind get kubeconfig --name fraud --internal > ~/.kube/kind-internal.yaml`. A better option is a kubeconfig for a ServiceAccount that only gets `get/list/watch` on `applications.argoproj.io` and pods/services in `staging`. Argo CD does the deploying, so CI never needs write access to the cluster.

For a deploy-free design, CI's only cluster interactions are:
- waiting for Argo CD to report the bumped revision as synced and healthy;
- running contract and load tests against staging (Q6).

#### Q3. SonarQube Cloud (formerly SonarCloud)

**Plan facts** ([subscription plans](https://docs.sonarsource.com/sonarqube-cloud/administering-sonarcloud/managing-subscription/subscription-plans/)):
- "Analysis of public projects: unlimited number of projects" on every plan, Free included.
- Free private projects: "Up to 50k LOC".
- Free tier limits:
  - "Only main branch analysis".
  - Pull request analysis "Only if the target branch is the main branch".
  - Members are limited to 5.
  - **No custom quality gates or custom quality profiles.** In the comparison table, those rows are ticked only for Team and Enterprise.
- "DevOps platform binding (GitHub, Bitbucket Cloud, GitLab, Azure DevOps)" is on all tiers.

**The built-in "Sonar way" gate** is therefore the coverage gate on Free ([quality gates](https://docs.sonarsource.com/sonarqube-cloud/standards/managing-quality-gates/introduction-to-quality-gates.md)): "New code test coverage is greater than or equal to 80.0%" and "Duplication in the new code is less than or equal to 3.0%". It also requires A ratings and "All new Security Hotspots are reviewed."

**Onboarding.** Import the GitLab group as an organization using a GitLab personal access token. Coverage import requires CI-based analysis, not automatic analysis ("if automatic analysis is not supported for your project or you don't want to use it, you'll need to set up CI-based analysis") ([GitLab onboarding](https://docs.sonarsource.com/sonarqube-cloud/getting-started/gitlab.md)).

**GitLab CI job** (verbatim from [SonarQube Cloud GitLab CI](https://docs.sonarsource.com/sonarqube-cloud/analyzing-source-code/ci-based-analysis/gitlab-ci.md); add `SONAR_TOKEN` as a masked, protected variable):

```yaml
variables:
 SONAR_USER_HOME: "${CI_PROJECT_DIR}/.sonar"  # Defines the location of the analysis task cache
 GIT_DEPTH: "0"  # Tells git to fetch all the branches of the project, required by the analysis task
sonarcloud-check:
 image:
   name: sonarsource/sonar-scanner-cli:latest
   entrypoint: [""]
 cache:
   key: "${CI_JOB_NAME}"
   paths:
     - .sonar/cache
 script:
   - sonar-scanner
 rules:
    - if: $CI_COMMIT_REF_NAME == 'main' || $CI_PIPELINE_SOURCE == 'merge_request_event'
```

Add `needs: [test]` so the job receives `coverage.xml`. Then `sonar-project.properties`:

```properties
sonar.projectKey=<org>_fraud-ml-platform
sonar.organization=<org>
sonar.sources=services,packages,dags
sonar.tests=tests
sonar.python.version=3.13
sonar.python.coverage.reportPaths=coverage.xml
sonar.qualitygate.wait=true
sonar.qualitygate.timeout=300
```
- On the gate, the docs say: to halt the pipeline, "incorporate `sonar.qualitygate.wait=true`". `sonar.qualitygate.timeout` is in seconds (default 300). "If this threshold is exceeded, the scanner treats it as a failure".
- Coverage: "The essential requirements are that the tool produces its report in the Cobertura XML format" (`pytest --cov --cov-report=xml`). Set `relative_files = True` in the coverage config so the paths resolve ([Python coverage](https://docs.sonarsource.com/sonarqube-cloud/analyzing-source-code/test-coverage/python-test-coverage.md)).

**Self-hosted SonarQube Community Build.**
- Latest is 26.9.0.129388 ([release notes](https://docs.sonarsource.com/sonarqube-community-build/server-update-and-maintenance/release-notes)).
- "SonarQube Community Build doesn't support various features such as the analysis of multiple branches and pull requests." ([GitLab integration](https://docs.sonarsource.com/sonarqube-community-build/devops-platform-integration/gitlab-integration/introduction.md))
- The release notes add that Community Build "doesn't scan for critical injection vulnerabilities such as SQL injection and XSS".
- It would add a ~1 GB JVM + Postgres to an already RAM-tight host, and gitlab.com runners could not reach it unless the self-hosted runner does the scanning.
- Its one advantage is custom quality gates, which the Cloud Free plan lacks.

#### Q4. UBI Python 3.13 images, build tooling, Trivy

**No UBI image or UBI RPM provides Python 3.13** (verified locally, 2026-10-04):
- `registry.access.redhat.com` returns tags for `ubi9/python-312`, `ubi9/python-314`, `ubi9/python-314-minimal`, `ubi10/python-312-minimal` and `ubi10/python-314-minimal`. It has nothing for `ubi9/python-313`, `ubi10/python-313` or `ubi10/python-313-minimal`.
- The UBI repos inside `ubi9/ubi-minimal` (RHEL 9.8) offer `python3.12-3.12.14` and `python3.14-3.14.7` only. `ubi10/ubi-minimal` (RHEL 10.2) offers `python3-3.12.14` and `python3.14-3.14.7`. `microdnf repoquery python3.13` returns nothing on either.
- The sclorg README agrees. It lists `rhel9/python-312` and `rhel9/python-314`; 3.13 exists only as `quay.io/sclorg/python-313-c10s` (CentOS Stream 10) and Fedora ([s2i-python-container](https://github.com/sclorg/s2i-python-container)).

**Working option: ubi9-minimal plus uv-managed CPython 3.13** (verified locally). The image built, ran as UID 123456 with GID 0, printed `3.13.16`, imported FastAPI, and was 344 MB.

```dockerfile
# services/scoring/Containerfile
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /bin/
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_PYTHON_PREFERENCE=only-managed \
    UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src
RUN uv python install 3.13
COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY services/scoring/pyproject.toml services/scoring/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package scoring --no-install-workspace
COPY services/scoring/ services/scoring/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package scoring --no-editable

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest
COPY --from=build /opt/python /opt/python
COPY --from=build /opt/venv /opt/venv
RUN chgrp -R 0 /opt/venv && chmod -R g=u /opt/venv
ENV PATH=/opt/venv/bin:$PATH PYTHONUNBUFFERED=1
USER 1001
EXPOSE 8080
CMD ["uvicorn", "scoring.app:app", "--host", "0.0.0.0", "--port", "8080"]
```

The uv Docker guide recommends this:
- Pin uv: "`COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /bin/`", or better, a SHA256 digest.
- Set `UV_COMPILE_BYTECODE=1`.
- For workspaces, "apply `--no-install-workspace` during initial dependency installation and use `--frozen` instead of `--locked`" ([uv Docker](https://docs.astral.sh/uv/guides/integration/docker/)).

The alternatives are base images that do exist: `registry.access.redhat.com/ubi9/python-312` (Red Hat-built interpreter), or `ubi9/python-314`, or `ubi9-minimal` with `microdnf install python3.12`.

**OpenShift/rootless practices** ([OpenShift image guidelines source](https://github.com/openshift/openshift-docs/blob/main/modules/images-create-guide-openshift.adoc)):
- "By default, OpenShift Container Platform runs containers using an arbitrarily assigned user ID."
- "directories and files that are written to by processes in the image must be owned by the root group and be read/writable by that group". The prescribed lines are `RUN chgrp -R 0 /some/directory && chmod -R g=u /some/directory`.
- "the processes running in the container must not listen on privileged ports, ports below 1024".

So: numeric `USER 1001`, port 8080, and nothing written outside `/tmp` or a group-0-writable path. On kind, set `securityContext: runAsNonRoot: true, allowPrivilegeEscalation: false, readOnlyRootFilesystem: true`.

**Build tool status:**
- **Kaniko is archived.** "This project is archived and no longer developed or maintained." The repo has been read-only since **2025-06-03** ([kaniko](https://github.com/GoogleContainerTools/kaniko)). GitLab's page says "kaniko is no longer a maintained project" and points to Docker, Buildah and Podman ([GitLab kaniko page](https://docs.gitlab.com/ci/docker/using_kaniko/)).
- **Docker-in-Docker** needs `privileged = true` in the runner config ([DinD](https://docs.gitlab.com/ci/docker/docker_in_docker/)). That is not acceptable on a personal machine serving a public repo (Q2).
- **Buildah (recommended).** It is Red Hat's tool, which fits the RedHat signal in the job ad, and needs no daemon. This is GitLab's example ([Docker build](https://docs.gitlab.com/ci/docker/using_docker_build/)):
  ```yaml
  build:
    stage: build
    image: quay.io/buildah/stable
    variables:
      STORAGE_DRIVER: vfs
      BUILDAH_FORMAT: docker
      FQ_IMAGE_NAME: "$CI_REGISTRY_IMAGE/test"
    before_script:
      - echo "$CI_REGISTRY_PASSWORD" | buildah login -u "$CI_REGISTRY_USER" --password-stdin $CI_REGISTRY
    script:
      - buildah images
      - buildah build -t $FQ_IMAGE_NAME
      - buildah images
      - buildah push $FQ_IMAGE_NAME
  ```
  The docs give the reason for `vfs`: "Buildah cannot stack overlayfs on top of another overlayfs filesystem."
  **(verified locally)** On Docker 29.8, Buildah 1.43.4 in an unprivileged container fails with `Error during unshare(CLONE_NEWUSER): Operation not permitted`. With `--security-opt seccomp=unconfined --security-opt apparmor=unconfined` it builds successfully. Hence the `security_opt` line in the runner config (Q2). That loosens isolation, but much less than `privileged`.
- Other options: rootless BuildKit is mentioned by GitLab ("rootless BuildKit options that eliminate Docker daemon dependency"), and Podman.

**Trivy:**
- GitLab's Container Scanning (`include: - template: Jobs/Container-Scanning.gitlab-ci.yml` or the component) uses Trivy. Its `CS_SEVERITY_THRESHOLD` only filters output: "The scanner outputs vulnerabilities with severity level higher than or equal to this threshold." The docs describe no fail-on-findings behaviour, and on Free there is no MR or Security-tab view ([container scanning](https://docs.gitlab.com/user/application_security/container_scanning/)). Blocking would need Ultimate security policies.
- **Use Trivy directly as the gate:** "By default, `Trivy` exits with code 0 even when security issues are detected. Use the `--exit-code` option". The example is `trivy image --exit-code 1 --severity CRITICAL ruby:2.4.0` ([Trivy options](https://trivy.dev/latest/docs/configuration/others/)).

```yaml
.scan:
  stage: scan
  image:
    name: aquasec/trivy:0.75.0@sha256:af6acf9a6b85dfe389a1941505c0ce9efef52a4719635e1a962f022a3d855daa
    entrypoint: [""]
  variables: { TRIVY_CACHE_DIR: .trivycache }
  cache: { key: trivy-db, paths: [.trivycache] }
  script:
    - trivy image --input $SVC.tar --exit-code 0 --severity LOW,MEDIUM,HIGH,CRITICAL --format cyclonedx --output sbom-$SVC.json
    - trivy image --input $SVC.tar --exit-code 1 --severity HIGH,CRITICAL --ignore-unfixed
  artifacts: { paths: ["sbom-$SVC.json"] }
```
The threshold is to fail on fixable HIGH/CRITICAL findings. A `.trivyignore` with justification comments lists accepted CVEs.

**Trivy supply-chain incident (CVE-2026-33634).** Advisory [GHSA-69fq-xp46-6x23](https://github.com/aquasecurity/trivy/security/advisories/GHSA-69fq-xp46-6x23):
- Trivy binary v0.69.4 (2026-03-19) was malicious.
- Docker Hub images 0.69.5–0.69.6 (2026-03-22) were malicious.
- `trivy-action` tags 0.0.1–0.34.2 were force-pushed to malicious commits.
- The recommendations are to pin by digest or SHA and to "rotate All Potentially Exposed Secrets".

Pin the Trivy image by digest (as above) and never use `:latest` in a job that can see `CI_REGISTRY_PASSWORD`.

#### Q5. GitOps tag bump, Image Updater, Argo CD on kind, promotion by tag

**CI-commits-tags pattern, same repo.** This is how the decision is worded. GitLab 18.4 made it clean:
- "You can configure your project to allow Git push requests that are authenticated with a CI/CD job token. This setting is turned off by default." It reached GA in GitLab 18.4.
- "**When you use a job token to push to the project, no CI/CD pipelines are triggered.**"
- "The job token has the same access permissions as the user who started the job."

The source for all three is [CI job token](https://docs.gitlab.com/ci/jobs/ci_job_token/). The bump commit therefore needs no `[skip ci]` and creates no loop. The fallbacks are:
- `[skip ci]` in the message: "add `[ci skip]` or `[skip ci]`, using any capitalization, to your commit message" ([pipelines](https://docs.gitlab.com/ci/pipelines/));
- `git push -o ci.skip`, which "Only affects branch pipelines, not merge request pipelines" ([push options](https://docs.gitlab.com/topics/git/commit/)).

Both still create an "empty pipeline … status is **Skipped**".

```yaml
bump-deploy:
  stage: release
  image: registry.access.redhat.com/ubi9/ubi-minimal:latest
  resource_group: gitops-bump            # serialize concurrent main pipelines
  needs:
    - { job: push:scoring, optional: true }
    - { job: push:case-summary, optional: true }
    - { job: push:monitoring, optional: true }
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
  script:
    - microdnf -y install git-core && curl -sSL -o /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 && chmod +x /usr/local/bin/yq
    - git config user.email "ci@fraud-ml-platform" && git config user.name "gitlab-ci"
    - git fetch origin $CI_DEFAULT_BRANCH && git checkout -B $CI_DEFAULT_BRANCH origin/$CI_DEFAULT_BRANCH
    - for s in $(cat built-services.txt); do yq -i ".${s}.image.tag = \"$IMAGE_TAG\"" deploy/images.yaml; done
    - git commit -am "deploy: images $IMAGE_TAG"
    - git push "https://gitlab-ci-token:${CI_JOB_TOKEN}@${CI_SERVER_HOST}/${CI_PROJECT_PATH}.git" HEAD:$CI_DEFAULT_BRANCH
    - git rev-parse HEAD > bump.sha
  artifacts: { paths: [bump.sha] }
```
- Same repo or separate config repo: a separate repo is the textbook pattern, since it keeps app history clean and gives separate permissions. The decision (one repo, `deploy/`) is fine for a demo and makes "CD ships DAGs + images together" (Q33) atomic. Say so in the interview.
- `main` is protected, so the job token's user must be allowed to push there (Maintainer).

**Argo CD Image Updater status:**
- The latest is v1.3.0 (2026-08-13). Configuration moved to an `ImageUpdater` CR (v1.1+) and legacy annotations are still read ([releases](https://github.com/argoproj-labs/argocd-image-updater/releases)).
- The README still says: "Argo CD Image Updater is under active development. We would not recommend it yet for _critical_ production workloads". It supports Helm/Kustomize apps and writes back via "Git commits" or the "Argo CD API" ([README](https://github.com/argoproj-labs/argocd-image-updater)).
- It would decouple bumps from CI, but it bumps on *any* new tag in the registry. That bypasses the model, LLM and Trivy gates unless only gated images get pushed. The CI-commit pattern keeps the gate-then-bump ordering explicit, so stay with the decision.

**Install on kind** (Argo CD v3.5.3; [getting started](https://argo-cd.readthedocs.io/en/stable/getting_started/)):

```bash
kubectl create namespace argocd
kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/v3.5.3/manifests/install.yaml
argocd admin initial-password -n argocd
kubectl port-forward svc/argocd-server -n argocd 8080:443
```
"The `--server-side` flag is required because some Argo CD CRDs (like ApplicationSet) exceed the 262KB annotation size limit." The URL is pinned to `v3.5.3` instead of `stable`. kind is installed per the [quick start](https://kind.sigs.k8s.io/docs/user/quick-start/): `curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.33.0/kind-linux-amd64`.

**Polling, not webhooks.** gitlab.com cannot reach a laptop cluster, so Argo CD polls. The default is `timeout.reconciliation: 120s` plus `timeout.reconciliation.jitter: 60s` ([argocd-cm.yaml](https://github.com/argoproj/argo-cd/blob/stable/docs/operator-manual/argocd-cm.yaml)). CI can force a refresh with the `argocd.argoproj.io/refresh` annotation (`normal` or `hard`). The docs say: "Indicates that app needs to be refreshed. Removed by application controller after app is refreshed." ([annotations](https://argo-cd.readthedocs.io/en/stable/user-guide/annotations-and-labels/))

**ApplicationSet vs app-of-apps.** Argo CD's bootstrapping guide says: "Our recommendation is to look at ApplicationSets" ([cluster bootstrapping](https://argo-cd.readthedocs.io/en/stable/operator-manual/cluster-bootstrapping/)). One ApplicationSet with a list generator, with one element per environment, gives two Applications. Their only differences are the revision and the values file. This follows the list-generator shape with `goTemplate: true` from the [docs](https://argo-cd.readthedocs.io/en/stable/operator-manual/applicationset/Generators-List/).

```yaml
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata: { name: fraud-platform, namespace: argocd }
spec:
  goTemplate: true
  goTemplateOptions: ["missingkey=error"]
  generators:
    - list:
        elements:
          - { env: staging, revision: main }
          - { env: prod,    revision: "v*" }      # latest semver git tag
  template:
    metadata: { name: 'fraud-{{.env}}' }
    spec:
      project: default
      source:
        repoURL: https://gitlab.com/<user>/fraud-ml-platform.git
        targetRevision: '{{.revision}}'
        path: deploy/charts/fraud-platform
        helm:
          valueFiles: [../../images.yaml, '../../envs/{{.env}}/values.yaml']
      destination: { server: https://kubernetes.default.svc, namespace: '{{.env}}' }
      syncPolicy:
        automated: { prune: true, selfHeal: true }
        syncOptions: [CreateNamespace=true]
```
App-of-apps is the alternative: one root Application pointing at `deploy/argocd/apps/` containing `staging.yaml` and `prod.yaml`. It is simpler to explain, but it duplicates the spec.

**Promotion by git tag** ([tracking strategies](https://argo-cd.readthedocs.io/en/stable/user-guide/tracking_strategies/)):
- "If a tag is specified, the manifests at the specified Git tag will be used to perform the sync comparison."
- "if you're using semantic versioning you can set the constraint in your service revision and Argo CD will get the latest version following the constraint rules." The examples include `1.*` and `*`.
- "Semver constraints … are **only matched against tags**, never branches."
- `tagPrefix` (for example `prod/`) can filter tags per env.

So prod tracks `v*`. Running `git tag v1.4.0 <bump-commit> && git push origin v1.4.0` promotes exactly the manifests and image tags that staging ran at that commit, with no extra commit. The alternative is a fixed `targetRevision: v1.4.0` changed by a promotion MR, which is a more explicit audit trail but makes the "git tag → prod" decision a two-step one.

**Waiting for staging in CI** (no Argo CD token needed with a read-only kubeconfig):

```bash
SHA=$(cat bump.sha)
kubectl -n argocd annotate application fraud-staging argocd.argoproj.io/refresh=normal --overwrite
until [ "$(kubectl -n argocd get application fraud-staging -o jsonpath='{.status.sync.revision}')" = "$SHA" ] && \
      [ "$(kubectl -n argocd get application fraud-staging -o jsonpath='{.status.health.status}')" = "Healthy" ]; do sleep 10; done
```
The `annotate` step needs `patch` on that one Application. Without it, polling picks up the change within about 3 minutes. Wrap the wait in a `timeout 900`. `argocd app wait fraud-staging --sync --health --timeout 600` is the CLI equivalent ([argocd app wait](https://argo-cd.readthedocs.io/en/stable/user-guide/commands/argocd_app_wait/)).

#### Q6. k6 and contract testing

**k6 thresholds and exit codes** ([thresholds](https://grafana.com/docs/k6/latest/using-k6/thresholds/)):
- When a threshold fails, "k6 would exit with a non-zero exit code".
- The exact code is `ThresholdsHaveFailed ExitCode = 99` ([exitcodes/codes.go](https://github.com/grafana/k6/blob/master/errext/exitcodes/codes.go)). Others include `ScriptException = 107` and `MarkedAsFailed = 110`.
- `abortOnFail` with `delayAbortEval` stops the run early.

**k6 v2 (2026-05-11)** brought these breaking changes ([v2.0.0 notes](https://github.com/grafana/k6/blob/master/release%20notes/v2.0.0.md)):
- "Removal of all long-deprecated CLI commands and flags: `k6 login`, `k6 pause`, `k6 resume`, `k6 scale`, `k6 status`, `--no-summary`, …". `--summary-mode=legacy` is also gone.
- Cloud non-threshold aborts "now return exit code `97` instead of `0`". The threshold abort stays `99`.
- The k6 images now have floating major tags, e.g. `grafana/k6:v1`.

Pre-2026 blog snippets using `--no-summary` will break.

```javascript
// tests/load/score.js
import http from 'k6/http';
import { check } from 'k6';
const BASE = __ENV.BASE_URL;                       // e.g. http://fraud-control-plane:30080
const P99_MS = __ENV.P99_BUDGET_MS || '50';
export const options = {
  scenarios: { steady: { executor: 'constant-arrival-rate', rate: 200, timeUnit: '1s',
                         duration: '2m', preAllocatedVUs: 50 } },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    http_req_duration: [{ threshold: `p(99)<${P99_MS}`, abortOnFail: true, delayAbortEval: '20s' }],
  },
};
const body = JSON.parse(open('./sample_txn.json'));
export default function () {
  const r = http.post(`${BASE}/score`, JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } });
  check(r, { '200': (x) => x.status === 200 });
}
```
```yaml
verify:k6:
  stage: verify
  tags: [kind]
  needs: [verify:staging-synced]
  image: { name: grafana/k6:2.3.0@sha256:9c2dee7f8ed74d317e4027c06a10f169b625638189de8d4555d0b3486a5aeb34, entrypoint: [""] }
  script:
    - k6 run --summary-export=k6-summary.json -e BASE_URL=http://fraud-control-plane:30080 -e P99_BUDGET_MS=$P99_BUDGET_MS tests/load/score.js
  artifacts: { when: always, paths: [k6-summary.json] }
```
- The `kind`-tagged runner's job container sits on the `kind` Docker network (Q2). It can reach a staging `NodePort` service at `<cluster>-control-plane:<nodePort>` without `kubectl port-forward`. Port-forward would add latency and distort p99.
- The more realistic alternative runs k6 *inside* the cluster as a `Job` in `staging`. CI then does `kubectl wait --for=condition=complete` and reads the logs. That needs create rights on Jobs in `staging`.
- The latency budget belongs in one place (a CI variable or `deploy/envs/staging/values.yaml`) and must match the SLO the scoring service documents.
- Note that `--summary-export` was not listed as removed in v2.0.0, but check it against `k6 run --help` on 2.3.0.

**Contract testing with Schemathesis** (4.29.1, 2026-10-03):
- CLI against staging ([quick start](https://schemathesis.readthedocs.io/en/stable/quick-start/)): `uvx schemathesis run http://fraud-control-plane:30080/openapi.json`. To target another host, `uvx schemathesis run ./openapi.yaml --url http://localhost:8000`.
- In-process for unit-test speed, verbatim from the [Python apps guide](https://schemathesis.readthedocs.io/en/stable/guides/python-apps/):
  ```python
  schema = schemathesis.openapi.from_asgi("/openapi.json", app)

  @schema.parametrize()
  def test_api(case):
      case.call_and_validate()
  ```
- Use both. The ASGI test runs in the `test` stage with no cluster, which fits Q21's "no full stack in CI". The CLI run against the deployed staging service is the Q34 "API contract tests in staging". Also commit the generated `openapi.json` and diff it in CI (`oasdiff` or a plain `git diff --exit-code`) to catch breaking schema changes before deploy.

### Recommendation

1. **Run every job on the self-hosted runner and disable instance runners.** 400 free minutes/month is the same quota for public and private projects; only the OSS program discounts it. Create three project runners with the new `glrt-` flow:
   - `fraud-docker`: unprotected, unprivileged, used for lint/test/build/scan;
   - `gpu`: protected, `gpus = "all"`;
   - `kind`: protected, `network_mode = "kind"`, read-only scoped kubeconfig mounted.

   Never enable `privileged` or socket binding.
2. **Pipeline:** `workflow:rules` (MR + main + `v*` tags) → lint (ruff+mypy) → test (pytest + coverage XML + the in-process Schemathesis test) → SonarQube Cloud with `sonar.qualitygate.wait=true` → per-service Buildah build (`rules:changes`, `needs`) to an OCI archive → Trivy gate (`--exit-code 1 --severity HIGH,CRITICAL --ignore-unfixed`, image pinned by digest) → Terraform checks → model and LLM gates (the `gpu` runner where needed) → push to `$CI_REGISTRY_IMAGE` → `bump-deploy` (job-token push to `main`, `resource_group`) → `verify` on the `kind` runner (wait for Argo sync of the bump SHA → Schemathesis CLI → k6 p99 gate). Tag pipelines run nothing heavy, because Argo CD picks up the tag itself.
3. **Images:**
   - `ubi9/ubi-minimal` + uv-managed CPython 3.13, multi-stage, `USER 1001`, group-0 permissions, port 8080.
   - Alternatively, switch to the Red Hat-built `ubi9/python-312` if "Red Hat Python" matters more than 3.13 parity (see Risks).
   - Build with Buildah, not Kaniko (archived 2025-06-03) and not DinD.
4. **GitOps:** keep the same-repo `deploy/` layout with a shared `deploy/images.yaml` that CI bumps. One Argo CD v3.5.3 ApplicationSet creates two Applications:
   - `staging` tracks `main`;
   - `prod` tracks semver tags `v*`. Promotion is `git tag vX.Y.Z <bump-sha>`.

   Skip Argo CD Image Updater (still "not recommended for critical production", and it would bypass the CI gates). Mention it as the alternative in the interview.
5. **SonarQube Cloud Free** on a public repo. Accept the fixed "Sonar way" gate (80% coverage on new code); custom gates need the Team plan. Skip self-hosted SonarQube, which costs RAM and has no MR analysis.

### Risks and gotchas

- **CONTRADICTS Q31/Q3 assumption — no UBI Python 3.13.** UBI ships 3.12 and 3.14 only (images and RPMs; verified locally on 2026-10-04). The uv-managed 3.13 interpreter is python-build-standalone, not a Red Hat RPM, so:
  - the "Red Hat" story becomes "UBI base OS" rather than "Red Hat-supported Python";
  - Trivy's OS-package scan will not track the interpreter itself through RHSA data.

  Pick one consciously: (a) UBI 9 minimal + uv 3.13 (parity with the dev machine), (b) `ubi9/python-312` with the project pinned to 3.12, or (c) `ubi9/python-314-minimal` with the project moved to 3.14.
- **SonarQube Cloud Free cannot customise the quality gate.** The "coverage gate" is Sonar way's 80% on *new code* only, not an overall threshold you choose. The Free plan analyses only the main branch, plus MRs that target main. A private repo would also cap at 50k LOC. If the repo goes private, re-check.
- **Free tier shows no Container Scanning results in MRs, and the GitLab template does not fail the pipeline.** Gate with Trivy directly. Remember the March 2026 Trivy compromise (CVE-2026-33634): pin by digest and keep registry credentials out of scan jobs where possible. Scanning the OCI archive with `--input` needs no registry credentials.
- **Unprivileged Buildah needs `seccomp:unconfined` and `apparmor:unconfined`** on this Docker 29.8 host (verified). `STORAGE_DRIVER=vfs` is slow and disk-hungry for large images (vLLM/LLM images). Prune the runner's Docker storage regularly, with ~600 GB free.
- **Public repo + personal runner.**
  - Every Developer-role member can run code on your machine.
  - Protected runners carry the GPU and the kubeconfig.
  - Restrict `allowed_images`.
  - Set CI/CD visibility to "Only project members" if logs could leak anything.
  - Never mount `~/.env`, `~/.kube/config` (admin) or the Docker socket into jobs.
  - Fork MRs run in the fork, so do not click "Run pipeline in parent project" on untrusted MRs.
- **Laptop availability is a hard dependency.** If the machine is off or kind is down, every pipeline stays pending. Q34's staging tests make `main` pipelines depend on a running kind cluster with Argo CD. Consider `allow_failure` or a manual `verify` job for days when the cluster is not running, and say so in the README.
- **`rules:changes` is always true for tag, scheduled, manual and new-branch pipelines.** Without `workflow:rules` and a `when: never` for tags, a `v*` tag would rebuild everything. Use `needs: optional: true` for skipped service jobs.
- **Concurrent bumps.** Two `main` pipelines can race on the bump push. Use `resource_group` plus fetch/rebase before commit. Tag the *bump* commit (the one whose `images.yaml` holds the new SHAs), not the merge commit, or prod gets the previous images.
- **The job-token push must be enabled in project settings** ("turned off by default", GA in 18.4). It runs with the permissions of the user who triggered the pipeline, and `main` protection must allow that user to push. If you fall back to `[skip ci]`, a tag later created on that commit may also be skipped. That is untested here, so verify it before relying on it.
- **Argo CD polling only (120s + up to 60s jitter);** gitlab.com webhooks cannot reach kind. Use the refresh annotation from CI. Avoid a branch and a tag with the same name, which can cause "constant reconciliation" ([tracking strategies](https://argo-cd.readthedocs.io/en/stable/user-guide/tracking_strategies/)).
- **Pin Argo CD.** v3.6.0 is at rc1 (2026-09-16). Use the `v3.5.3` manifest URL, not `stable`, so a demo rebuild is reproducible.
- **k6 v2 removed flags.** `--no-summary` → `--summary-mode=disabled`; `legacy` summary is gone. Exit code `99` = thresholds failed; treat any non-zero as a failed gate. Measuring p99 through `kubectl port-forward` would misstate latency, so use NodePort on the kind network or an in-cluster Job.
- **RAM.** kind + Argo CD + Kafka + Airflow + MLflow + vLLM, plus concurrent CI jobs (Buildah vfs builds, the Trivy DB, the SonarScanner JVM), on 31 GiB. Keep the runner at `concurrent = 2–3`.
- **Registration tokens are deprecated, with removal scheduled for GitLab 20.0.** Older tutorials using `--registration-token` and `--tag-list` will not survive; set tags and protection in the UI.

### Sources

- GitLab compute minutes: https://docs.gitlab.com/ci/pipelines/compute_minutes/
- GitLab instance-runner quota enforcement: https://docs.gitlab.com/ci/pipelines/instance_runner_compute_minutes/
- GitLab hosted Linux runners: https://docs.gitlab.com/ci/runners/hosted_runners/linux/
- GitLab storage quotas: https://docs.gitlab.com/user/storage_usage_quotas/
- GitLab CI YAML reference (rules:changes, compare_to, needs): https://docs.gitlab.com/ci/yaml/ (raw: https://gitlab.com/gitlab-org/gitlab/-/raw/master/doc/ci/yaml/_index.md)
- GitLab needs: https://docs.gitlab.com/ci/yaml/needs/
- GitLab job rules: https://docs.gitlab.com/ci/jobs/job_rules/
- GitLab CI/CD components: https://docs.gitlab.com/ci/components/
- GitLab components (sast, secret-detection, container-scanning, opentofu, dependency-scanning): https://gitlab.com/components
- GitLab predefined variables: https://docs.gitlab.com/ci/variables/predefined_variables/
- GitLab CI/CD variables (masking caveat): https://docs.gitlab.com/ci/variables/
- GitLab container registry: https://docs.gitlab.com/user/packages/container_registry/
- GitLab CI job token (push to repo, no pipelines triggered): https://docs.gitlab.com/ci/jobs/ci_job_token/
- GitLab skip pipelines: https://docs.gitlab.com/ci/pipelines/
- GitLab push options: https://docs.gitlab.com/topics/git/commit/
- GitLab pipeline settings (visibility): https://docs.gitlab.com/ci/pipelines/settings/
- GitLab MR pipelines from forks: https://docs.gitlab.com/ci/pipelines/merge_request_pipelines/
- GitLab Runner install: https://docs.gitlab.com/runner/install/linux-repository/
- GitLab Runner register: https://docs.gitlab.com/runner/register/
- GitLab Runner GPUs: https://docs.gitlab.com/runner/configuration/gpus/
- GitLab Runner advanced configuration: https://docs.gitlab.com/runner/configuration/advanced-configuration/
- GitLab Runner security: https://docs.gitlab.com/runner/security/
- GitLab configure runners (protected, tags): https://docs.gitlab.com/ci/runners/configure_runners/
- GitLab Runner releases: https://gitlab.com/gitlab-org/gitlab-runner/-/releases
- GitLab Docker builds (Buildah, socket binding): https://docs.gitlab.com/ci/docker/using_docker_build/
- GitLab Docker-in-Docker: https://docs.gitlab.com/ci/docker/docker_in_docker/
- GitLab kaniko page: https://docs.gitlab.com/ci/docker/using_kaniko/
- GitLab container scanning: https://docs.gitlab.com/user/application_security/container_scanning/
- uv GitLab integration: https://docs.astral.sh/uv/guides/integration/gitlab/
- uv Docker integration: https://docs.astral.sh/uv/guides/integration/docker/
- SonarQube Cloud plans: https://docs.sonarsource.com/sonarqube-cloud/administering-sonarcloud/managing-subscription/subscription-plans/
- SonarQube Cloud GitLab CI: https://docs.sonarsource.com/sonarqube-cloud/analyzing-source-code/ci-based-analysis/gitlab-ci.md
- SonarQube Cloud Python coverage: https://docs.sonarsource.com/sonarqube-cloud/analyzing-source-code/test-coverage/python-test-coverage.md
- SonarQube Cloud quality gates: https://docs.sonarsource.com/sonarqube-cloud/standards/managing-quality-gates/introduction-to-quality-gates.md
- SonarQube Cloud GitLab onboarding: https://docs.sonarsource.com/sonarqube-cloud/getting-started/gitlab.md
- SonarQube Community Build release notes: https://docs.sonarsource.com/sonarqube-community-build/server-update-and-maintenance/release-notes
- SonarQube Community Build GitLab integration: https://docs.sonarsource.com/sonarqube-community-build/devops-platform-integration/gitlab-integration/introduction.md
- sclorg s2i-python-container (Red Hat Python image matrix): https://github.com/sclorg/s2i-python-container
- Red Hat registry tag listings (queried): https://registry.access.redhat.com/v2/ubi9/python-314/tags/list (and ubi9/python-312, ubi10/python-314-minimal, etc.)
- OpenShift image guidelines (arbitrary UIDs): https://github.com/openshift/openshift-docs/blob/main/modules/images-create-guide-openshift.adoc
- Kaniko (archived): https://github.com/GoogleContainerTools/kaniko
- Trivy options (exit code, severity): https://trivy.dev/latest/docs/configuration/others/
- Trivy releases: https://github.com/aquasecurity/trivy/releases
- Trivy advisory GHSA-69fq-xp46-6x23 / CVE-2026-33634: https://github.com/aquasecurity/trivy/security/advisories/GHSA-69fq-xp46-6x23
- Argo CD releases: https://github.com/argoproj/argo-cd/releases
- Argo CD getting started: https://argo-cd.readthedocs.io/en/stable/getting_started/
- Argo CD tracking strategies: https://argo-cd.readthedocs.io/en/stable/user-guide/tracking_strategies/
- Argo CD ApplicationSet list generator: https://argo-cd.readthedocs.io/en/stable/operator-manual/applicationset/Generators-List/
- Argo CD cluster bootstrapping: https://argo-cd.readthedocs.io/en/stable/operator-manual/cluster-bootstrapping/
- Argo CD argocd-cm reference: https://github.com/argoproj/argo-cd/blob/stable/docs/operator-manual/argocd-cm.yaml
- Argo CD annotations: https://argo-cd.readthedocs.io/en/stable/user-guide/annotations-and-labels/
- Argo CD `app wait`: https://argo-cd.readthedocs.io/en/stable/user-guide/commands/argocd_app_wait/
- Argo CD Image Updater: https://github.com/argoproj-labs/argocd-image-updater and https://github.com/argoproj-labs/argocd-image-updater/releases
- kind quick start: https://kind.sigs.k8s.io/docs/user/quick-start/
- kind local registry: https://kind.sigs.k8s.io/docs/user/local-registry/
- kind `get kubeconfig --internal` source: https://github.com/kubernetes-sigs/kind/blob/main/pkg/cmd/kind/get/kubeconfig/kubeconfig.go
- kind releases: https://github.com/kubernetes-sigs/kind/releases
- k6 thresholds: https://grafana.com/docs/k6/latest/using-k6/thresholds/
- k6 exit codes source: https://github.com/grafana/k6/blob/master/errext/exitcodes/codes.go
- k6 v2.0.0 release notes: https://github.com/grafana/k6/blob/master/release%20notes/v2.0.0.md
- k6 releases: https://github.com/grafana/k6/releases
- Schemathesis quick start: https://schemathesis.readthedocs.io/en/stable/quick-start/
- Schemathesis Python apps (ASGI): https://schemathesis.readthedocs.io/en/stable/guides/python-apps/
- Schemathesis releases: https://github.com/schemathesis/schemathesis/releases
