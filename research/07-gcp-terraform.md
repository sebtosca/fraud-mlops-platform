## 07. GCP Terraform

Researched 2026-10-04. Every version, price and doc date below is as of that day. Prices are list prices in USD for **Iowa (us-central1)**, on-demand, 730 h per month, before taxes and discounts.

### Questions

1. Current Terraform / OpenTofu version (and licence context), `hashicorp/google` provider version; env layout (directories vs workspaces); Google's official modules vs raw resources for a portfolio repo.
2. GKE Standard vs Autopilot for Strimzi + Airflow + GPU inference; GPU node pool (L4/T4 machine types, `gpu_driver_installation_config`); Autopilot GPUs; private cluster; Workload Identity Federation for GKE in Terraform; KSA → GCP IAM for GCS and Secret Manager (ESO vs Secret Manager CSI add-on).
3. Cloud SQL for PostgreSQL in Terraform: private IP (PSA/PSC), Auth Proxy sidecar vs connectors, IAM database auth, dev vs HA settings.
4. GCS remote state; validating in CI without credentials (`init -backend=false` + `validate`); tflint + google ruleset; checkov findings and suppressions; `terraform test` / terratest offline.
5. Monthly cost estimate for the dev environment, and for the managed alternatives (Composer 3, Vertex AI endpoints, Managed Kafka).

### Findings

#### Q1. Versions, licence, layout, modules

**Versions (GitHub releases API, fetched 2026-10-04):**

| Tool | Latest | Released |
|---|---|---|
| Terraform (hashicorp/terraform) | **v1.16.5** | 2026-10-02 |
| OpenTofu | **v1.13.1** | 2026-10-01 |
| `hashicorp/google` provider | **v8.5.0** (8.0.0 released 2026-08-26) | 2026-09-29 |
| tflint | v0.64.0 | 2026-07-17 |
| tflint-ruleset-google | v0.40.0 | 2026-09-23 |
| checkov | 3.3.21 | 2026-09-30 |
| terraform-google-modules/kubernetes-engine | v45.0.0 | 2026-09-02 |
| terraform-google-modules/sql-db | v28.3.0 | 2026-09-14 |
| terraform-google-modules/network | v18.3.0 | 2026-09-14 |
| terratest | modules/core/v2.0.0 | 2026-09-23 |
| cloud-sql-proxy (git tag) | v2.26.0 | — |
| external-secrets (git tag) | v2.11.0 | — |

Sources: https://github.com/hashicorp/terraform/releases, https://github.com/opentofu/opentofu/releases, https://github.com/hashicorp/terraform-provider-google/releases, https://github.com/terraform-linters/tflint/releases, https://github.com/terraform-linters/tflint-ruleset-google/releases, https://github.com/bridgecrewio/checkov/releases, https://github.com/terraform-google-modules/terraform-google-kubernetes-engine/releases, https://github.com/terraform-google-modules/terraform-google-sql-db/releases, https://github.com/terraform-google-modules/terraform-google-network/releases, https://github.com/gruntwork-io/terratest/releases

**Licence context.** On 2023-08-10 HashiCorp announced it was "changing its source code license from Mozilla Public License v2.0 (MPL 2.0) to the Business Source License (BSL, also known as BUSL) v1.1 on all future releases of HashiCorp products", while "HashiCorp APIs, SDKs, and almost all other libraries will remain MPL 2.0" (https://www.hashicorp.com/en/blog/hashicorp-adopts-business-source-license). The HashiCorp licence FAQ allows internal and non-competitive use; the restriction is on building "a product that is competitive with HashiCorp" (https://www.hashicorp.com/en/license-faq). A portfolio or interview repo is clearly allowed. OpenTofu is "licensed under MPL-2.0 and governed by the Linux Foundation" and presents itself as "a drop-in replacement for Terraform" (https://opentofu.org/). The google provider is itself MPL-2.0 and is mirrored in the OpenTofu registry, so both CLIs can run this code. **Recommendation:** use Terraform 1.16.x, since that is what the job ads name. Add a README line saying the code is OpenTofu-compatible and why it matters (BSL), and keep `required_version = ">= 1.9"` so `tofu` works too. The project is not tested on OpenTofu in CI.

**Provider 8.0 breaking changes that touch this design** (https://github.com/hashicorp/terraform-provider-google/blob/main/website/docs/guides/version_8_upgrade.html.markdown):
- `google_container_cluster`: "`enable_components` within `logging_config` and `monitoring_config` have been converted from `list` to `set`".
- `google_container_node_pool`: `name_prefix` max length went from 14 to 31.
- `google_secret_manager_secret_version`: "`secret_data_wo_version` field has changed type from `Integer` to `String`", and "`secret_data_wo` and `secret_data_wo_version` are now linked with `RequiredWith`". If secret values are ever written, use write-only `secret_data_wo` with `secret_data_wo_version = "1"`. The recommended path is to create only the secret containers and leave values out of Terraform state.
- Pinning advice in the same guide: `version = "~> 8.0.0"` style constraints. Google's root-module guide says to "Pin to minor versions" (https://docs.cloud.google.com/docs/terraform/best-practices/root-modules, updated 2026-09-30).

**Envs: directories, not workspaces.** Both vendors agree:
- HashiCorp: "CLI workspaces within a working directory use the same backend, so they are not a suitable isolation mechanism for this scenario" [separate credentials and access controls]. The alternative they give is to "use one or more re-usable modules to represent the common elements and then represent each instance as a separate configuration … in the context of a different backend" (https://developer.hashicorp.com/terraform/cli/workspaces).
- Google: "Use only the default workspace"; "Don't include more than 100 resources (and ideally only a few dozen) in a single state". The recommended tree is `modules/<service>/{main,variables,outputs,provider}.tf` plus `environments/{dev,qa,prod}/{backend.tf,main.tf}` (https://docs.cloud.google.com/docs/terraform/best-practices/root-modules, updated 2026-09-30).

Proposed layout (matches Q26):

```
infra/terraform/
├── modules/
│   ├── network/          # VPC, subnet + secondary ranges, Cloud NAT, PSA range
│   ├── gke/              # cluster, system pool, GPU pool
│   ├── cloudsql/         # instance, dbs, IAM users
│   ├── storage/          # GCS buckets (mlflow-artifacts, data-snapshots)
│   ├── registry/         # Artifact Registry docker repo
│   └── workload-iam/     # GSAs/KSA principals, Secret Manager secrets + bindings
├── environments/
│   ├── dev/  {backend.tf, main.tf, variables.tf, terraform.tfvars, versions.tf}
│   └── prod/ {backend.tf, main.tf, variables.tf, terraform.tfvars, versions.tf}
├── tests/                 # *.tftest.hcl with mock_provider (see Q4)
└── .tflint.hcl
```

**Official modules vs raw resources.** Checked against `versions.tf` at each module's tag:

| Module | google constraint | Works with provider 8.5? |
|---|---|---|
| kubernetes-engine v45.0.0 (root and `modules/private-cluster`) | `">= 7.39.0, < 8"` | **No** |
| sql-db v28.3.0 `modules/postgresql` | `">= 7.22, < 9"` | Yes |
| network v18.3.0 | `">= 4.64, < 9"` | Yes |

Sources: https://github.com/terraform-google-modules/terraform-google-kubernetes-engine/blob/v45.0.0/versions.tf, https://github.com/terraform-google-modules/terraform-google-sql-db/blob/v28.3.0/modules/postgresql/versions.tf, https://github.com/terraform-google-modules/terraform-google-network/blob/v18.3.0/versions.tf

Trade-offs for a portfolio repo:
- **Raw resources (recommended).** An interviewer can read every GKE/Cloud SQL decision line by line (Workload Identity, private nodes, GPU drivers, PSA). Static checks (checkov, tflint) see the real resources without `--download-external-modules`. The latest provider (8.x) can be used. Wrapping these in a few small local modules shows module design. The cost is more lines to write and no upstream hardening for free.
- **terraform-google-modules.** These are battle-tested and are what many real platform teams use. They are large, opaque, variable-heavy interfaces, though. The GKE module currently forces provider `< 8`, which would pin the whole root to 7.x. Checkov scans them only if it downloads external modules.
- **Middle ground.** Use `terraform-google-modules/network` (fine with 8.x) and write GKE and Cloud SQL as raw resources. Mention in the README that a real team might adopt the GKE module once it supports 8.x.

#### Q2. GKE: Standard vs Autopilot, GPU pool, private cluster, Workload Identity, secrets

**Standard vs Autopilot.** The feature comparison (updated 2026-10-02) says:
- Autopilot: you "pay based on actual Pod resource requests". Standard: "you pay for node capacity regardless of whether Pods use the resources".
- Standard gives full node control. Autopilot enforces built-in security constraints, with custom node shapes available through "Custom ComputeClasses".
- "After you create a Standard cluster, you can run some workloads in Autopilot mode" via "an Autopilot ComputeClass in your Standard cluster".

Source: https://docs.cloud.google.com/kubernetes-engine/docs/resources/autopilot-standard-feature-comparison

Autopilot does support GPUs: `nodeSelector: cloud.google.com/gke-accelerator: nvidia-l4` plus `resources.limits: nvidia.com/gpu`. "Autopilot automatically installs the default NVIDIA drivers", and "The Autopilot node-based billing model applies to GPU Pods". L4 and T4 are both supported (https://docs.cloud.google.com/kubernetes-engine/docs/how-to/autopilot-gpus, updated 2026-10-02).

The cluster management fee is the same for both modes: "A flat cluster management fee of $0.10 per cluster per hour … irrespective of the mode of operation". The free tier is "$74.40 in monthly credits per billing account, which is equivalent to one free Autopilot or zonal Standard cluster per month" (https://cloud.google.com/kubernetes-engine/pricing).

**Choice: Standard.** ADR-0003 and Q26 already say "GPU node pool", which is a Standard concept. A dedicated GPU node pool with scale-to-zero autoscaling is the most legible pattern in an interview. Strimzi (StatefulSet-like broker pods with PVCs) and Airflow run unchanged on Standard. Autopilot would also work for this workload and is not a blocker. It belongs in the build-vs-buy paragraph ("Autopilot removes node ops; we chose Standard to show node-pool design, GPU drivers and taints explicitly").

**GPU node pool.** The GPU doc (https://docs.cloud.google.com/kubernetes-engine/docs/how-to/gpus, updated 2026-10-02) says:
- L4 requires a G2 machine type, e.g. `g2-standard-4`. T4 works with N1 machine types.
- `gpu-driver-version` takes `default` ("Install the default driver version for your node GKE version"), `latest` ("Container-Optimized OS only") or `disabled`.

Provider doc (v8.5.0, https://github.com/hashicorp/terraform-provider-google/blob/v8.5.0/website/docs/r/container_cluster.html.markdown): `gpu_driver_version` accepts `"GPU_DRIVER_VERSION_UNSPECIFIED"`, `"INSTALLATION_DISABLED"`, `"DEFAULT"` and `"LATEST"`. Note that "Before GKE `1.30.1-gke.1156000`, the default value is to not install any GPU driver."

For a ~7–8B model, an L4 (24 GB) matches the local RTX 3090 (24 GB) in ADR-0004, so the same quantised model and vLLM args carry over. A T4 (16 GB) would need a smaller or more heavily quantised config. Prefer `g2-standard-8` (8 vCPU / 32 GiB), which leaves headroom for vLLM CPU-side work. `g2-standard-4` (16 GiB) is the cheapest.

**Private cluster: current naming.** The network-isolation concept page (updated 2026-09-30) no longer frames clusters as binary private or public: "Clusters with or without external endpoints all share the same architecture". It also says "You can enable private nodes at an individual cluster level or at the node pool (for Standard)…", and recommends the DNS-based endpoint (https://docs.cloud.google.com/kubernetes-engine/docs/concepts/private-cluster-concept). The provider exposes this as `control_plane_endpoints_config { dns_endpoint_config {...} ip_endpoints_config {...} }`, and `private_cluster_config.enable_private_nodes` remains. The provider warns: "it's recommended that you omit the block entirely if the field is not set to `true`" (container_cluster doc above). Private nodes need Cloud NAT to pull public images, for example Strimzi and vLLM from quay.io/docker.io, unless everything is mirrored to Artifact Registry.

**Workload Identity Federation for GKE.** This is the current product name. The principal format is `principal://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/PROJECT_ID.svc.id.goog/subject/ns/NAMESPACE/sa/KSA_NAME`. Node pools need `--workload-metadata=GKE_METADATA`. The alternative is linking a KSA to an IAM service account with the annotation `iam.gke.io/gcp-service-account=…` and `roles/iam.workloadIdentityUser` (https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity, updated 2026-10-02). In Terraform: `workload_identity_config { workload_pool = "${project_id}.svc.id.goog" }` on the cluster, and `node_config { workload_metadata_config { mode = "GKE_METADATA" } }` on pools.

Per-service limitations for direct federated principals (https://docs.cloud.google.com/iam/docs/federated-identity-supported-services, updated 2026-09-30):
- **Cloud Storage:** "Identity federation with all Cloud Storage APIs is supported only for uniform bucket-level access buckets" and "identity federation users and workloads cannot generate signed URLs." MLflow features that need signed URLs (e.g. proxied multipart upload) would need the KSA→GSA impersonation path.
- **Secret Manager:** "No known limitations".
- **Cloud SQL:** the API has "No known limitations". IAM *database* users are typed `CLOUD_IAM_SERVICE_ACCOUNT` and named from a service-account email (sql_user doc: `trimsuffix(google_service_account.x.email, ".gserviceaccount.com")`, https://github.com/hashicorp/terraform-provider-google/blob/v8.5.0/website/docs/r/sql_user.html.markdown). My inference is that Postgres IAM auth needs a GSA plus a KSA→GSA link for MLflow, Airflow and the scoring service.

**Recommended mapping:** use one GSA per workload that touches Cloud SQL or GCS (mlflow, airflow, scorer), linked via `roles/iam.workloadIdentityUser`. Use direct `principal://` bindings for Secret-Manager-only consumers such as External Secrets Operator. tflint-ruleset-google's IAM member rules accept both `principal://` and `principalSet://` (https://github.com/terraform-linters/tflint-ruleset-google/blob/v0.40.0/rules/google_project_iam_member_invalid_member.go).

**Secrets into pods, three options:**
1. **Secret Manager add-on for GKE (managed CSI).** Enabled with `--enable-secret-manager` or Terraform `secret_manager_config { enabled = true }`. It uses `SecretProviderClass` `secrets-store.csi.x-k8s.io/v1` with provider `gke`, and IAM `roles/secretmanager.secretAccessor` on the principal. "The Secret Manager add-on doesn't support the Sync as Kubernetes Secret feature" (https://docs.cloud.google.com/secret-manager/docs/secret-manager-managed-csi-component, updated 2026-09-30). Secrets mount as files only. A separate `secret_sync_config { enabled = true }` cluster field exists for the newer "Sync as K8s secret" feature (provider doc above).
2. **External Secrets Operator** (v2.11.0). This is a `SecretStore`/`ClusterSecretStore` with `provider.gcpsm`. ESO "resolves credentials in this order: static service account JSON …, GKE Workload Identity (`auth.workloadIdentity`), … then Application Default Credentials", and with core-controller auth `gcpsm: {}` is enough (https://external-secrets.io/latest/provider/google-secrets-manager/). It produces ordinary Kubernetes Secrets.
3. **Recommendation: ESO.** The Airflow, MLflow and Strimzi Helm charts all consume Kubernetes Secrets. Locally those come from `.env` (Q22). On GKE, an `ExternalSecret` creates same-named Secrets from Secret Manager, so the charts in `deploy/` are untouched. That keeps the "same charts on kind and GKE" story in ADR-0003. Mention the managed add-on as the zero-operator alternative.

**HCL: cluster, system pool, GPU pool, WI bindings.** I wrote this against the v8.5.0 resource docs. It has not been run through `terraform validate`.

```hcl
# modules/gke/main.tf
resource "google_container_cluster" "this" {
  name     = var.name
  location = var.location            # zone for dev (free-tier credit), region for prod
  project  = var.project_id

  network    = var.network_id
  subnetwork = var.subnetwork_id

  remove_default_node_pool = true    # provider docs: separately managed node pools "recommended"
  initial_node_count       = 1
  deletion_protection      = var.deletion_protection

  release_channel { channel = "REGULAR" }                     # CKV_GCP_70
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"           # Workload Identity Federation for GKE
  }
  ip_allocation_policy {                                      # VPC-native, CKV_GCP_23
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }
  datapath_provider = "ADVANCED_DATAPATH"                     # Dataplane V2 (built-in NetworkPolicy)
  network_policy { enabled = false }                          # needed for CKV_GCP_12 to pass with DPv2

  private_cluster_config {
    enable_private_nodes = true                               # CKV_GCP_25 / CKV_GCP_64
  }
  control_plane_endpoints_config {
    dns_endpoint_config { allow_external_traffic = true }     # IAM-authenticated DNS endpoint
    ip_endpoints_config { enabled = false }                   # no public IP endpoint
  }

  secret_manager_config { enabled = false }                   # using ESO instead (see ADR)
  enable_intranode_visibility = true                          # CKV_GCP_61
  enable_shielded_nodes       = true                          # CKV_GCP_71
  resource_labels             = var.labels                    # CKV_GCP_21
}

resource "google_container_node_pool" "system" {
  name     = "system"
  cluster  = google_container_cluster.this.id
  location = var.location
  autoscaling {
    min_node_count = 1
    max_node_count = 3
  }
  management {
    auto_repair  = true    # CKV_GCP_9
    auto_upgrade = true    # CKV_GCP_10
  }
  node_config {
    machine_type    = "e2-standard-4"
    image_type      = "COS_CONTAINERD"                         # CKV_GCP_22
    service_account = var.node_sa_email                        # minimal node SA, not default compute SA
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    workload_metadata_config { mode = "GKE_METADATA" }         # CKV_GCP_69
    shielded_instance_config {
      enable_secure_boot          = true                       # CKV_GCP_68
      enable_integrity_monitoring = true                       # CKV_GCP_72
    }
    labels = var.labels
  }
}

resource "google_container_node_pool" "gpu" {
  name     = "gpu-l4"
  cluster  = google_container_cluster.this.id
  location = var.location
  node_locations = var.gpu_zones                              # zones that actually have L4 capacity
  autoscaling {
    min_node_count = 0                                        # scale to zero when vLLM is idle
    max_node_count = 1
  }
  management {
    auto_repair  = true
    auto_upgrade = true
  }
  node_config {
    machine_type = "g2-standard-8"                            # L4 requires G2; T4 alt: n1-standard-4 + nvidia-tesla-t4
    spot         = var.gpu_spot
    image_type   = "COS_CONTAINERD"
    disk_size_gb = 200                                        # model weights + vLLM image
    guest_accelerator {
      type  = "nvidia-l4"
      count = 1
      gpu_driver_installation_config {
        gpu_driver_version = "LATEST"                         # or "DEFAULT"
      }
    }
    service_account = var.node_sa_email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    workload_metadata_config { mode = "GKE_METADATA" }
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
    labels = merge(var.labels, { workload = "llm" })
  }
}
```

```hcl
# modules/workload-iam/main.tf: KSA -> GSA (for Cloud SQL IAM auth + GCS) and direct principal (ESO)
data "google_project" "this" { project_id = var.project_id }

locals {
  wi_principal = "principal://iam.googleapis.com/projects/${data.google_project.this.number}/locations/global/workloadIdentityPools/${var.project_id}.svc.id.goog/subject"
}

resource "google_service_account" "mlflow" {
  project    = var.project_id
  account_id = "mlflow"
}

resource "google_service_account_iam_member" "mlflow_wi" {
  service_account_id = google_service_account.mlflow.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[mlflow/mlflow]"   # [ns/ksa]
}
# KSA annotation in the Helm values: iam.gke.io/gcp-service-account: mlflow@PROJECT.iam.gserviceaccount.com

resource "google_storage_bucket_iam_member" "mlflow_artifacts" {
  bucket = var.mlflow_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.mlflow.email}"
}

resource "google_project_iam_member" "mlflow_sql" {
  for_each = toset(["roles/cloudsql.client", "roles/cloudsql.instanceUser"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.mlflow.email}"
}

# Secret Manager: direct federated principal for the ESO controller KSA
resource "google_secret_manager_secret" "app" {
  for_each  = toset(var.secret_ids)      # values are added out-of-band, never in state
  project   = var.project_id
  secret_id = each.value
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "eso" {
  for_each  = google_secret_manager_secret.app
  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "${local.wi_principal}/ns/external-secrets/sa/external-secrets"
}
```

#### Q3. Cloud SQL for PostgreSQL

**Private IP.** Private services access (PSA) needs the Service Networking API, an allocated range and a peering connection. Google's own HCL (https://docs.cloud.google.com/sql/docs/postgres/configure-private-ip, updated 2026-09-30):

```hcl
resource "google_compute_global_address" "private_ip_address" {
  name          = "private-ip-address"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.peering_network.id
}

resource "google_service_networking_connection" "default" {
  network                 = google_compute_network.peering_network.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address.name]
}

resource "google_sql_database_instance" "default" {
  settings {
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.peering_network.id
    }
  }
}
```

PSC is the alternative: `ip_configuration { psc_config { psc_enabled = true, allowed_consumer_projects = [...] } ipv4_enabled = false }`, shown in the provider doc examples (https://github.com/hashicorp/terraform-provider-google/blob/v8.5.0/website/docs/r/sql_database_instance.html.markdown). PSC avoids VPC peering and range planning but adds an endpoint and forwarding rule. **Use PSA.** It is the canonical, simpler pattern for a single-VPC GKE plus Cloud SQL setup.

**Edition and tier gotcha (important).** From the provider doc: "instances with `database_version` `POSTGRES_16` or later default to `ENTERPRISE_PLUS`". In addition, "shared-core and custom tiers such as `db-g1-small`, `db-f1-micro`, and `db-custom-*` require `edition = "ENTERPRISE"`". If you omit it, creation "fails at create time with `Invalid Tier (...) for (ENTERPRISE_PLUS) Edition`". Cloud SQL's default major version is now "PostgreSQL 18 (default)" (https://docs.cloud.google.com/sql/docs/db-versions, updated 2026-09-30). checkov CKV_GCP_79 expects `POSTGRES_18` (checkov 3.3.21 `CloudSqlMajorVersion.py`). Use `database_version = "POSTGRES_18"` and `edition = "ENTERPRISE"` explicitly. `terraform validate` will **not** catch a missing `edition`. It fails only at apply, and this repo never applies.

**Connecting from GKE.** "The Cloud SQL Auth Proxy is the recommended way to connect to Cloud SQL, even when using private IP", and "We recommend running the Cloud SQL Auth Proxy in a `sidecar` pattern". For IAM auth, use `--auto-iam-authn` and grant `roles/cloudsql.client` + `roles/cloudsql.instanceUser` (https://docs.cloud.google.com/sql/docs/postgres/connect-kubernetes-engine, updated 2026-09-30). Language connectors are the other option. For Python, `cloud-sql-python-connector` with SQLAlchemy would mean code changes in MLflow and Airflow. **The sidecar keeps the charts portable:** apps still connect to `localhost:5432`, exactly like local Postgres on kind. Run it as a Kubernetes native sidecar (an `initContainers` entry with `restartPolicy: Always`) so Jobs and Airflow task pods terminate cleanly. Proxy image `gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.26.0` with flags `--private-ip --auto-iam-authn --structured-logs <INSTANCE_CONNECTION_NAME>`.

**IAM database auth.** This needs the database flag `cloudsql.iam_authentication`. Service-account usernames drop the `.gserviceaccount.com` suffix. Automatic IAM auth "lets you hand off requesting and managing access tokens to an intermediary Cloud SQL connector", which the Auth Proxy and the Go, Java and Python connectors support. After creating users, "Use the PostgreSQL GRANT command to grant database privileges" (https://docs.cloud.google.com/sql/docs/postgres/iam-authentication, updated 2026-09-30). The GRANTs are SQL, not Terraform, so a migration job or a documented manual step is needed.

**Dev vs prod HCL:**

```hcl
resource "google_sql_database_instance" "this" {
  name                = var.name
  project             = var.project_id
  region              = var.region
  database_version    = "POSTGRES_18"
  deletion_protection = var.env == "prod"
  depends_on          = [google_service_networking_connection.psa]

  settings {
    edition           = "ENTERPRISE"                          # REQUIRED for db-custom-* on PG16+
    tier              = var.env == "prod" ? "db-custom-2-7680" : "db-custom-1-3840"
    availability_type = var.env == "prod" ? "REGIONAL" : "ZONAL"   # REGIONAL = HA (2x vCPU/RAM price)
    disk_type         = "PD_SSD"
    disk_size         = 20
    disk_autoresize   = true
    deletion_protection_enabled = var.env == "prod"           # API-level guard (all surfaces)

    ip_configuration {
      ipv4_enabled    = false                                  # CKV_GCP_11 / CKV_GCP_60
      private_network = var.network_id
      ssl_mode        = "ENCRYPTED_ONLY"                       # CKV_GCP_6
    }
    backup_configuration {
      enabled                        = true                    # CKV_GCP_14
      point_in_time_recovery_enabled = var.env == "prod"
      start_time                     = "02:00"
    }
    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"
    }
    # Postgres logging flags that satisfy CKV_GCP_51..57/108/109 (see Q4 for the ones to suppress)
    database_flags {
      name  = "log_checkpoints"
      value = "on"
    }
    database_flags {
      name  = "log_connections"
      value = "on"
    }
    database_flags {
      name  = "log_disconnections"
      value = "on"
    }
    database_flags {
      name  = "log_lock_waits"
      value = "on"
    }
    database_flags {
      name  = "log_temp_files"
      value = "0"
    }
    database_flags {
      name  = "log_min_duration_statement"
      value = "-1"
    }
    insights_config { query_insights_enabled = var.env == "prod" }
    maintenance_window {
      day  = 7
      hour = 3
    }
  }
}

resource "google_sql_database" "db" {
  for_each = toset(["mlflow", "airflow", "fraud"])   # fraud = predictions, labels, monitoring
  name     = each.value
  instance = google_sql_database_instance.this.name
}

resource "google_sql_user" "iam_sa" {
  for_each = var.iam_service_account_emails           # mlflow, airflow, scorer GSAs
  name     = trimsuffix(each.value, ".gserviceaccount.com")
  instance = google_sql_database_instance.this.name
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
}
```

Provider notes (sql_database_instance doc): Terraform-level `deletion_protection` "only protects instances from deletion within Terraform", so use `settings.deletion_protection_enabled` for the API-level guard. "Shared CPU machine types (db-f1-micro and db-g1-small) are not covered by the Cloud SQL SLA" (https://cloud.google.com/sql/pricing).

#### Q4. Remote state, credential-less CI, linters, tests

**GCS backend.** Example from the docs: `backend "gcs" { bucket = "tf-state-prod"  prefix = "terraform/state" }`. "It is highly recommended that you enable Object Versioning on the GCS bucket". The backend "supports state locking", optionally with `kms_encryption_key`. Credentials come from `credentials`, `GOOGLE_BACKEND_CREDENTIALS`, `GOOGLE_CREDENTIALS` or ADC (https://developer.hashicorp.com/terraform/language/backend/gcs). One bucket can hold both envs under `prefix = "fraud-ml/dev"` and `"fraud-ml/prod"`. Better isolation is one state bucket per env project. The state bucket itself is created by a tiny `bootstrap/` root with local state, which is the usual chicken-and-egg answer.

```hcl
# environments/dev/backend.tf
terraform {
  backend "gcs" {
    bucket = "fraud-ml-tfstate-dev"   # created by infra/terraform/bootstrap (versioning on, UBLA, PAP enforced)
    prefix = "fraud-ml/dev"
  }
}

# environments/dev/versions.tf
terraform {
  required_version = ">= 1.9"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.5"
    }
  }
}
```

**Validate without credentials.** The docs say: "It does not validate remote services, such as remote state or provider APIs". "Validation requires an initialized working directory with any referenced plugins and modules installed". "To initialize a working directory for validation without accessing any configured backend, use: `terraform init -backend=false`" (https://developer.hashicorp.com/terraform/cli/commands/validate). `validate` does not configure the provider, so no Google credentials are needed. Network access to registry.terraform.io is still needed to download the provider. Keep `provider "google" {}` free of data sources in a way that would matter. Data sources are only read at plan, so `validate` is fine. `-json` gives machine-readable output.

Not runnable: `terraform plan` (it calls Google APIs and reads the backend). Also not runnable: tflint "Deep Checking", which invokes APIs (https://github.com/terraform-linters/tflint-ruleset-google/blob/v0.40.0/docs/rules/README.md). Keep deep checking off.

**tflint.** The plugin needs "TFLint v0.46+":

```hcl
# infra/terraform/.tflint.hcl
plugin "terraform" {
  enabled = true
  preset  = "recommended"
}
plugin "google" {
  enabled = true
  version = "0.40.0"
  source  = "github.com/terraform-linters/tflint-ruleset-google"
}
```

It catches invalid machine types (`google_container_node_pool_invalid_machine_type`, ERROR), invalid IAM members, and 100+ Magic-Modules-generated enum checks (https://github.com/terraform-linters/tflint-ruleset-google/blob/v0.40.0/README.md). Note: "custom machine types cannot be detected correctly" (rules README). `tflint --init` downloads the plugin from GitHub. Set `GITHUB_TOKEN` in CI to avoid API rate limits. I hit the unauthenticated limit while researching this.

**checkov 3.3.21.** Typical findings on this GKE + Cloud SQL + GCS design, from the checkov policy index (https://github.com/bridgecrewio/checkov/blob/3.3.21/docs/5.Policy%20Index/terraform.md) and check source:

| Check | What | Handling |
|---|---|---|
| CKV_GCP_12 | Network policy | Passes only if `network_policy.enabled = true`, or `network_policy { enabled = false }` **plus** `datapath_provider = "ADVANCED_DATAPATH"` (source `GKENetworkPolicyEnabled.py`). Keep the explicit block. |
| CKV_GCP_18 / CKV_GCP_20 | Public control plane / master authorized networks | CKV_GCP_18 only fails on `0.0.0.0/0` in authorized networks. CKV_GCP_20 wants `master_authorized_networks_config`. With the IP endpoint disabled and the DNS endpoint IAM-gated, **skip with justification**. |
| CKV_GCP_24 | PodSecurityPolicy | Returns UNKNOWN unless `min_master_version < 1.25` (PSP was "removed … >= 1.25.0"). Not a finding. |
| CKV_GCP_25 / CKV_GCP_64 | Private cluster / private nodes | Both inspect `private_cluster_config`. Keep `enable_private_nodes = true` there, not only at node-pool level. |
| CKV_GCP_65 | RBAC via Google Groups | Skip: needs a Workspace domain. |
| CKV_GCP_66 | Binary Authorization | Enable `binary_authorization { evaluation_mode = "PROJECT_SINGLETON_POLICY_ENFORCE" }` or skip (images signed? Trivy-scanned only). |
| CKV_GCP_9/10/22/68/69/70/71/72/21/61/23/123 | Node repair/upgrade, COS, shielded, metadata server, release channel, labels, intranode visibility, alias IPs, no inline node pools | Set as in the Q2 HCL. |
| CKV_GCP_6/11/14/60/79 | SSL, public IP, backups, latest major (`POSTGRES_18`) | Set as in the Q3 HCL. |
| CKV_GCP_51–57, 108–111 | Postgres log flags, pgAudit, log statements | Set the logging flags. Skip CKV_GCP_110 (pgAudit) and CKV_GCP_111 (log all statements) in dev with justification: cost/noise, no PII beyond hashed card numbers. |
| CKV_GCP_29/78/114/62 | GCS UBLA, versioning, public access prevention, access logs | Set UBLA, versioning and PAP (UBLA is also required for federated identities, see Q2). Skip CKV_GCP_62 in dev. |
| CKV_GCP_84 | Artifact Registry CSEK/CMEK | Skip in dev with justification (Google-managed keys). Prod could add KMS. |

Suppression syntax: "`#checkov:skip=<check_id>:<suppression_comment>`" inside the resource block. CLI: `--skip-check`, `--soft-fail`, `--config-file`, `-o gitlab_sast`, `-o junitxml` (https://www.checkov.io/2.Basics/Suppressing%20and%20Skipping%20Policies.html, https://www.checkov.io/2.Basics/CLI%20Command%20Reference.html). `--download-external-modules` must be set to scan registry modules. Not needed with raw resources.

```hcl
resource "google_container_cluster" "this" {
  #checkov:skip=CKV_GCP_20:IP endpoint disabled; access only via IAM-authenticated DNS endpoint
  #checkov:skip=CKV_GCP_65:No Google Workspace domain in a portfolio project; RBAC via IAM roles
  ...
}
```

**Native `terraform test` offline.** "Test mocking is available in Terraform v1.7.0 and later". Mocked computed values: "Numbers will be 0. Booleans will be false. Strings will be a random 8 character alphanumeric string". The docs say this allows you to test "without creating infrastructure or requiring credentials" (https://developer.hashicorp.com/terraform/language/tests/mocking). This is the one *behavioural* test runnable in CI with no GCP project. Assert on module logic: dev is ZONAL, prod is REGIONAL, the GPU pool scales from 0, Cloud SQL has no public IP, and `edition = "ENTERPRISE"` is always set (which guards the PG18 gotcha above).

```hcl
# infra/terraform/tests/cloudsql.tftest.hcl
mock_provider "google" {
  override_data {
    target = data.google_project.this
    values = { number = "123456789012" }
  }
}

variables {
  project_id = "fraud-ml-dev"
  region     = "europe-west1"
  env        = "dev"
}

run "dev_is_zonal_private_enterprise" {
  command = plan
  module { source = "./modules/cloudsql" }

  assert {
    condition     = google_sql_database_instance.this.settings[0].availability_type == "ZONAL"
    error_message = "dev Cloud SQL must be ZONAL"
  }
  assert {
    condition     = google_sql_database_instance.this.settings[0].ip_configuration[0].ipv4_enabled == false
    error_message = "Cloud SQL must not have a public IP"
  }
  assert {
    condition     = google_sql_database_instance.this.settings[0].edition == "ENTERPRISE"
    error_message = "db-custom tiers require ENTERPRISE edition on PG16+"
  }
}
```

Terratest (modules/core v2.0.0) applies real infrastructure and needs credentials. It is **not** usable under ADR-0003. Mention it only as "what a real team would add".

**GitLab CI job** (Terraform-checks stage from Q27). Image tags are assumed to follow the release versions above; I did not check them on the registries.

```yaml
terraform:checks:
  stage: terraform
  image: { name: hashicorp/terraform:1.16.5, entrypoint: [""] }
  script:
    - cd infra/terraform
    - terraform fmt -check -recursive
    - for env in environments/*; do terraform -chdir=$env init -backend=false -input=false && terraform -chdir=$env validate; done
    - terraform test -test-directory=tests   # mock_provider, no credentials
  rules: [{ changes: ["infra/terraform/**/*"] }]

terraform:tflint:
  stage: terraform
  image: { name: ghcr.io/terraform-linters/tflint:v0.64.0, entrypoint: [""] }
  script:
    - cd infra/terraform && tflint --init && tflint --recursive --config "$PWD/.tflint.hcl"
  rules: [{ changes: ["infra/terraform/**/*"] }]

terraform:checkov:
  stage: terraform
  image: { name: bridgecrew/checkov:3.3.21, entrypoint: [""] }
  script:
    - checkov -d infra/terraform --framework terraform -o cli -o gitlab_sast --output-file-path console,gl-sast-checkov.json
  artifacts: { reports: { sast: gl-sast-checkov.json } }
  rules: [{ changes: ["infra/terraform/**/*"] }]
```

#### Q5. Cost estimate (us-central1, on-demand, 730 h/month, prices read 2026-10-04)

Unit prices:

| Item | Price | Source |
|---|---|---|
| GKE cluster management fee | $0.10/cluster/h; $74.40/month free-tier credit covers 1 zonal or Autopilot cluster | https://cloud.google.com/kubernetes-engine/pricing |
| e2-standard-4 (4 vCPU, 16 GiB) | $0.13402284/h | https://cloud.google.com/products/compute/pricing/general-purpose |
| n1-standard-4 (4 vCPU, 15 GiB) | $0.189999/h | same |
| g2-standard-4 (1× L4, 4 vCPU, 16 GiB) | $0.706832276/h; Spot $0.424056/h | https://cloud.google.com/products/compute/pricing/accelerator-optimized |
| g2-standard-8 (1× L4, 8 vCPU, 32 GiB) | $0.853624312/h; Spot $0.512112/h | same |
| NVIDIA T4 (attached GPU) | $0.35/GPU/h | https://cloud.google.com/products/compute/gpus-pricing |
| Cloud SQL Enterprise vCPU / memory | $0.0413/vCPU-h, $0.007/GiB-h (HA: $0.0826, $0.014) | https://cloud.google.com/sql/pricing |
| Cloud SQL SSD storage | $0.000232877/GiB-h (≈ $0.17/GiB-month) | same |

**Dev environment as designed** (zonal Standard cluster, 2× e2-standard-4 system nodes for Strimzi, Airflow, MLflow, Argo CD and monitoring, 1× g2-standard-4 L4 node, Cloud SQL `db-custom-1-3840` ZONAL with 20 GiB SSD):

| Component | Calc | $/month |
|---|---|---|
| GKE management fee (zonal) | $73.00 − $74.40 credit | **$0** |
| System pool 2× e2-standard-4 | 2 × 0.13402284 × 730 | **$195.67** |
| GPU pool 1× g2-standard-4 (L4), always on | 0.706832276 × 730 | **$515.99** |
| Cloud SQL db-custom-1-3840 ZONAL | (0.0413 + 3.75 × 0.007) × 730 | **$49.31** |
| Cloud SQL 20 GiB SSD | 20 × 0.000232877 × 730 | **$3.40** |
| **Total (excl. node boot disks, Cloud NAT, egress, GCS/AR/Secret Manager, which are small)** | | **≈ $765/month** |

Variants for the README:
- GPU pool scaled to 0 except ~4 h/day of demos: GPU ≈ $86. **Total ≈ $335/month.**
- GPU node on Spot g2-standard-4: $309.56. Total ≈ $560/month.
- g2-standard-8 instead (recommended headroom): $623.15. Total ≈ $872/month.
- T4 alternative, n1-standard-4 + T4: (0.189999 + 0.35) × 730 = $394.20. Total ≈ $643/month.
- Prod-ish Cloud SQL `db-custom-2-7680` REGIONAL (HA): (2 × 0.0826 + 7.5 × 0.014) × 730 = $197.26 + storage.
- A regional prod cluster is not covered by the free tier: +$73/month management fee.

**Managed alternatives (build-vs-buy section):**

| Managed option | Pricing basis | Rough $/month | vs self-hosted on GKE |
|---|---|---|---|
| **Cloud Composer 3**, now listed as "Managed Service for Apache Airflow (Gen 3)" | "$0.06" per DCU-hour (1,000 milli-DCU-hours). Google's example environment uses 12 DCUs (and 15 DCUs while scaled up) | 12 DCU × 0.06 × 730 = **≈ $526** (+ DB storage $0.17/GiB-month) | Airflow on the shared system pool adds roughly one e2-standard-4 node ≈ $98 |
| **Vertex AI online prediction endpoint**, pricing page now titled "Gemini Enterprise Agent Platform pricing", section "Prediction and explanation" | Node-hour while deployed: g2-standard-4 $0.81293/h; g2-standard-8 $0.98181/h; n1-standard-4 $0.219/h + T4 $0.42/h | g2-standard-4: **≈ $593**; g2-standard-8: ≈ $717; n1-standard-4+T4: ≈ $466 | GKE g2-standard-4 $516 (≈ 15% cheaper), but you run vLLM, autoscaling and drivers yourself |
| **Managed Service for Apache Kafka** | $0.09/DCU-h (1 vCPU + 4 GiB = 1 DCU). "At least 3 vCPUs per cluster". Billed "100 GB of local storage per CPU". Inter-zone replication $0.01/GiB, plus PSC data-processing charges | Minimum 3 vCPU/12 GiB: 3 × 0.09 × 730 = $197.10 + 300 GiB × $0.17 = $51.00 ⇒ **≈ $248 + traffic** | Strimzi 3 small brokers fit in the existing system pool (marginal node cost). Google's own table: 10 MiB/s ≈ $0.9K self-run on GCE vs $1.1K managed |

Sources: https://cloud.google.com/composer/pricing (page title "Managed Service for Apache Airflow pricing"), https://cloud.google.com/vertex-ai/pricing, https://cloud.google.com/managed-service-for-apache-kafka/pricing, https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/create-cluster (min 3 vCPU, 1–8 GiB per vCPU, updated 2026-09-30).

### Recommendation

- **Tooling:** Terraform 1.16.x, `hashicorp/google ~> 8.5`. Add a README note that the code is OpenTofu 1.13-compatible (MPL-2.0 vs BSL). tflint 0.64 + google ruleset 0.40.0, checkov 3.3.21, native `terraform test` with `mock_provider "google"`. Do not run terratest or plan.
- **Layout:** `infra/terraform/{bootstrap,modules/*,environments/{dev,prod},tests}`, with one GCS backend prefix per env and no workspaces. Follow Google's root-module guidance verbatim and cite it in the README.
- **Modules:** write raw resources in thin local modules. Optionally use `terraform-google-modules/network` v18.3.0. Do **not** use the GKE module v45, which pins google `< 8`.
- **GKE:** Standard, zonal in dev and regional in prod. Private nodes, IAM DNS endpoint with the IP endpoint disabled, Dataplane V2, Workload Identity Federation for GKE, REGULAR channel, shielded COS nodes. A system pool on e2-standard-4, and a GPU pool on g2-standard-8 (L4) with `gpu_driver_installation_config { gpu_driver_version = "LATEST" }` that autoscales from 0 (optionally Spot).
- **Identity and secrets:** use one GSA per DB/GCS workload (KSA→GSA via `roles/iam.workloadIdentityUser`), because Cloud SQL IAM DB users and GCS signed URLs need a service account. Use direct `principal://` bindings for ESO → Secret Manager. Use External Secrets Operator so the Helm charts keep consuming plain Kubernetes Secrets on both kind and GKE. The managed Secret Manager add-on is the alternative.
- **Cloud SQL:** `POSTGRES_18`, `edition = "ENTERPRISE"`, PSA private IP, `ssl_mode = "ENCRYPTED_ONLY"`, `cloudsql.iam_authentication=on`, Auth Proxy v2 as a native sidecar with `--auto-iam-authn --private-ip`. Dev: `db-custom-1-3840` ZONAL, no PITR. Prod: `db-custom-2-7680` REGIONAL, PITR, deletion protection.
- **README numbers:** quote "≈ $765/month always-on dev (≈ $335 with the GPU pool scaled to zero outside demos), us-central1 list prices as of 2026-10-04". For build-vs-buy, quote Composer 3 ≈ $526, Vertex endpoint (L4) ≈ $593 and Managed Kafka ≥ $248 per month.
- **Contradictions with decisions:** none are blocking. Naming updates for the README and ADR-0003: "Workload Identity" is now **Workload Identity Federation for GKE**. "Private cluster" is now **private nodes + DNS-based endpoint**. Cloud Composer's pricing page is now **Managed Service for Apache Airflow**. Vertex AI pricing sits under **Gemini Enterprise Agent Platform**. One soft conflict: "use Google's official modules" would force the old provider. The recommendation above resolves it by using raw resources.

### Risks and gotchas

- **Unproven at apply time (ADR-0003).** `validate`, tflint and checkov cannot catch API-side errors. The real example here is a PG16+ instance with a `db-custom-*` tier and no `edition`: it passes `validate` and fails only on create. Mitigation: `terraform test` assertions on known API rules, plus an honest README note.
- **GKE module pinned to google `< 8`** (v45.0.0). Mixing it with provider 8.x roots fails `init`.
- **GPU capacity.** L4/G2 is available only in some zones, and Spot/on-demand stock varies. Use `node_locations` with zones known to offer L4. The GPU pool scales from 0, so the first vLLM request has a cold start (node boot + driver + image + weights). Pin model weights in GCS or an image and use a larger boot disk.
- **GPU taints and tolerations.** vLLM pods need `nodeSelector cloud.google.com/gke-accelerator: nvidia-l4` and a toleration for `nvidia.com/gpu`. Check this against the kind chart values so the "same charts" claim holds; use per-env values files.
- **Private nodes need Cloud NAT** (or Artifact Registry remote repos) to pull quay.io/docker.io images such as Strimzi and vLLM. NAT cost is not in the estimate.
- **Federated identity limits:** GCS needs uniform bucket-level access, and federated principals "cannot generate signed URLs". Cloud SQL IAM DB users require a service-account email. Hence the GSA path for MLflow, Airflow and the scorer.
- **Secret Manager add-on does not sync to Kubernetes Secrets** (it is a separate `secret_sync_config` feature). Charts that expect `secretKeyRef` need ESO or that feature.
- **IAM DB users still need SQL GRANTs**, which are outside Terraform. Document a bootstrap Job or migration.
- **Secrets in state.** Create only `google_secret_manager_secret` containers in Terraform. If versions are ever set, use `secret_data_wo` + `secret_data_wo_version = "1"` (required as a string since provider 8.0).
- **checkov false positives:** CKV_GCP_20 (authorized networks) when using a DNS-only endpoint, and CKV_GCP_12 unless the explicit `network_policy { enabled = false }` + `ADVANCED_DATAPATH` pair is present. Every skip needs a written justification in the `#checkov:skip` comment.
- **tflint plugin download** hits the GitHub API. Set `GITHUB_TOKEN` in GitLab CI, or cache `~/.tflint.d`.
- **`init -backend=false` still needs network access** to registry.terraform.io (or a provider mirror) in CI. It needs no credentials.
- **Prices change.** All numbers are list prices on 2026-10-04 for us-central1. A European region (likely for a French-speaking role) is typically higher, so re-check the region in the calculator before quoting. Sustained-use and committed-use discounts are ignored.
- **Licence wording.** Terraform ≥ 1.6 is BSL 1.1, which is fine for this use. Do not describe Terraform as "open source" in the README. Say "source-available (BSL); OpenTofu is the MPL fork".

### Sources

- Terraform releases: https://github.com/hashicorp/terraform/releases
- OpenTofu: https://opentofu.org/ ; releases: https://github.com/opentofu/opentofu/releases
- HashiCorp BSL announcement (2023-08-10): https://www.hashicorp.com/en/blog/hashicorp-adopts-business-source-license
- HashiCorp licence FAQ: https://www.hashicorp.com/en/license-faq
- Google provider releases: https://github.com/hashicorp/terraform-provider-google/releases
- Provider 8.0 upgrade guide: https://github.com/hashicorp/terraform-provider-google/blob/main/website/docs/guides/version_8_upgrade.html.markdown (registry: https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/version_8_upgrade)
- `google_container_cluster` v8.5.0 doc: https://github.com/hashicorp/terraform-provider-google/blob/v8.5.0/website/docs/r/container_cluster.html.markdown
- `google_container_node_pool` v8.5.0 doc: https://github.com/hashicorp/terraform-provider-google/blob/v8.5.0/website/docs/r/container_node_pool.html.markdown
- `google_sql_database_instance` v8.5.0 doc: https://github.com/hashicorp/terraform-provider-google/blob/v8.5.0/website/docs/r/sql_database_instance.html.markdown
- `google_sql_user` v8.5.0 doc: https://github.com/hashicorp/terraform-provider-google/blob/v8.5.0/website/docs/r/sql_user.html.markdown
- terraform-google-modules versions.tf: https://github.com/terraform-google-modules/terraform-google-kubernetes-engine/blob/v45.0.0/versions.tf ; https://github.com/terraform-google-modules/terraform-google-sql-db/blob/v28.3.0/modules/postgresql/versions.tf ; https://github.com/terraform-google-modules/terraform-google-network/blob/v18.3.0/versions.tf
- Terraform workspaces: https://developer.hashicorp.com/terraform/cli/workspaces
- Google root-module best practices (2026-09-30): https://docs.cloud.google.com/docs/terraform/best-practices/root-modules
- Google general style (2026-09-30): https://docs.cloud.google.com/docs/terraform/best-practices/general-style-structure
- GKE GPUs (2026-10-02): https://docs.cloud.google.com/kubernetes-engine/docs/how-to/gpus
- GKE Autopilot GPUs (2026-10-02): https://docs.cloud.google.com/kubernetes-engine/docs/how-to/autopilot-gpus
- Autopilot vs Standard comparison (2026-10-02): https://docs.cloud.google.com/kubernetes-engine/docs/resources/autopilot-standard-feature-comparison
- GKE network isolation (2026-09-30): https://docs.cloud.google.com/kubernetes-engine/docs/concepts/private-cluster-concept
- Workload Identity Federation for GKE (2026-10-02): https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity
- Identity federation product limitations (2026-09-30): https://docs.cloud.google.com/iam/docs/federated-identity-supported-services
- Secret Manager add-on for GKE (2026-09-30): https://docs.cloud.google.com/secret-manager/docs/secret-manager-managed-csi-component
- External Secrets Operator GCP provider: https://external-secrets.io/latest/provider/google-secrets-manager/
- Cloud SQL private IP (2026-09-30): https://docs.cloud.google.com/sql/docs/postgres/configure-private-ip
- Cloud SQL from GKE (2026-09-30): https://docs.cloud.google.com/sql/docs/postgres/connect-kubernetes-engine
- Cloud SQL IAM database auth (2026-09-30): https://docs.cloud.google.com/sql/docs/postgres/iam-authentication
- Cloud SQL DB versions (2026-09-30): https://docs.cloud.google.com/sql/docs/db-versions
- Cloud SQL Auth Proxy: https://github.com/GoogleCloudPlatform/cloud-sql-proxy
- terraform validate: https://developer.hashicorp.com/terraform/cli/commands/validate
- GCS backend: https://developer.hashicorp.com/terraform/language/backend/gcs
- Terraform test mocking: https://developer.hashicorp.com/terraform/language/tests/mocking
- tflint-ruleset-google v0.40.0: https://github.com/terraform-linters/tflint-ruleset-google/blob/v0.40.0/README.md ; rules: https://github.com/terraform-linters/tflint-ruleset-google/blob/v0.40.0/docs/rules/README.md
- checkov policy index (3.3.21): https://github.com/bridgecrewio/checkov/blob/3.3.21/docs/5.Policy%20Index/terraform.md ; check sources under https://github.com/bridgecrewio/checkov/tree/3.3.21/checkov/terraform/checks/resource/gcp
- checkov suppressions: https://www.checkov.io/2.Basics/Suppressing%20and%20Skipping%20Policies.html ; CLI: https://www.checkov.io/2.Basics/CLI%20Command%20Reference.html
- terratest: https://github.com/gruntwork-io/terratest/releases
- GKE pricing: https://cloud.google.com/kubernetes-engine/pricing
- Compute general-purpose pricing: https://cloud.google.com/products/compute/pricing/general-purpose
- Compute accelerator-optimized (G2) pricing: https://cloud.google.com/products/compute/pricing/accelerator-optimized
- GPU pricing (T4): https://cloud.google.com/products/compute/gpus-pricing
- Cloud SQL pricing: https://cloud.google.com/sql/pricing
- Composer / Managed Service for Apache Airflow pricing: https://cloud.google.com/composer/pricing
- Vertex AI / Agent Platform pricing: https://cloud.google.com/vertex-ai/pricing
- Managed Service for Apache Kafka pricing: https://cloud.google.com/managed-service-for-apache-kafka/pricing ; cluster sizing: https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/create-cluster
