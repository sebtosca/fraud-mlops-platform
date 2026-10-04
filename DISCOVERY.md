# Discovery: Fraud ML Platform

Status: complete
Last updated: 2026-10-04

## The idea
A small MLOps platform that trains, deploys, monitors, and continuously updates a fraud-detection model on streaming card transactions. It deliberately recreates the production problems from *Designing Machine Learning Systems* (Chip Huyen), chapters 7–10, so they can be detected and handled end to end:

- **Ch. 7 – Deployment:** online scoring of each transaction via Kafka, nightly batch scores per account, one shared feature package for training and serving (no train/serve skew), ONNX latency benchmark.
- **Ch. 8 – Shift & monitoring:** inject covariate, label, and concept shift; detect with KS and PSI over time windows; monitor prediction distributions while labels are delayed.
- **Ch. 9 – Continual learning:** retraining triggered by drift or schedule; stateless vs. stateful retraining comparison; challenger in shadow mode then canary; promote only if it wins.
- **Ch. 10 – Infrastructure:** Airflow orchestrator, MLflow tracking + registry, Docker Compose (stretch: Kubernetes with kind), GitLab CI, build-vs-buy write-up.

Proposed stack: Python (uv/Poetry), FastAPI, Kafka, PostgreSQL, MLflow, Airflow, Evidently or custom PSI/KS, Docker Compose, GitLab CI; optional Grafana, ONNX Runtime. Data: a public credit-card fraud dataset replayed as a stream. Optional LLM add-on: case summaries for flagged transactions with a versioned, CI-gated prompt.

Purpose: interview preparation for an ML/MLOps role (job description mentions "modèles ML et plateformes LLM"), built within a 4-day prep plan (~2 days of build time), ending with a 2-minute architecture walkthrough. Source brief: `fraud-ml-platform-project.md`.

### Facts gathered (not decisions)
- Machine: 16 cores, 31 GiB RAM (~17 GiB available), RTX 3090 24 GB, ~600 GB free disk.
- Tooling present: Docker 29.8 + Compose v5.5, Python 3.13, uv 0.11, git. Not installed: Poetry, kind.
- User's existing repos live in `~/projects/` and are hosted on GitHub (`github.com/sebtosca`).
- Related prior work: `~/projects/Model_Regression_Detection_System` ("PromptGuard") — CI-gated prompt regression detection with a golden dataset, LLM judge, GitHub Actions gate, and Slack alerts. Overlaps heavily with the optional LLM add-on's "versioned prompt gated by an eval suite".

## Decisions summary
**Goal & constraints**
- Goal: both learning and portfolio; the 2-minute walkthrough + working demo + strong README take priority over full feature coverage (Q1)
- Timebox: two phases (Q24, supersedes Q2's single 2-day box). **Phase 1** = core fraud loop, ~2 days, hard limit; when behind, cut in the brief's order (step 4 CI → batch scoring), protect monitoring + feedback-loop story. **Phase 2** = platform layer (Kubernetes/GitOps, Terraform, GitLab CI/CD, LLM, Spark), ~3–4 more days. Exact days and interview date still unknown
- Skills to demonstrate: the job description pasted in Round 3 (ML + LLM platform ownership, robust data pipelines for data scientists, reproducibility/testability, MLOps CI/CD + monitoring + model management + versioning, microservice architecture, craftsmanship) is the yardstick for every scope call (Round 3)
- Docs language: English for code and docs; short French summary in README if the interview is in French (Q7)

**Repo & tooling**
- Location: `~/projects/fraud-ml-platform` (Q3)
- Hosting/CI: gitlab.com + GitLab CI (the job lists GitLab) (Q3 → settled by Round 3 input and Q27)
- Python env: uv (already installed; Poetry is not) (Q3, follows from tooling facts)

**Data**
- Dataset: Sparkov synthetic credit-card fraud data (Kaggle `fraudTrain.csv`/`fraudTest.csv`); user downloads it manually (Q4, [ADR-0001](docs/adr/0001-sparkov-dataset.md))

**Simulation**
- The platform acts on predictions: each transaction gets a Decision (block / review / allow); blocked transactions never receive a label except a small random Exploration sample that is let through (Q5)

**Scope**
- In: ONNX latency benchmark (Q6)
- If time allows: Grafana (alerts land in a Postgres table first) (Q6)
- ~~Out: Kubernetes/kind; LLM add-on~~ — reversed in Round 4: both are in Phase 2 (Q25, Q28)
- Out: Java, Keras/TensorFlow, Jenkins (mentioned in README), Oracle, Elasticsearch, MongoDB, Databricks (build-vs-buy only); Red Hat shown via UBI base images (Q31; **refined by Q43**)

**Platform (Phase 2)**
- Runtime: kind + Helm + Argo CD is the single prod-like runtime; same charts target GKE; Compose only as a near-free inner loop — **amended by Q38** (Q25, [ADR-0003](docs/adr/0003-kind-argocd-portable-gke.md))
- GCP Terraform (never applied): portable layout — GKE (+GPU node pool), Cloud SQL Postgres, GCS, Artifact Registry, Secret Manager, Workload Identity; Kafka via Strimzi on GKE; modules + dev/prod envs + GCS remote-state backend; CI runs fmt/validate/tflint/checkov only; managed alternatives (Composer, Vertex AI, Managed Kafka) in build-vs-buy (Q26, ADR-0003)
- GitLab CI/CD stages: ruff+mypy → tests → SonarCloud (coverage gate) → build UBI images → Trivy → Terraform checks → model quality gate → LLM prompt-eval gate → push + bump image tags in `deploy/` Helm values → Argo CD syncs to kind; self-hosted GitLab runner on the user's machine for GPU/integration jobs (Q27)
- LLM: self-hosted ~7–8B quantised open model on vLLM (OpenAI-compatible) on the RTX 3090; separate case-summary microservice for flagged transactions; prompts versioned in MLflow prompt registry; eval suite gates prompt/model changes in CI (PromptGuard approach) on the GPU runner; token/latency/cost metrics (Q28, [ADR-0004](docs/adr/0004-self-hosted-llm-vllm.md))
- Data engineering: PySpark local mode for the batch layer — raw CSV → bronze/silver/gold Parquet with data-quality checks, training dataset build, nightly per-account batch scoring — orchestrated by Airflow; training uses `groupBy(card).applyInPandas` over the ADR-0002 state function (Q29)
- Reproducibility (**amended by Q36, Q45**): MinIO as local GCS stand-in with immutable versioned data snapshots; every MLflow run logs data hash + git SHA + config; no DVC; DS guide in `docs/`; ports-and-adapters design, typing, pre-commit (Q30)

**Domain & simulation mechanics**
- Account = card (`cc_num`); all per-account history, velocity features and batch scores key on it (Q9)
- Timeline: train on earliest ~12 months, replay the rest as the live stream on a Simulated clock driven by event time, accelerated (~1 simulated day ≈ 30 s) (Q10)
- Label delay: fraud labels arrive as chargebacks 7–45 simulated days later; legit labels confirmed at 45 days with no chargeback; review Decisions labelled by a simulated analyst after ~1 day; blocks unlabeled unless in the Exploration sample (Q11)
- Decision policy: Alert budget — block top 0.3% of scores, review next 0.7%; thresholds chosen on validation data at training time and stored with the model version; Exploration sample = 2% of would-be blocks (Q12; **amended by Q40**)
- Shift scenarios (each toggleable, start/end logged as ground truth): covariate = amounts ×1.8 in two categories; label = fraud rate ~3× via oversampling; concept = new night-time card-testing bursts of tiny amounts in a category the champion considers safe (Q13; **refined by Q42**)

**Model & features**
- Model: LightGBM (real warm start via `init_model`; ONNX via `onnxmltools`) (Q14)
- Online feature state: one pure per-card state-update function in `features/`, used by training (replay) and serving (in-memory, Kafka partitioned by card, snapshots to Postgres); no Redis (Q15, [ADR-0002](docs/adr/0002-shared-feature-state-function.md))

**Continual learning**
- Promotion: challenger in Shadow mode for N simulated days → must beat champion on fraud-dollar recall at equal Alert budget by ≥1 pt on Matured labels, not lose on PR-AUC, and keep p99 latency in budget → Canary 10% → move `champion` alias. Canary only if time allows (Q16)

**Continual learning (cont.)**
- Censored labels: drop unlabeled blocks; weight Exploration-sample rows by 1/0.02 = 50 (inverse-propensity weighting); also train a naive model and report the difference (Q18)
- Drift rules: 1-sim-day windows vs champion's training data; feature alert = PSI > 0.2 or (KS p < 0.01 and effect size > 0.1) on ≥2 consecutive windows; same on prediction scores; performance check on Matured labels; custom PSI/KS, not Evidently (Q19; **extended by Q39**)
- Retraining: triggered by any drift alert or every 30 sim days; each trigger trains a stateless challenger (12 months of matured labels) and a stateful one (LightGBM warm start on data since last train); compute + quality recorded; better one goes to shadow (Q20)

**Quality & security**
- Testing: unit tests on features incl. train/serve parity test, PSI/KS on known distributions, decision policy; model quality gate (fixed small sample, PR-AUC floor); no full stack in CI — `make demo` is the local integration test; extended by the Q27 pipeline (Q21)
- Security: local-only, no service auth; git-ignored `.env` + committed `.env.example`; dataset git-ignored; card numbers hashed at ingestion; cloud side uses Secret Manager + Workload Identity in Terraform; "real client" section in README (Q22, Q26)

**Results**
- `make demo` writes `reports/results.md` + PNG charts, key numbers copied into README; monitoring + alerts in Postgres (Grafana if built); MLflow holds run comparisons (Q23)

**MLOps maturity (Google Cloud reference)**
- Target: Google Cloud MLOps Level 2; README maps components to its 6 stages (Q32)
- Pipeline CI/CD: Airflow tasks run versioned container images (same image for notebooks); CI runs component, integration and convergence tests on the training pipeline; CD ships DAGs and images together (Q33)
- Environments (**amended by Q37**): `staging` and `prod` namespaces on kind via Argo CD; merge to `main` → staging auto-sync, git tag → prod; API contract tests + k6 load test (p99 vs latency budget) in staging (Q34)
- Validation: pandera schema/value checks first in every training run and in the silver layer; schema skew aborts retraining and alerts; model validation = Q16 gate; no feature store (ADR-0002), README explains when Feast/Vertex Feature Store would be adopted (Q35)

**Done**
- `make demo`: stack up → train + register champion → stream → inject concept shift → alert → retrain → shadow → auto-promote → results summary (detection lag per shift type, stateless vs stateful cost/quality, ONNX vs native latency, feedback-loop measurement); README with architecture diagram and build-vs-buy (Q17)

**Amendments from research (Round 6, 2026-10-04) — these override the lines they reference**
- Object store: SeaweedFS replaces MinIO (archived, image no longer pullable); docs say "S3-compatible object store" (Q36, [ADR-0005](docs/adr/0005-seaweedfs-object-store.md))
- Topology: shared platform services (Kafka, Postgres with per-env DBs, MLflow, object store, Airflow, vLLM) in platform namespaces; only app services duplicated in `staging`/`prod`; vLLM `replicas: 0` by default, scaled up for LLM demos (Q37, ADR-0003 amended)
- Runtime by phase: Phase 1 runs on Docker Compose; Phase 2 moves the same images, env-var config and service names to kind (Q38, ADR-0003 amended)
- Monitoring additions: per-category amount drift and fixed-cutoff block/review-rate monitoring (binomial test); score thresholds frozen per model version (Q39)
- Exploration rate: configurable; demo profile 10% (weight 10), default 2%; report effective sample size and bootstrap CIs, consider SNIPS / weight clipping (Q40)
- Clock vs orchestration: no wall-clock Airflow schedules; the replayer emits asset events; batch scoring every K sim days (e.g. 7) and/or replay pauses while gated jobs run (Q41)
- Concept shift: keep `home` category, tiny amounts at night, plus an acceptance test proving the champion misses the pattern before the demo relies on it; tune if not (Q42)
- Python/images: ubi9-minimal base with uv-installed CPython 3.13; README says "UBI base OS", not Red Hat-built Python (Q43)
- State recovery: predictions, card state and Kafka offsets committed in one Postgres transaction → exactly-once effects, no state lost on crash (Q44, ADR-0002 amended)
- Data hash: log our own SHA-256 of each snapshot manifest; MLflow dataset digest is not a reproducibility hash (Q45)

## Glossary
Terms are defined in `CONTEXT.md`. Decision records are in `docs/adr/`.

## Open questions
_None blocking._ Research completed 2026-10-04 (`RESEARCH.md`); its suggested changes were accepted in Round 6. Known unknown, asked four times without an answer: interview date and number of build days (Q8/Q24). **Planning assumption:** Phase 1 ≈ 2 days, Phase 2 ≈ 3–4 days, Phase 1 must be demoable on its own; PLAN should order Phase 2 so it can stop at any point with a coherent story. GitLab and a French-language interview are inferred from the job description.

## Suggested research areas
1. **Sparkov dataset** — exact schema, fraud rate, time span, download/licence, quirks; how each injected shift (Q13) maps onto its columns.
2. **Kafka on Kubernetes** — Strimzi on kind, partition-by-card ordering, Python client choice (confluent-kafka vs aiokafka), consumer state snapshot/restore (ADR-0002).
3. **LightGBM continual training & ONNX** — `init_model` / warm-start semantics and pitfalls, categorical features in ONNX export (onnxmltools), ONNX Runtime vs native latency methodology.
4. **Drift & delayed labels** — PSI binning, KS at high volume and effect sizes, prediction-drift monitoring, inverse-propensity weighting and counterfactual evaluation under censored fraud labels (Q18, Q19).
5. **MLflow (current major version)** — registry aliases, prompt registry, deployment on Kubernetes with Postgres + MinIO backends.
6. **Airflow on Kubernetes** — official Helm chart, executor choice, KubernetesPodOperator with versioned images, triggering retraining from drift alerts (REST API vs assets/datasets).
7. **PySpark** — local mode in containers, `groupBy().applyInPandas` for the shared state function, medallion layout, pandera-on-Spark.
8. **LLM serving** — vLLM with GPU on kind (NVIDIA device plugin / container toolkit inside kind), 7–8B quantised model choice for French/English summaries on 24 GB, LLM eval methods and gating.
9. **GitLab CI/CD** — SonarCloud for GitLab, self-hosted GPU runner, Trivy, Terraform static checks, GitOps image-tag bump pattern, Argo CD multi-environment promotion.
10. **Terraform on GCP** — module layout, GKE + GPU node pool, Cloud SQL, Workload Identity, Secret Manager, GCS backend; tflint/checkov rule sets.
11. **Resource budget** — whether Kafka + Airflow + MLflow + Postgres + MinIO + Spark + vLLM + Argo CD fit on kind with 31 GiB RAM / 24 GB VRAM, and what to slim down.
12. **Google Cloud MLOps Level 2** — map each component to its 6 stages and check nothing is missing.

## Q&A log

### Round 1 — 2026-10-02
User answered the whole round with a single "Yes" (accepting every recommendation). The unknowns inside Q2, Q3 and Q7 are carried to Open questions.

**Q1 - Primary goal**
- Asked: Is the main point (a) learning ch. 7–10 by doing, (b) a public portfolio repo, or (c) both, ranked?
- Recommended: Both, with the 2-minute walkthrough ranked first: a working demo + strong README beat full coverage.
- Answer: "Yes"

**Q2 - Deadline**
- Asked: When is the interview and when does day 1 start? Is ~2 days of build a hard limit?
- Recommended: Treat it as a hard limit and use the brief's cut order.
- Answer: "Yes" (hard limit accepted; dates not given)

**Q3 - Repo location and hosting**
- Asked: Where should it live, and GitHub or GitLab? (Brief says GitLab CI; user's repos are on GitHub.)
- Recommended: `~/projects/fraud-ml-platform`; GitLab only if the target company uses it, otherwise GitHub Actions with a GitLab note in the README.
- Answer: "Yes" (folder created; company's VCS still unknown)

**Q4 - Dataset**
- Asked: ULB creditcard (PCA features, 2 days, no account ID) vs Sparkov synthetic (card/merchant/category/location/timestamps, ~2 years) vs PaySim vs own generator?
- Recommended: Sparkov — supports per-account history, velocity features, batch scores, and a meaningful time split.
- Answer: "Yes"

**Q5 - Does the model act on transactions?**
- Asked: Should the simulation block transactions so blocked ones never get a label (the feedback-loop problem)?
- Recommended: Yes — block/allow/review decisions; blocked ones unlabeled except a small random exploration sample.
- Answer: "Yes"

**Q6 - Optional scope**
- Asked: Which are in: LLM add-on, Grafana, Kubernetes/kind, ONNX benchmark?
- Recommended: ONNX in; Grafana if time allows; kind out; LLM add-on out (point to PromptGuard), at most a thin summary endpoint if early.
- Answer: "Yes"

**Q7 - Language**
- Asked: README/docs in English or French?
- Recommended: English, plus a short French summary if the interview is in French.
- Answer: "Yes"

### Round 2 — 2026-10-02
User answered with a single "Yes" (accepting every Round 2 recommendation). Q8 asked for facts, not a decision, and remains unanswered.

**Q8 - Facts (interview date, day-1 start, GitLab at target company, interview language)**
- Asked: Interview date and day-1 start? Does the target company use GitLab? Is the interview in French?
- Recommended: (facts only — no recommendation)
- Answer: "Yes" — does not answer the facts; kept open

**Q9 - What is an "account"?**
- Asked: Sparkov has `cc_num` and customer fields but no account ID. What keys per-account features and batch scores?
- Recommended: Account = card (`cc_num`).
- Answer: "Yes"

**Q10 - Time and replay**
- Asked: How is the timeline split between training and the replayed stream?
- Recommended: Train on earliest ~12 months, replay the rest on an accelerated simulated event-time clock (~1 sim day ≈ 30 s).
- Answer: "Yes"

**Q11 - Label delay**
- Asked: How do labels arrive?
- Recommended: Fraud as chargebacks after 7–45 sim days; legit confirmed at 45 days; review labelled after ~1 day; blocks unlabeled unless exploration.
- Answer: "Yes"

**Q12 - Decision policy**
- Asked: How are block/review thresholds set, and how big is the exploration sample?
- Recommended: Alert budget — block top 0.3%, review next 0.7%, thresholds fixed on validation and stored with the model; 2% of would-be blocks let through.
- Answer: "Yes"

**Q13 - Shift scenarios**
- Asked: What does each injected shift look like?
- Recommended: Covariate = amounts ×1.8 in two categories; label = ~3× fraud rate; concept = night-time tiny-amount card-testing bursts in a "safe" category; each toggle logs start/end as ground truth.
- Answer: "Yes"

**Q14 - Model family**
- Asked: Which model?
- Recommended: LightGBM — real warm start, ONNX export, tabular standard.
- Answer: "Yes"

**Q15 - Online feature state**
- Asked: Where does per-card velocity state live at serving time without train/serve skew?
- Recommended: One pure state-update function in `features/`; training replays through it; serving keeps state in memory (Kafka partitioned by card, Postgres snapshots); no Redis.
- Answer: "Yes"

**Q16 - Promotion rule**
- Asked: When does a challenger replace the champion?
- Recommended: Shadow N sim days → ≥1 pt better fraud-dollar recall at equal alert budget on matured labels, no PR-AUC loss, p99 latency in budget → canary 10% → alias switch; canary only if time allows.
- Answer: "Yes"

**Q17 - Definition of done**
- Asked: What does finished look like?
- Recommended: One `make demo` running the full loop and writing a results summary; README with architecture diagram and build-vs-buy.
- Answer: "Yes"

### Round 3 — 2026-10-02
Round 3 asked Q18–Q23 (listed under Open questions). The user did not answer them directly; instead they gave new direction that reopens Q3, Q6 and Q2's timebox. Logged verbatim:

**User input (unprompted, verbatim)**
> I wont run it in the cloud but I do want to set it up on GCP with terraform, and have CI/CD automation. Goal is to build a project that practices:
>
> Piloter le développement et l'amélioration des modèles ML et des plateformes LLM
> Créer et déployer des pipelines de données robustes pour data scientists et analysts
> Accompagner les Data Scientists pour garantir reproductibilité, testabilité et évolutivité des solutions
> Collaborer avec les parties prenantes pour faire évoluer les solutions techniques
> Mettre en œuvre les principes MLOps : CI/CD, monitoring, gestion des modèles, testing, versioning
> Coacher et coordonner les équipes, promouvoir les bonnes pratiques et assurer la veille technologique
>
> My skills in this project should show:
>
> Design, développement et implémentation de solutions techniques
> Maîtrise des pipelines ML, de la conception à la production.
> Excellentes compétences en Python.
> Expérience de déploiement de Large Language Models et l'écosystème LLMOps
> Maîtrise des bonnes pratiques de développement (design patterns, craftsmanship, industrialisation de code, tests..) et le Data Engineering
> Capacité à concevoir des architectures micro services
>
> Uses from these technologies:
>
> * Langages de programmation : Python, Java
> * Frameworks: Pandas, Keras, Tensorflow, Scikit-Learn, Numpy, Pyspark
> * Plateformes: Databricks, Kafka, Spark
> * DevOps et CI/CD : GitLab, Jenkins, Sonar, GitOps, Kubernetes, Docker
> * MLOps et Big Data : Airflow, ML Flow, MLOps
> * Bases de données : PostgreSQL, Oracle, ElasticSearch, MongoDB
> * Systèmes d'exploitation : RedHat

Implications noted by the interviewer (not yet decisions):
- GCP infrastructure as Terraform code, never applied → Terraform must be validated/planned without a live project.
- The job lists GitLab → strong evidence for GitLab CI (partially answers Q8/Q3).
- The job description is in French → interview likely in French (partially answers Q8/Q7).
- "Déploiement de LLM / LLMOps" is a skill to *show* → reopens Q6 (LLM add-on was out).
- Kubernetes, GitOps, Spark/PySpark, Sonar now explicitly targeted → reopens Q6 (kind was out) and strains Q2's 2-day timebox.

### Round 4 — 2026-10-02
User answer to the whole round (and to the still-open Round 3, Q18–Q23): "Yes, I agree." plus a reference: https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning
The two facts requested (days available, interview date) were not given.

**Q18 - Retraining on censored labels**
- Recommended: Drop unlabeled blocks; IPW weight 50 on Exploration sample; also train a naive model and report the difference.
- Answer: "Yes, I agree."

**Q19 - Drift detection rules**
- Recommended: 1-sim-day windows vs training data; PSI > 0.2 or (KS p < 0.01 and effect > 0.1) on ≥2 consecutive windows; same on scores; matured-label performance; custom PSI/KS.
- Answer: "Yes, I agree."

**Q20 - Retraining triggers and windows**
- Recommended: Drift alert or every 30 sim days; train stateless (12 months) and stateful (warm start) challengers each time; better one to shadow.
- Answer: "Yes, I agree."

**Q21 - Testing and CI**
- Recommended: Feature unit tests incl. parity test; PSI/KS and policy tests; model quality gate; no full stack in CI.
- Answer: "Yes, I agree." (extended by Q27)

**Q22 - Security and auth**
- Recommended: Local-only, no auth; `.env` git-ignored + `.env.example`; dataset git-ignored; hash card numbers; "real client" README section.
- Answer: "Yes, I agree." (cloud side extended by Q26)

**Q23 - Where results live**
- Recommended: `reports/results.md` + PNGs, numbers in README; monitoring in Postgres; MLflow for run comparisons.
- Answer: "Yes, I agree."

**Q24 - Timebox**
- Asked: 2 days can't hold the new scope. (a) keep 2 days and cut, (b) Phase 1 core ~2 days + Phase 2 platform ~3–4 days, (c) new total? How many days do you have?
- Recommended: (b).
- Answer: "Yes, I agree." — (b); day count not given

**Q25 - Local runtime**
- Asked: Docker Compose or Kubernetes?
- Recommended: kind + Helm + Argo CD as the single runtime, same charts for GKE; Compose only as a cheap inner loop. Reverses Q6's "kind out".
- Answer: "Yes, I agree."

**Q26 - GCP Terraform scope**
- Asked: Managed services (Composer, Vertex, Pub/Sub) or portable?
- Recommended: Portable — GKE (+GPU pool), Cloud SQL, GCS, Artifact Registry, Secret Manager, Workload Identity, Strimzi; modules + dev/prod + GCS backend; CI fmt/validate/tflint/checkov only; managed options in build-vs-buy.
- Answer: "Yes, I agree."

**Q27 - GitLab CI/CD pipeline**
- Recommended: gitlab.com; ruff/mypy → tests → SonarCloud → UBI image build → Trivy → Terraform checks → model gate → prompt-eval gate → push + bump Helm values → Argo CD sync; self-hosted GPU runner; Jenkins out.
- Answer: "Yes, I agree."

**Q28 - LLM deployment**
- Recommended: Self-hosted ~7–8B quantised model on vLLM on the 3090; case-summary microservice; prompts in MLflow prompt registry; CI eval gate on GPU runner; token/latency/cost metrics.
- Answer: "Yes, I agree."

**Q29 - Spark / data engineering**
- Recommended: PySpark local mode for bronze/silver/gold + DQ checks, training set build, nightly batch scoring; Airflow orchestrates; `applyInPandas` over the shared state function; Databricks only in build-vs-buy.
- Answer: "Yes, I agree."

**Q30 - Reproducibility and versioning**
- Recommended: MinIO with immutable snapshots; MLflow logs data hash + git SHA + config; no DVC; DS guide; ports-and-adapters, typing, pre-commit.
- Answer: "Yes, I agree."

**Q31 - Remaining listed tech**
- Recommended: Java, Keras/TF, Jenkins, Oracle, Elasticsearch, MongoDB out; Red Hat via UBI base images.
- Answer: "Yes, I agree."

Reference read (Google Cloud, "MLOps: Continuous delivery and automation pipelines in machine learning"): defines Level 0 (manual), Level 1 (ML pipeline automation / continuous training: data and model validation, feature store, metadata management, pipeline triggers), Level 2 (CI/CD pipeline automation: development & experimentation → pipeline CI → pipeline CD → automated triggering → model continuous delivery → monitoring; CI includes unit, component-integration and convergence tests; CD includes infra compatibility, prediction-service API tests, load tests, test → pre-prod → prod promotion). Gaps it exposed are asked in Round 5.

### Round 5 — 2026-10-02
User answer to the whole round: "Yesd" (read as "Yes"). Interview date and build days again not given; recorded as a planning assumption under Open questions.

**Q32 - Target maturity**
- Asked: Explicitly target Google Cloud MLOps Level 2?
- Recommended: Yes; README maps components to its 6 stages.
- Answer: "Yesd"

**Q33 - Pipeline CI/CD**
- Asked: Treat the training pipeline as the deployable unit?
- Recommended: Versioned images per Airflow task (same image for notebooks); CI component/integration/convergence tests; CD ships DAGs + images together.
- Answer: "Yesd"

**Q34 - Environments**
- Asked: How do test → pre-prod → prod work?
- Recommended: `staging` + `prod` namespaces on kind via Argo CD; merge → staging, tag → prod; API contract tests + k6 load test in staging.
- Answer: "Yesd"

**Q35 - Data validation and feature store**
- Asked: Where do data validation and a feature store fit?
- Recommended: pandera checks first in training and the silver layer, schema skew aborts + alerts; model validation = Q16; no feature store (ADR-0002), README explains when to adopt one.
- Answer: "Yesd"

### Round 6 — 2026-10-04 (research amendments)
After `RESEARCH.md` was produced, the coordinator proposed changes A1–A10 (RESEARCH.md, "Cross-cutting findings", section A). User answer to all of them: "yes" (accepting option (a) for A8, as stated in the question).

**Q36 - Object store (A1)**
- Asked: Replace MinIO (community edition archived, image no longer pullable) with SeaweedFS?
- Recommended: Yes; new ADR; "S3-compatible object store" in docs.
- Answer: "yes"

**Q37 - Shared infrastructure (A2)**
- Asked: Share platform services across envs and duplicate only app services; vLLM off by default?
- Recommended: Yes — full stack per namespace ≈ 31–35 GiB does not fit; slim peak ≈ 12.8 GiB (vLLM off) / 18.8 GiB (on).
- Answer: "yes"

**Q38 - Phase 1 on Compose (A3)**
- Asked: Run Phase 1 on Docker Compose and migrate the same images to kind in Phase 2?
- Recommended: Yes.
- Answer: "yes"

**Q39 - Monitoring additions (A4)**
- Asked: Add per-category amount drift and fixed-cutoff action-rate monitoring; freeze thresholds per model version?
- Recommended: Yes — Q19 rules alone miss the label and concept scenarios.
- Answer: "yes"

**Q40 - Exploration rate (A5)**
- Asked: Make it configurable with a 10% demo profile?
- Recommended: Yes — 2% gives ~56 labelled blocks per replay-year.
- Answer: "yes"

**Q41 - Clock vs orchestration (A6)**
- Asked: Replayer-driven asset events, batch scoring every K sim days and/or replay pause during gated jobs?
- Recommended: Yes.
- Answer: "yes"

**Q42 - Concept-shift acceptance test (A7)**
- Asked: Add a test proving the champion misses the injected pattern?
- Recommended: Yes — night-time is already Sparkov's strongest fraud signal.
- Answer: "yes"

**Q43 - Python version on UBI (A8)**
- Asked: (a) ubi9-minimal + uv 3.13, (b) pin 3.12 on `ubi9/python-312`, (c) move to 3.14?
- Recommended: (a).
- Answer: "yes" → (a)

**Q44 - State recovery wording (A9)**
- Asked: Commit predictions, card state and offsets in one Postgres transaction and amend ADR-0002?
- Recommended: Yes.
- Answer: "yes"

**Q45 - Data hash (A10)**
- Asked: Log our own SHA-256 per snapshot instead of relying on the MLflow digest?
- Recommended: Yes.
- Answer: "yes"
