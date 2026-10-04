# PRD: Fraud ML Platform

Status: approved · Last updated: 2026-10-04
Sources: DISCOVERY.md, RESEARCH.md, PLAN.md (terms as defined in CONTEXT.md)

## Problem statement
A Platform owner preparing for an ML/MLOps engineer interview needs to show, not just describe, that they can take ML and LLM systems from design to production. The target role covers robust pipelines, monitoring, model management, CI/CD, testing, versioning, microservices and LLMOps.

The production problems that matter in fraud detection are hard to show with a notebook:
- Shifts happen, and Labels arrive weeks late.
- The model's own Decisions hide the outcomes it later learns from (the Degenerate feedback loop).
- Retraining and promotion must be safe and auditable.

Reviewers want to see these problems recreated, detected, measured and handled. The work must be on a platform built with the tools named in the job description, and the Platform owner must be able to explain it in two minutes.

## Solution
A platform that replays a year of synthetic card Transactions as a live stream on an accelerated Simulated clock. It scores each Transaction with a Champion, makes a block / review / allow Decision, and lets a small Exploration sample of would-be blocks through so they can be labelled. It releases Labels with realistic delays and lets the Platform owner switch covariate, label and concept Shifts on and off. The platform:
- detects Shifts with drift, per-category and action-rate monitoring, plus estimators that work before Labels mature;
- retrains Challengers in Stateless and Stateful modes with and without exploration weighting;
- runs Challengers in Shadow mode, then Canary, and promotes them only when they pass the Promotion gate;
- records every step for audit, and writes a results report with measured numbers.

Part A runs everything on one machine with a single demo command. Part B rebuilds it as a production-shaped platform:
- GitLab CI/CD with quality, security, model and prompt gates;
- GitOps deployment to staging and prod on Kubernetes;
- Airflow and Spark pipelines;
- a self-hosted LLM that writes Case summaries for the Fraud analyst;
- cloud infrastructure-as-code for GCP that is validated but never applied.

## Target users
| User | Who they are and what they want | Context |
|---|---|---|
| **Platform owner** | The candidate. Builds, runs, demos and explains the platform. Wants a reliable one-command demo, measured results and a defensible design story. | Linux workstation: 16 cores, 31 GiB RAM (~17 GiB free), RTX 3090 24 GB. Expert Python, comfortable with Docker. Uses the platform daily during the build and again before the interview. |
| **Reviewer** | An interviewer or hiring engineer at a French-speaking employer using GitLab, Kubernetes, Airflow, MLflow, Kafka, Spark and Red Hat. Wants to judge design, craftsmanship and MLOps maturity quickly. | Reads the repo and README on GitLab in a browser, maybe watches a live demo. Spends 5–30 minutes. May read French. |
| **Data scientist** | Adds features, runs experiments and reads results using the shared code and images. Wants reproducibility and fast, safe iteration. | Runs notebooks on the same image as the pipelines. Comfortable with Python and pandas, less so with Kubernetes. |
| **Fraud analyst** (simulated) | Works the review queue. Wants a short, accurate explanation of why a Transaction was flagged. | Reads Case summaries in English or French. A simulated role: no human UI beyond stored summaries and the API. |

## Goals
1. Recreate the four production problems end to end, each with logged ground truth: covariate Shift, label Shift, concept Shift and the Degenerate feedback loop.
2. Detect every injected Shift automatically and measure detection lag and false alarms.
3. Retrain and promote safely: Stateless vs Stateful and IPW vs naive, compared with measured cost and quality, through Shadow mode, Canary and an audited Promotion gate.
4. Guarantee train/serve feature parity and exactly-once scoring effects.
5. Give the Platform owner one unattended command that produces a results report (Part A).
6. Show a production-shaped delivery path: gated CI, image security, GitOps to staging and prod, and pipeline orchestration per environment (Part B).
7. Deploy and evaluate a self-hosted LLM feature with versioned prompts and a CI eval gate (Part B).
8. Describe a portable GCP target as validated infrastructure-as-code with a costed build-vs-buy analysis (Part B).
9. Let a Reviewer understand the system, its maturity (Google Cloud MLOps Level 2) and its limits from the repo alone.

## Non-goals
- **Real fraud performance.** The data is synthetic (Sparkov). Absolute metrics are not real-world claims; only relative changes under Shift matter (ADR-0001).
- **Running anything in the cloud.** The GCP code is never planned or applied (ADR-0003).
- **Production security.** Local services have no authentication; the "real client" document covers what would change (DISCOVERY Q22).
- **High availability or scale.** One Kafka broker and one node. About 85 Transactions/s is enough (RESEARCH §02).
- **A user interface for Fraud analysts.** Case summaries are stored and served by API only.
- **Breadth over depth.** Java, Keras/TensorFlow, Jenkins, Oracle, Elasticsearch, MongoDB and Databricks are not used (DISCOVERY Q31).
- **A feature store, Delta Lake or DVC.** These are replaced by the shared feature function and immutable Data snapshots, and explained in build-vs-buy (ADR-0002, DISCOVERY Q30, Q35).
- **LLM fine-tuning.** The LLM feature only shows serving, prompt versioning and evaluation.

## Success metrics
| Metric | Target | How it is measured |
|---|---|---|
| Part A demo runtime | `make demo` completes unattended in ≤ 60 min on the host | Wall time logged by the demo script and recorded in the README |
| Covariate Shift detection lag | ≤ 3 sim days from Shift start to first Alert | `shift_events` start vs first matching Alert, in the results report |
| Label Shift detection lag | ≤ 3 sim days (action-rate Alert) | Same, results report |
| Concept Shift detection lag | ≤ 21 sim days (action-rate, early-fraud or concept-gap Alert) | Same, results report |
| False alarms | ≤ 1 Alert per 30 sim days across all rules, over the 7-month null replay | Null replay report |
| Exactly-once scoring | 0 duplicate and 0 missing Champion predictions after a forced crash and restart | Integration test assertion |
| Train/serve parity | 100% identical feature vectors and scores on the parity fixture | Parity test |
| Alert budget adherence (no Shift) | Block share 0.3% ± 0.1 pp, review share 0.7% ± 0.2 pp | Decision mix query over 30 sim days |
| Feedback-loop measurement | Oracle / naive / IPW table reported with Kish ESS and 95% bootstrap CIs | Results report |
| Promotion safety | ≥ 1 promotion in the demo; a deliberately worse Challenger is rejected and no alias moves | Results report promotion history; gate test |
| ONNX speed-up | ONNX single-row p50 ≥ 2× faster than native at identical Decisions | Benchmark run in MLflow |
| Scoring latency in staging | `/score` p99 ≤ 50 ms at 200 req/s for 2 min, error rate < 0.1% | k6 verify job |
| Memory footprint | Compose ≤ 6 GiB; kind steady state ≤ 13 GiB with vLLM off | `docker stats` / `kubectl top`, recorded in the resources doc |
| CI duration | MR pipeline (lint, test, Sonar, build, scan, model gate) ≤ 20 min on the self-hosted runner | GitLab pipeline duration |
| Code quality | SonarQube Cloud "Sonar way" gate passes on `main` (≥ 80% coverage on new code) | Sonar dashboard |
| Image security | 0 fixable HIGH/CRITICAL vulnerabilities in shipped images, apart from justified exceptions | Trivy gate |
| Case summary quality | 100% of Golden set cases pass the deterministic hard checks; summary p95 latency ≤ 5 s | Eval run in MLflow; LLM ops doc |
| IaC hygiene | fmt, validate, `terraform test`, tflint and checkov green, with every checkov skip justified | CI `iac` stage |
| Walkthrough | The 2-minute walkthrough reads aloud in ≤ 2:15, and every number comes from a report | Rehearsal log |

## User stories
Grouped by feature area. IDs are stable: `US-<n>`.

### A. Setup and data
**US-1** As a Platform owner, I want to start the local platform with one command, so that I can work and demo without manual setup.
- Given Docker is installed and `.env` exists, when I run the start command, then Postgres, Kafka, the S3-compatible object store and MLflow all report healthy within 3 min.
- Given the platform is running, when I check memory, then total use is ≤ 6 GiB.
- Given a required environment value is missing, when a service starts, then it exits with a message that names the missing setting.

**US-2** As a Platform owner, I want to ingest the downloaded dataset into a validated, immutable Data snapshot, so that every later step uses the same verified data.
- Given the two Kaggle files are in place, when I run ingest, then bronze and silver snapshots are written with a manifest and its SHA-256 printed, and the 2019 / 2020 row counts are 924,850 / 927,544.
- Given a file is truncated (wrong row count), when I run ingest, then it stops before writing anything and reports the expected and actual counts.
- Given a snapshot id already exists, when I run ingest with that id, then it refuses to overwrite.

**US-3** As a Platform owner, I want schema and value violations to stop a pipeline, so that bad data never trains or scores a model.
- Given input with a wrong type, an unknown category, a negative amount or an extra column, when validation runs (ingest, retraining or batch scoring), then the step aborts with the failing checks listed and a schema-skew Alert is recorded.

**US-4** As a Platform owner, I want card numbers never stored or shown in clear, so that the platform follows good PII practice even on synthetic data.
- Given any stored table, topic, log or Case summary, when I search for raw card numbers, then none is found; cards appear only as keyed hashes.
- Given the hashing key changes, when data is re-ingested, then all hashes change consistently.

**US-5** As a Platform owner, I want every training run linked to its exact Data snapshot hash, code version, image and trigger, so that any model can be reproduced and explained.
- Given a registered model version, when I open its MLflow run, then I see the snapshot URI, the full SHA-256 data hash, the git SHA, the image digest, the config and the trigger reason (Alert ids or schedule).

### B. Features and the initial Champion
**US-6** As a Platform owner, I want to train and register an initial Champion on 2019 with frozen thresholds for the Alert budget, so that scoring starts from a known model.
- Given the 2019 gold data, when I run initial training, then version 1 is registered as Champion with block and review thresholds (0.3% / 0.7% of validation scores), a calibrator, a drift reference profile, and train and test metrics.
- Given a Champion already exists, when I run initial training again, then a new version is registered but the Champion alias does not move.

**US-7** As a Data scientist, I want training and serving to compute features with the same code, so that there is no train/serve skew.
- Given a card's Transactions, when they go through the offline builder and through the streaming scorer, then the feature vectors and scores are identical.
- Given the input rows are shuffled before the offline build, when it runs, then the result is unchanged.
- Given a feature needs "now", when it is computed, then it uses the Transaction's event time, never the wall clock.

**US-8** As a Data scientist, I want a documented, safe way to add a feature, so that I can iterate without breaking serving.
- Given the guide, when I add a feature and run the tests, then the parity and safety tests tell me whether serving and training still agree.
- Given a new category level appears, when features are encoded, then existing codes keep their values (frozen vocabulary).

### C. Streaming scoring and Decisions
**US-9** As a Platform owner, I want to replay 2020 on an accelerated Simulated clock that I can pause, resume and restart, so that months of traffic fit in a demo.
- Given a Replay window and a speed-up, when the replay runs, then Transactions are produced in event-time order at the requested pace, keyed by card.
- Given the replay is paused, when one second passes, then no new Transactions are produced, and resuming continues from the same point.
- Given the replayer restarts, when it resumes, then no Transaction is produced twice.

**US-10** As a Platform owner, I want every Transaction scored and given a Decision, so that the platform acts on its predictions.
- Given a Champion is registered, when a Transaction arrives, then a prediction with score, Decision, would-be Decision, model version and event time is stored.
- Given no Challenger exists, when scoring runs, then only Champion rows are written and the service stays healthy.
- Given 30 sim days with no Shift, when I query the Decision mix, then blocks are 0.3% ± 0.1 pp and reviews 0.7% ± 0.2 pp.

**US-11** As a Platform owner, I want scoring to survive crashes without losing or duplicating results, so that metrics and Labels stay correct.
- Given the scorer is killed mid-batch, when it restarts, then every replayed Transaction has exactly one Champion prediction and per-card state is as if no crash happened.

**US-12** As a Platform owner, I want a dry-run scoring API, so that tests and load tests never corrupt live state.
- Given a request to the scoring API, when it is processed, then a Decision is returned and no prediction, card state or offset is written.
- Given a malformed request, when it is sent, then a 4xx validation error is returned and nothing is written.

**US-13** As a Platform owner, I want the scorer to pick up a new Champion or Challenger without restarting, so that promotions take effect quickly.
- Given an alias moves in the registry, when ≤ 30 s pass, then new predictions carry the new model version and in-flight scoring is uninterrupted.

**US-14** As a Platform owner, I want health, readiness and metrics endpoints on every service, so that Compose and Kubernetes can manage them and I can observe them.
- Given a service has not loaded its model or partitions yet, when readiness is checked, then it reports not ready; once loaded, it reports ready.
- Given the scorer is running, when metrics are scraped, then latency, throughput, errors and consumer lag are exposed.

### D. Labels and the Degenerate feedback loop
**US-15** As a Platform owner, I want a random Exploration sample of would-be blocks to be allowed and recorded with its propensity, so that blocked fraud can still be measured.
- Given a would-be block, when the Decision is made, then with probability equal to the exploration rate it becomes allow, flagged explored, with the propensity stored.
- Given the same Transaction is scored again, when the draw is repeated, then the result is the same (deterministic).
- Given Canary is active, when the exploration draw happens, then it happens before routing and applies to both Champion and Challenger.

**US-16** As a Platform owner, I want the exploration rate to be configurable, so that the demo can show a measurable effect and the default stays realistic.
- Given the demo profile, when the platform runs, then exploration is 10%; otherwise it is 2%.

**US-17** As a Platform owner, I want Labels to arrive with realistic delays and never for unexplored blocks, so that the simulation reproduces delayed and censored Labels.
- Given an allowed fraudulent Transaction, when 7–45 sim days pass, then a fraud Label arrives (chargeback).
- Given an allowed legitimate Transaction, when 45 sim days pass, then a legitimate Label is confirmed.
- Given a reviewed Transaction, when 1 sim day passes, then its Label arrives.
- Given a blocked, unexplored Transaction, when any time passes, then no Label ever arrives.

**US-18** As a Platform owner, I want retraining data built only from Matured labels, with explored rows weighted by inverse propensity, so that Challengers are trained without the feedback-loop bias.
- Given a retraining as-of date, when the dataset is built, then it holds only Labels matured by that date, unlabelled blocks are dropped, and explored rows carry weight 1/propensity.
- Given the naive option, when the dataset is built, then all weights are 1.

**US-19** As a Platform owner, I want the feedback-loop bias measured against oracle truth, so that I can quantify it in the interview.
- Given a replay with exploration, when the report runs, then it shows a 2 × 3 table (naive- and IPW-trained Challengers × oracle, naive and IPW evaluation) with Kish ESS and 95% bootstrap CIs.

**US-20** As a Platform owner, I want oracle truth kept out of features and training, so that the simulation never cheats.
- Given any feature, training or scoring code, when the boundary test runs, then it fails if that code reads oracle truth.

### E. Shift injection
**US-21** As a Platform owner, I want to switch covariate, label and concept Shifts on and off (scheduled or live), with ground truth logged, so that detection can be scored.
- Given the covariate Shift is on, when Transactions in gas_transport and grocery_pos are produced, then their amounts are ×1.8 (ramped) and each change is audited.
- Given the label Shift is on, when it runs, then the fraud rate roughly triples through whole cloned fraud episodes, and cloned rows are audited.
- Given the concept Shift is on, when it runs, then tiny night-time `home` Transactions labelled fraud are added across many cards, and they are audited.
- Given any Shift toggles, when I query ground truth, then its kind, parameters, seed and sim start/end are recorded.

**US-22** As a Platform owner, I want proof that the concept Shift is a real blind spot for the Champion, so that the demo story is honest.
- Given the trained Champion, when the blind-spot check runs, then fewer than 10% of synthetic concept rows score above the review threshold, or the check reports which parameter to change and the tuned values are recorded.

**US-23** As a Platform owner, I want a null replay that measures false alarms, so that detection lag is reported against a baseline.
- Given a replay with no Shifts, when it completes, then a report lists false Alerts per rule and the statistical null thresholds next to the configured ones.

### F. Monitoring and Alerts
**US-24** As a Platform owner, I want feature and per-category drift detected each sim day, so that covariate Shift raises an Alert.
- Given the covariate Shift starts, when ≤ 3 sim days pass, then a drift Alert naming the amount feature (global or per-category) is recorded.
- Given a single noisy window, when only one window breaches a rule, then no Alert fires (2 consecutive windows required).

**US-25** As a Platform owner, I want block and review rates at the frozen thresholds monitored against the Alert budget, so that label and concept Shifts are caught before Labels mature.
- Given the label Shift starts, when ≤ 3 sim days pass, then an action-rate Alert is recorded.

**US-26** As a Platform owner, I want estimators that work before Labels mature, so that performance problems are visible early.
- Given the label Shift is on, when Labels are only partly matured, then the delay-corrected fraud-rate estimate raises an Alert before day 45.
- Given matured performance diverges from the calibrated estimate, when the gap persists, then a concept-gap Alert is recorded.
- Given Transactions at least 45 sim days old, when the daily job runs, then matured precision and fraud-dollar recall are recorded.

**US-27** As a Platform owner, I want operational metrics recorded alongside drift, so that system health is part of monitoring.
- Given the scorer is running, when a window closes, then throughput, p50/p99 latency, error count and consumer lag for that window are stored.

**US-28** As a Platform owner, I want each Alert stored and passed reliably to the pipeline trigger, so that retraining starts automatically.
- Given an Alert is committed, when the trigger runs, then exactly one retraining request is created for that Alert and sim date.
- Given the orchestrator API is down, when an Alert is raised, then the request is retried until delivered and no Alert is lost.

### G. Continual learning and promotion
**US-29** As a Platform owner, I want retraining to start on a drift Alert or every 30 sim days, with the replay paused while it runs, so that the simulation and pipelines stay in step.
- Given a drift Alert or a 30-sim-day boundary, when the trigger fires, then the replay pauses, Challengers are trained and registered, and the replay resumes.
- Given several triggers arrive during one run, when they are processed, then they merge into the next run rather than queueing duplicates.

**US-30** As a Platform owner, I want Stateless and Stateful Challengers (and a naive comparison model) trained on every trigger, so that I can compare them with measured numbers.
- Given a trigger, when retraining completes, then the stateless, stateful and naive runs are logged with mode, parent version, tree count, wall-clock and CPU time, and train and test metrics.
- Given the Stateful model's tree count exceeds 2× the baseline, when retraining runs, then it falls back to Stateless and records why.
- Given the comparison, when the report runs, then it shows compute, quality and tree count per cycle for both modes.

**US-31** As a Platform owner, I want warm starts and encodings protected by tests, so that Stateful retraining cannot silently corrupt the model.
- Given a warm-started model, when it predicts with only the old trees, then the output equals the parent model's.
- Given training on a fixture, when it runs, then loss decreases and no NaN or infinite value appears in features, predictions or evaluation history.

**US-32** As a Platform owner, I want the best Challenger to run in Shadow mode, so that it is evaluated on live traffic without affecting Decisions.
- Given a Challenger alias is set, when Transactions are scored, then Challenger predictions are stored as not applied and Decisions come only from the Champion.

**US-33** As a Platform owner, I want a Promotion gate on Matured labels with segment checks, so that only better models advance.
- Given the shadow window ends, when evaluation runs, then Champion and Challenger are compared at equal Alert budget (each with its own threshold), IPW-weighted, with confidence intervals.
- Given the Challenger gains ≥ 1 pp fraud-dollar recall, loses no PR-AUC, meets the p99 latency budget and regresses no segment (category, amount band, night/day) by more than 5 pp, when the gate decides, then it advances to Canary, and later to Champion.
- Given any rule fails, when the gate decides, then it rejects, no alias moves, and the reason is recorded.

**US-34** As a Platform owner, I want a 10% Canary routed by card, so that a Challenger's Decisions are tried on live traffic before full promotion.
- Given Canary at 10%, when Transactions are scored, then about 10% (± 1 pp) of cards get the Challenger's Decision, and each card stays in one arm.

**US-35** As a Platform owner, I want every promotion decision audited and rollback possible, so that model changes are traceable.
- Given any gate decision, when it completes, then an immutable record (metrics, rule version, window, git SHA), version tags and an append-only promotion row exist.
- Given a promotion, when it is applied, then the previous Champion keeps a rollback alias.

### H. Batch scoring and latency
**US-36** As a Platform owner, I want per-Account risk scores every 7 sim days, so that the platform covers batch prediction as well as online scoring.
- Given a 7-sim-day boundary, when batch scoring runs, then every active Account has a risk score stored for that as-of date, after the input passes validation.

**US-37** As a Platform owner, I want ONNX and native latency compared fairly with proven parity, so that I can quote a measured speed-up.
- Given a Champion, when the benchmark runs, then ONNX and native Decisions are identical at the stored thresholds, scores differ by < 1e-5, and single-row p50/p99/p99.9 and batch numbers are logged under identical threading.

### I. Demo, reporting and model documentation
**US-38** As a Platform owner, I want one unattended demo command, so that I can produce the full story on demand.
- Given the dataset is downloaded, when I run the demo, then it starts the platform, trains the Champion, replays the demo window with scheduled Shifts, retrains, promotes, and writes the results report, all within 60 min and with no intervention.
- Given the demo is interrupted, when I run it again, then it resumes without repeating finished steps.

**US-39** As a Platform owner, I want a results report with every headline number, so that the README and walkthrough cite measured facts.
- Given a demo run, when the report is generated, then it includes detection lag and false alarms per Shift, Stateless vs Stateful, the oracle / naive / IPW table, ONNX vs native latency, promotion history and the Decision mix over time, with charts.

**US-40** As a Reviewer, I want a model card, so that I can see intended use, data, excluded attributes and limits.
- Given the latest report, when I read the model card, then every number matches it and the excluded protected attributes and simulation compressions are stated.

### J. CI and code quality
**US-41** As a Platform owner, I want a CI pipeline on my own runner that lints, tests, checks API contracts and enforces the Sonar quality gate on every merge request, so that quality is enforced automatically without paid CI minutes.
- Given an MR, when the pipeline runs, then lint, type checks, unit and contract tests and the Sonar gate run on the self-hosted runner and the MR shows test results.
- Given the scoring API changes without updating its published contract, when CI runs, then it fails.

**US-42** As a Platform owner, I want images built on Red Hat UBI, scanned and inventoried, so that shipped images are secure and traceable.
- Given a service change, when CI runs, then only the affected images are built, as non-root on UBI.
- Given an image with a fixable HIGH or CRITICAL vulnerability, when it is scanned, then the pipeline fails, unless the CVE has a justified, dated exception.
- Given any build, when it completes, then an SBOM is attached.

**US-43** As a Platform owner, I want a model quality gate in CI, so that code changes cannot silently degrade the model.
- Given a change, when the gate trains on the fixed sample, then it fails if PR-AUC is below the floor, or if convergence, NaN, warm-start or ONNX parity tests fail.

**US-44** As a Platform owner, I want only gated images pushed to the registry, so that deployable artifacts have passed every check.
- Given a `main` pipeline, when all gates pass, then images are pushed with immutable commit tags and their digests recorded; when any gate fails, nothing is pushed.

**US-45** As a Platform owner, I want the self-hosted runners configured safely for a public repo, so that my machine is not exposed.
- Given the runner setup guide, when I follow it, then no runner is privileged or has the Docker socket, GPU and cluster runners are protected, and allowed images are restricted.

**US-46** As a Reviewer, I want secret-detection and SAST reports on merge requests, so that security practice is visible.
- Given an MR, when CI runs, then secret-detection and SAST reports are attached.

### K. GitOps, environments and release
**US-47** As a Platform owner, I want to create the local Kubernetes cluster and check prerequisites with commands, so that Part B is reproducible.
- Given a missing prerequisite (kind, inotify limits, NVIDIA toolkit), when I run the prerequisite check, then it names the missing step.
- Given the prerequisites are met, when I create the cluster, then it is Ready with the namespaces and Argo CD installed, and re-running is harmless.

**US-48** As a Platform owner, I want shared platform services and per-environment apps that fit the machine's memory, so that staging and prod both run locally.
- Given the cluster, when the platform is synced, then one Kafka, Postgres (a database per env), MLflow, object store and Airflow serve both environments, with topics and consumer groups prefixed per env.
- Given vLLM is off, when steady state is measured, then memory use is ≤ 13 GiB, and the resources doc records measured vs estimated numbers.

**US-49** As a Platform owner, I want a merge to `main` to deploy staging automatically and verify it, so that every change is tested in a production-like environment.
- Given gated images are pushed, when the deploy step runs, then image tags are updated in Git without retriggering CI, Argo CD syncs staging to that commit, and the verify jobs pass: model loads in the serving image, resources fit, contract tests pass, and p99 ≤ 50 ms at 200 req/s.

**US-50** As a Platform owner, I want CI to stay usable when my local cluster is off, so that pipelines never hang.
- Given the cluster is down, when a `main` pipeline runs, then the verify jobs are manual and non-blocking rather than pending forever.

**US-51** As a Platform owner, I want to release to prod by tagging only after several green staging runs, so that prod gets exactly what staging verified.
- Given the last 3 staging verifications passed, when I release a version, then the bump commit is tagged and prod syncs to the same image digests.
- Given any of them failed, when I release, then the release is refused with the reason.

### L. Orchestration and data platform
**US-52** As a Platform owner, I want event-triggered pipelines per environment with their own pipeline-code version, so that pipeline changes go through staging before prod.
- Given a drift Alert in staging, when it is raised, then the staging training pipeline runs within 3 sim days with the Alert id recorded in its run.
- Given a commit to `main`, when pipelines refresh, then only staging pipelines change version; prod changes only on a release tag.
- Given heavy jobs (Spark, training), when several are queued, then only one runs at a time.

**US-53** As a Data scientist, I want the batch layer on Spark to produce the same gold data as the pandas path, so that scale-out does not change results.
- Given the fixture, when gold is built with Spark and with pandas, then the outputs are identical.
- Given silver data violating the contract, when the Spark job validates it, then it aborts with a schema-skew Alert.

**US-54** As a Data scientist, I want notebooks on the same image as the pipelines, so that experiments behave like production.
- Given the notebook command, when it starts, then the notebook runs on the jobs image against the platform services, and the guide's example runs end to end.

**US-55** As a Platform owner, I want an optional trigger when enough new Matured labels exist, so that the "new data" retraining trigger can be shown.
- Given the option is on, when 7 sim days of Labels mature, then a training run lists that event among its triggers.

### M. LLM Case summaries
**US-56** As a Fraud analyst, I want a short, accurate Case summary for each flagged Transaction in English or French, so that I can review it quickly.
- Given a review or block Decision and a running LLM, when ≤ 1 min passes, then a structured Case summary (summary, risk factors, recommended action equal to the platform Decision, language) is stored.
- Given a requested language of French, when the summary is generated, then it is in French with French number formatting.
- Given any summary, when checked, then every number and named entity comes from the Transaction data and no card number appears.

**US-57** As a Platform owner, I want summaries to wait, not fail, when the LLM is scaled to zero, so that memory can be freed without breaking the platform.
- Given the LLM is off, when flagged Transactions arrive, then they are marked pending and processed after the LLM returns, and the service stays healthy.

**US-58** As a Platform owner, I want summary prompts versioned in the registry and protected against injection, so that prompt changes are controlled and safe.
- Given a new prompt version, when it is registered, then it does not become the production version until it passes the eval gate.
- Given a merchant name that contains instructions, when a summary is generated, then the instructions are ignored and the Golden set adversarial case passes.

**US-59** As a Platform owner, I want prompt and model changes gated by a Golden set evaluation on my GPU runner, so that LLM regressions are blocked like code regressions.
- Given an MR changing the prompt or model, when the LLM gate runs, then deterministic checks (schema, grounded numbers and entities, Decision match, language, no card number) must all pass, the advisory judge score is recorded, and a per-case regression list and HTML report are attached.
- Given every case errors (e.g. LLM unreachable), when the gate runs, then it reports critical and blocks.

**US-60** As a Platform owner, I want the LLM served on the GPU through Kubernetes, or through a documented fallback, so that GPU serving is shown honestly.
- Given the GPU spike succeeds, when the LLM is scaled up, then it runs as a Kubernetes workload with one GPU allocated; otherwise it runs as a container reachable from the cluster, and the decision record says why.
- Given the LLM is started, when it is ready, then it serves the configured model within the memory budget, and scaling it down returns memory to the vLLM-off budget.

**US-61** As a Platform owner, I want LLM latency, throughput and cost measured, so that I can discuss LLMOps trade-offs with numbers.
- Given a benchmark at the target p95, when it runs, then time-to-first-token, end-to-end p95, tokens/s and cost per 1,000 summaries are documented.

**US-62** As a Platform owner, I want an optional bake-off between candidate models, so that the model choice is evidence-based.
- Given the bake-off runs, when it completes, then pass rates, judge scores and latency for each candidate are reported with a stated choice.

### N. Cloud infrastructure-as-code
**US-63** As a Reviewer, I want the GCP target written as validated, tested Terraform, so that I can judge cloud design without anything being deployed.
- Given an MR touching infrastructure code, when CI runs, then fmt, validate (no credentials), mocked tests, tflint and checkov pass, and every checkov skip has a written justification.
- Given a Cloud SQL definition missing the required edition, when tests run, then they fail.

**US-64** As a Reviewer, I want the same deployment charts to target GKE through overlays, so that the portability claim is credible.
- Given the GKE overlays, when the charts are rendered, then they render without errors, including workload identity annotations, the database proxy sidecar, external secrets and GPU node selection.

**US-65** As a Reviewer, I want a costed target architecture, so that I can see the cloud trade-offs.
- Given the GCP doc, when I read it, then it shows the target architecture, the kind → GKE mapping, dated monthly costs (always-on vs GPU scaled to zero) and managed-service alternatives.

### O. Documentation and interview pack
**US-66** As a Reviewer, I want a README that explains the system, results and limits and lets me run Part A, so that I can assess the project quickly.
- Given the README, when I follow the quick start, then Part A runs. Every number matches a report, every link resolves, and a French summary is included.

**US-67** As a Reviewer, I want a build-vs-buy analysis, so that I can see the Platform owner's judgement about managed services.
- Given the document, when I read it, then every excluded tool or service from the plan appears with when it would be bought instead, and the cost where known.

**US-68** As a Reviewer, I want the platform mapped to Google Cloud MLOps Level 2, so that I can judge its maturity against a known standard.
- Given the mapping, when I read it, then every Level 1 and Level 2 component, stage and CI/CD test item points to a concrete artifact, with status and reasons, and the omitted feature store is explained.

**US-69** As a Reviewer, I want to know what would change for a real client, so that I can judge production readiness thinking.
- Given the document, when I read it, then it covers access control, secrets, PII, HA, scaling, governance, data residency, real dispute windows and feedback-loop policy approval.

**US-70** As a Platform owner, I want a 2-minute walkthrough in English and French plus talking points, so that I can present confidently.
- Given the walkthrough, when I read it aloud, then it takes ≤ 2:15 and every number comes from a report.

**US-71** As a Platform owner, I want a fresh-clone rehearsal log, so that I know the demo works on the day.
- Given a fresh clone, when I follow the docs, then each built phase's checkpoint passes and the timings are recorded.

**US-72** As a Platform owner, I want an optional dashboard of Alerts, Decisions, drift and promotions, so that a live demo is visual.
- Given the dashboard is deployed, when a staging replay runs, then its panels fill with live data.

## Non-functional requirements
| Category | Requirement |
|---|---|
| Performance | Scoring `/score` p99 ≤ 50 ms at 200 req/s (staging). Streaming keeps up with ≥ 1 sim day per 10 s (about 250 Transactions/s) with consumer lag < 1 sim hour. Native single-row scoring uses one thread. |
| Reliability | Exactly-once effects into the database across crashes and rebalances. The replay resumes after restarts. Pipeline trigger requests survive orchestrator outages (outbox with retries). Kafka partition count for transactions is fixed for life. |
| Resource limits | Every container and pod has memory requests and limits. Compose ≤ 6 GiB; kind ≤ 13 GiB steady with vLLM off, ≤ 19 GiB peak with vLLM on. Heavy jobs are serialised. vLLM uses ≤ 85% of GPU memory. |
| Security | No secrets in Git (`.env` and Secrets out of band). Images non-root on UBI. No privileged CI runners. Scanner images pinned by digest. Services local-only (bound to 127.0.0.1); LLM endpoint cluster-internal. |
| Privacy | Card numbers stored only as keyed hashes. Protected attributes (gender, date of birth) and names and addresses excluded from features. Case summaries never contain card numbers. |
| Reproducibility | Immutable Data snapshots with SHA-256 manifests. Every model run records data hash, git SHA, image digest, config and trigger. Pinned tool versions (DISCOVERY / RESEARCH versions). Deterministic seeds for exploration, label delays and Shifts. |
| Portability | Services are configured only by environment variables. The same images and charts run on Compose, kind and (by overlay) GKE. |
| Observability | Health, readiness and metrics endpoints on every service. Drift, operational metrics, Alerts and promotions in the database. Model and LLM runs and traces in MLflow. |
| Localization | Code and docs in English, plus a French summary in the README and a French walkthrough. Case summaries in English or French on request. |
| Supported platforms | Linux x86-64 host with Docker, Python 3.13 and an NVIDIA GPU (24 GB) for the LLM. GitLab.com for CI. |
| Accessibility | No custom UI. Reports and docs are Markdown with alt text on charts. |

## Implementation decisions
- **Dataset:** Sparkov synthetic card data; train on 2019, replay 2020; card number = Account (ADR-0001; DISCOVERY Q4, Q9, Q10; RESEARCH §01).
- **Shared feature function:** one pure per-card state-update function used by offline replay (pandas, later Spark) and streaming scoring. No feature store (ADR-0002 + amendment; DISCOVERY Q15, Q35).
- **Exactly-once scoring:** predictions, card state and Kafka offsets committed in one database transaction per micro-batch. Kafka keyed by hashed card with the Java-compatible partitioner (DISCOVERY Q44; RESEARCH §02).
- **Model:** LightGBM with isotonic calibration. Thresholds and drift reference stored with each version. Stateful retraining = warm start with a tree cap. Stable categorical codes from a frozen vocabulary (DISCOVERY Q14, Q20, Q39; RESEARCH §03).
- **Decision policy:** Alert budget 0.3% block / 0.7% review with frozen thresholds. Deterministic exploration (default 2%, demo 10%) drawn before Canary routing (DISCOVERY Q12, Q40; RESEARCH §01, §03).
- **Labels:** chargebacks 7–45 sim days, legit confirmed at 45, reviews at 1 day, unexplored blocks never. Oracle truth kept in a separate store read only by evaluation (DISCOVERY Q11; PLAN Decision 7).
- **Monitoring:** custom PSI/KS with stored reference edges, per-category drift, fixed-cutoff action-rate tests, delay-corrected counts, calibrated-estimate gap and matured performance. 2-window streaks (DISCOVERY Q19, Q39; RESEARCH §01).
- **Promotion:** Shadow mode → Canary 10% by card → alias move, through a Promotion gate on Matured labels with IPW, segment checks and an audit trail (DISCOVERY Q16; RESEARCH §03).
- **Orchestration:** a `PipelineTrigger` interface with a local adapter in Part A and an Airflow asset-event adapter in Part B. Airflow 3 with LocalExecutor and Kubernetes pod tasks, one pipeline-code version per environment (DISCOVERY Q20, Q41; PLAN Decisions 2, 10; RESEARCH §04).
- **Storage:** PostgreSQL (a database per environment) and an S3-compatible object store (SeaweedFS); MLflow 3.16 for tracking, registry and prompts (ADR-0005; DISCOVERY Q23, Q28, Q36, Q37).
- **Runtime:** Docker Compose for Part A; kind + Helm + Argo CD for Part B, with shared platform services and per-env apps; GPU topology per ADR-0006 (ADR-0003 + amendment; DISCOVERY Q25, Q37, Q38).
- **Delivery:** public GitLab project, self-hosted runners, Buildah UBI images, Trivy, SonarQube Cloud, image tags bumped in Git, staging tracks `main`, prod tracks release tags (DISCOVERY Q27, Q34, Q43; RESEARCH §06; PLAN Decisions 9, 11).
- **LLM:** self-hosted Ministral 3 8B (FP8) on vLLM, scaled to zero by default. Structured output. Deterministic eval checks are the hard gate; the judge is advisory (ADR-0004; DISCOVERY Q28; RESEARCH §05; PLAN Decision 13).
- **Cloud:** Terraform for GKE Standard, Cloud SQL, GCS, Artifact Registry, Workload Identity Federation and Secret Manager with ESO. Validated, never applied (ADR-0003; DISCOVERY Q26; RESEARCH §07).

## Testing decisions
- **Good tests check behaviour you can see from outside:** Decisions, stored rows, Alerts, exit codes, API responses. They do not check internal helpers. Every success metric above has a test or a generated report behind it.
- **Unit tests (default run, CI):** features and their determinism, the Decision policy and exploration, label delays, drift statistics and streaks, IPW metrics, the Promotion gate rules, Shift injection, the trigger, LLM eval scorers and data contracts.
- **Property and parity tests:** train/serve feature parity (shuffled input), Spark vs pandas gold parity, pandas vs Spark schema parity, ONNX vs native Decision parity, warm-start invariance.
- **Safety tests:** convergence, no NaN or inf, stable encodings, the oracle import boundary, the concept blind-spot check.
- **Contract tests:** scoring API schema tests in-process (CI) and against staging, plus a published-contract diff.
- **Integration tests (local Compose, marked separately):** migrations, exactly-once under a forced crash, the labeler never labelling unexplored blocks, monitoring raising Alerts for a scheduled Shift.
- **End-to-end:** `make demo` is the Part A end-to-end test. Staging verification (sync, infra compatibility, contract, k6) is the Part B one.
- **Gates:** model quality gate (PR-AUC floor on a fixed sample), LLM eval gate (Golden set), Terraform tests with a mocked provider, Trivy, Sonar.
- **Not tested in CI:** the full stack, Terraform plan or apply, GPU serving except on the protected GPU runner.

## Assumptions and dependencies
- The Platform owner downloads the Sparkov dataset from Kaggle (needs a Kaggle account).
- A gitlab.com account and a **public** project; a SonarQube Cloud org; the GitLab registry allows anonymous pulls.
- Host prerequisites installed by the Platform owner (sudo): kind, kubectl, helm, raised inotify limits, NVIDIA Container Toolkit 1.20.1, Terraform 1.16, the GitLab Runner.
- Internet access for images, model weights (Hugging Face) and Terraform providers.
- Other GPU applications are stopped during LLM demos (about 1.9 GB VRAM otherwise in use).
- Versions as researched on 2026-10-04 stay available. MinIO is not, which is why SeaweedFS is used.
- Electricity and cloud prices are list prices on 2026-10-04 and must be re-checked before quoting.
- The interview date and number of build days are unknown. The plan assumes Part A ≈ 2 days and Part B ≈ 3–4 days, with Part B stoppable after any phase.

## Out of scope
- Everything listed in PLAN.md "Out of scope": excluded technologies, managed services (build-vs-buy only), cloud apply, Delta Lake/DVC, Redis/feature store, Kafka transactions, kube-prometheus-stack, real auth, HA, Autopilot, OpenTofu in CI, custom Sonar gates, LLM fine-tuning.
- A Fraud analyst UI.
- Real dispute windows (1–3 months). They are compressed to 7–45 sim days for the demo; documented.
- Multiple-seed statistical studies beyond 3 exploration seeds.

## Open questions
_None._ (Interview date and build days remain unknown; handled as a planning assumption under Assumptions and dependencies.)

## Story → plan map
| Story | Plan tasks |
|---|---|
| US-1 | P1.T1, P1.T2, P1.T3, P1.T4 |
| US-2 | P1.T6, P1.T7 |
| US-3 | P1.T5, P5.T1, P5.T6, P8.T2 |
| US-4 | P1.T7, P1.T9, P9.T5 |
| US-5 | P1.T7, P1.T8, P5.T1, P8.T5 |
| US-6 | P2.T2, P2.T3 |
| US-7 | P2.T1, P2.T2, P2.T4 |
| US-8 | P2.T1, P8.T7 |
| US-9 | P2.T6, P5.T5 |
| US-10 | P2.T7, P3.T1, P3.T2 |
| US-11 | P2.T7 |
| US-12 | P2.T7, P6.T7 |
| US-13 | P2.T7 |
| US-14 | P2.T7, P7.T7 |
| US-15 | P3.T1, P3.T2 |
| US-16 | P3.T1, P5.T9 |
| US-17 | P3.T3 |
| US-18 | P3.T4 |
| US-19 | P3.T5, P5.T8 |
| US-20 | P3.T6 |
| US-21 | P4.T1 |
| US-22 | P4.T7 |
| US-23 | P4.T5 |
| US-24 | P4.T2, P4.T3 |
| US-25 | P4.T3 |
| US-26 | P4.T4 |
| US-27 | P4.T3, P2.T7 |
| US-28 | P4.T6, P8.T6 |
| US-29 | P5.T1, P5.T5, P8.T5 |
| US-30 | P5.T1, P5.T8 |
| US-31 | P5.T2 |
| US-32 | P2.T7, P5.T3 |
| US-33 | P5.T3 |
| US-34 | P5.T4 |
| US-35 | P5.T3 |
| US-36 | P5.T6, P8.T5 |
| US-37 | P5.T7 |
| US-38 | P5.T9 |
| US-39 | P5.T8 |
| US-40 | P5.T10 |
| US-41 | P6.T2, P6.T3, P6.T7 |
| US-42 | P2.T5, P6.T4, P6.T5 |
| US-43 | P6.T6 |
| US-44 | P6.T8 |
| US-45 | P6.T1 |
| US-46 | P6.T9 |
| US-47 | P7.T1, P7.T3, P7.T4 |
| US-48 | P7.T5, P7.T6, P7.T7, P7.T10 |
| US-49 | P7.T7, P7.T8 |
| US-50 | P7.T8 |
| US-51 | P7.T9 |
| US-52 | P8.T3, P8.T4, P8.T5, P8.T6 |
| US-53 | P8.T1, P8.T2 |
| US-54 | P8.T7 |
| US-55 | P8.T8 |
| US-56 | P9.T2, P9.T3 |
| US-57 | P9.T3 |
| US-58 | P9.T2, P9.T4, P9.T6 |
| US-59 | P9.T4, P9.T5, P9.T6 |
| US-60 | P7.T2, P9.T1 |
| US-61 | P9.T8 |
| US-62 | P9.T7 |
| US-63 | P10.T1, P10.T2, P10.T3, P10.T4, P10.T6 |
| US-64 | P10.T5 |
| US-65 | P10.T7 |
| US-66 | P5.T11, P11.T1 |
| US-67 | P11.T2 |
| US-68 | P11.T3 |
| US-69 | P11.T4 |
| US-70 | P11.T6 |
| US-71 | P11.T7 |
| US-72 | P11.T5 |
