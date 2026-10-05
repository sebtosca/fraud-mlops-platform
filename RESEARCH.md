# Research: Fraud ML Platform

Generated: 2026-10-04 from DISCOVERY.md
Domains:
1. [Data, shift and label science](#01-data-shift-and-label-science) — `research/01-data-shift-labels.md`
2. [Streaming and the scoring service](#02-streaming-and-the-scoring-service) — `research/02-streaming-scoring.md`
3. [Model lifecycle](#03-model-lifecycle) — `research/03-model-lifecycle.md`
4. [Orchestration and data engineering](#04-orchestration-and-data-engineering) — `research/04-orchestration-data-eng.md`
5. [LLM serving and evaluation](#05-llm-serving-and-evaluation) — `research/05-llm-serving-eval.md`
6. [CI/CD and GitOps](#06-cicd-and-gitops) — `research/06-cicd-gitops.md`
7. [GCP Terraform](#07-gcp-terraform) — `research/07-gcp-terraform.md`
8. [Resource budget and MLOps Level 2 mapping](#08-resource-budget-and-mlops-level-2-mapping) — `research/08-resources-mlops-l2.md`

## Cross-cutting findings (coordinator)

_Written by the coordinator from the eight domain files below. Domain sections are appended verbatim; where this section and a domain disagree, the domain section and its sources win._

### A. Decisions in DISCOVERY.md that research says should change
These need the user's confirmation before PLAN.md; once confirmed they belong in DISCOVERY.md as a new round.

| # | Decision | Finding | Suggested change | Domains |
|---|---|---|---|---|
| A1 | Q30 object store = MinIO | MinIO community edition is archived; `minio/minio` no longer pullable from Docker Hub (checked 2026-10-04) | Replace with **SeaweedFS** (Apache-2.0, S3 API, versioning); new ADR; say "S3-compatible object store" in docs | 04, 08 |
| A2 | Q34 staging + prod namespaces | Full stack per namespace ≈ 31–35 GiB; does not fit | **Shared infra** (Kafka, Postgres with per-env DBs, MLflow, object store, Airflow, vLLM) in platform namespaces; only app services duplicated in `staging`/`prod`. vLLM `replicas: 0` by default (slim peak ≈ 12.8 GiB off / 18.8 GiB on) | 08, 05, 02 |
| A3 | Q25 Compose "only as inner loop" | Phase 1 on Compose ≈ 5–6 GiB with the same images and env config makes migration to kind cheap | **Phase 1 runs on Docker Compose**; Phase 2 moves the same images to kind. ADR-0003 amended | 08 |
| A4 | Q19 drift rules | Score PSI barely moves under label (0.001–0.008) or concept shift; ×1.8 amount shift only alerts via KS and only for some category pairs | Keep Q19 and **add per-category amount monitoring and fixed-cutoff block/review-rate monitoring** (binomial test); **freeze thresholds per model version** | 01 |
| A5 | Q12/Q18 exploration = 2% | ≈ 56 labelled blocked rows per replay-year; IPW numbers dominated by noise | Make the rate configurable; **demo profile 10%** (weight 10); report ESS + bootstrap CIs, consider SNIPS / weight clipping | 01 |
| A6 | Q10 clock (1 sim day ≈ 30 s) vs Q29 "nightly" batch and Q20 training | Pod + JVM start alone ≈ 30–90 s; a training run takes minutes | Airflow triggered by **asset events from the replayer**, no wall-clock schedules; batch scoring every K sim days (e.g. 7) or replay **pauses while gated jobs run** | 04 |
| A7 | Q13 concept shift = night-time card testing in a "safe" category | Night-time is Sparkov's strongest fraud signal (84.6% vs 23.2%), so the champion may already catch it | Keep `home` category, tiny amounts, but add an **acceptance test that the champion actually misses the pattern** before the demo relies on it; tune hour band/amounts if not | 01 |
| A8 | Q31 Red Hat via UBI + Python 3.13 | UBI ships Python 3.12 and 3.14 only | Choose: (a) ubi9-minimal + uv-installed 3.13, (b) pin project to 3.12 on `ubi9/python-312`, (c) move to 3.14. Coordinator leans (a) | 06 |
| A9 | ADR-0002 consequence "crash loses state since last snapshot" | Writing predictions, card state and offsets in **one Postgres transaction** gives exactly-once effects; nothing lost | Amend ADR-0002 wording; adopt that pattern | 02 |
| A10 | Q30 "data hash logged in MLflow" | `mlflow.data` digest is an 8-char MD5 of the first 10k rows | Log our own **SHA-256** of each snapshot manifest | 03 |

### B. Refinements (no decision change, but PLAN must include them)
- **Kafka keying:** confluent-kafka defaults to CRC32; set `partitioner=murmur2_random` everywhere. Put event time in the payload, **not** the Kafka timestamp (historical CreateTime triggers immediate retention deletes). Never change the `transactions` partition count (02).
- **Features vs ONNX/warm start:** encode categoricals as stable integer codes from a frozen vocabulary (pandas category codes silently corrupt warm-started models); keep high-cardinality columns (merchant, city, job) out of LightGBM categoricals (they can erase the ONNX speed-up). Add a warm-start invariance test (03).
- **Stateful retraining is additive boosting**, not fine-tuning: cap tree growth (forced stateless reset), optionally add `Booster.refit` as a third variant; benchmark with `num_threads=1` (03).
- **Train on 2019, replay 2020** (924,850 / 927,544 rows); avoid December for the label-shift scenario; the censoring effect only shows once replay months dominate the training window — plan the demo timeline for it (01).
- **Shadow comparison under censoring** is only fair on matured labels plus the exploration sample (03).
- **One GPU:** a single vLLM serves both envs; CI model-change evals must scale it down and swap models; prompt-only changes reuse it (05).
- **LLM model:** Ministral-3-8B-Instruct-2512 (FP8, Apache-2.0, French lab) primary; Qwen3.5-9B W4A16 fallback; deterministic scorers are the hard gate, LLM judge advisory; delimit transaction fields against prompt injection (05).
- **CI:** gitlab.com free minutes are 400/month, so run every job on the self-hosted runner; SonarQube Cloud Free fixes the gate at 80% coverage on new code; Buildah (Kaniko archived); Trivy pinned by digest (March 2026 compromise); Argo CD ApplicationSet, `staging` tracks `main`, `prod` tracks `v*` tags (06).
- **Terraform:** raw resources in thin local modules (official GKE module pins google `< 8`); Cloud SQL PG16+ needs `edition = "ENTERPRISE"` for `db-custom-*`; use current names (Workload Identity Federation for GKE, private nodes + DNS endpoint); dev ≈ $765/month always-on, ≈ $335 with GPU pool at zero (07).
- **Avoid Bitnami charts/images** (moved to `bitnamilegacy`, unmaintained); Airflow chart's embedded Postgres uses one — disable it (08).
- **MLOps Level 2:** 9 gap items listed in domain 08 (segment-level model validation, NaN test, infra-compatibility check, per-env DAG bundles, "N green staging runs before prod tag", …); feature store stays a documented omission (08).

### C. Host prerequisites the user must do themselves (sudo / accounts)
- Install **kind**, **Terraform** (or OpenTofu), **JDK 17/21** (host has Java 1.8; only needed for running Spark outside containers).
- Install and configure the **NVIDIA Container Toolkit** for Docker (host-wide runtime change) — required for nvkind; timebox nvkind to ~2 h, fallback is vLLM as a plain Docker container outside kind (weakens ADR-0004's "GPU scheduling on Kubernetes" evidence).
- Create the **gitlab.com** project, register self-hosted runners (new `glrt-` token flow), enable job-token push, create a **SonarQube Cloud** org, download **Sparkov** from Kaggle (verify 1,296,675 / 555,719 rows; some mirrors are truncated).
- Stop other GPU apps before LLM demos (~1.9 GB VRAM already in use).

### D. Risks to the timeline
- Phase 2 grew: GPU-in-kind, runner security, GitOps bump/race handling and Level 2 gaps each add work. With the interview date still unknown, PLAN should order Phase 2 so each step leaves a demoable state, and put nvkind behind a timebox.
- The laptop is a hard CI dependency (self-hosted runners, staging verify against kind); `verify` should be manual or `allow_failure` when the cluster is down.
- Volatile versions recorded across domains (2026-10-04): kind 0.33.0, Strimzi 1.2.0 / Kafka 4.3.1, Airflow 3.3.2 (chart 1.22.0), MLflow 3.16.1, PySpark 4.2.0 (pandas < 3), LightGBM 4.7.0, onnxruntime 1.30.0, pandera 0.33.1, vLLM 0.30.0, Argo CD 3.5.3, Terraform 1.16 / google provider 8.5, confluent-kafka 2.15.

---

## 01. Data, shift and label science

Researched 2026-10-04. Library versions checked on PyPI that day: scipy 1.18.1 (2026-08-21), numpy 2.5.3 (2026-09-06), lightgbm 4.7.0 (2026-07-18), evidently 0.7.23 (2026-09-11), nannyml 0.13.1 (2025-07-12, the latest release, more than a year old). The code snippets below were run locally with scipy 1.17.1 / numpy 2.4.6 (uv cache). They only use APIs that also exist in 1.18.

Dataset numbers marked **[measured]** were computed for this report with DuckDB over a byte-identical Hugging Face mirror of the Kaggle files ([NeerajCodz/creditCardFraudDetection](https://huggingface.co/datasets/NeerajCodz/creditCardFraudDetection), which has the same 23 columns and the same 1,296,675 / 555,719 rows as Kaggle). Nothing was saved to the repo. Re-run them on the real Kaggle files once they have been downloaded.

### Questions

1. Sparkov dataset: exact columns and types, row counts, fraud rate, date range, distinct cards/merchants/categories, per-category fraud rates, licence, canonical URLs, quirks (leaky columns, PII), and which categories suit the covariate shift and the concept-shift "safe" category.
2. How to inject each Shift cleanly on a replayed stream, with ground truth logged; literature and tooling.
3. PSI (formula, binning, epsilon, thresholds and where they come from, categorical PSI) and KS (p-values at large n, effect size, thresholds), with Python code.
4. Monitoring under delayed Labels: prediction-score drift and estimating performance before Labels arrive (CBPE/DLE). What is cheap and credible?
5. Degenerate feedback loops: IPW with an Exploration sample, off-policy evaluation of a Challenger under censored Labels, and how to measure naive vs IPW.

### Findings

#### Q1. Sparkov dataset

**Provenance and licence**

- Kaggle dataset `kartik2112/fraud-detection`, "Credit Card Transactions Fraud Detection Dataset" by Kartik Shenoy. It is at version 1, last updated 2020-08-05, and its licence is **CC0: Public Domain**. The files are `fraudTrain.csv` (351,238,196 B) and `fraudTest.csv` (150,354,339 B). Source: the Kaggle API metadata at `https://www.kaggle.com/api/v1/datasets/view/kartik2112/fraud-detection` and `.../datasets/list/kartik2112/fraud-detection`. Canonical page: https://www.kaggle.com/datasets/kartik2112/fraud-detection
  - Kaggle description: "This is a simulated credit card transaction dataset containing legitimate and fraud transactions from the duration 1st Jan 2019 - 31st Dec 2020. It covers credit cards of 1000 customers doing transactions with a pool of 800 merchants."
  - Kaggle description: "What I did was generate transactions across all profiles and then merged them together to create a more realistic representation of simulated transactions."
- The generator is **`namebrandon/Sparkov_Data_Generation`** by Brandon Harris (https://github.com/namebrandon/Sparkov_Data_Generation). It is **MIT** licensed ("Copyright (c) 2016-2022 Brandon Harris", https://github.com/namebrandon/Sparkov_Data_Generation/blob/master/LICENSE.md). The last push was 2022-06-22. The Kaggle data predates the 2022 refactor: the dataset author's own PR ("Minor changes made to code to make it compatible with Python3") was merged on 2020-07-28. The `namankumar/...` repo named in the brief is not the generator.

**Schema**

Types come from the HF mirror's parquet conversion: https://datasets-server.huggingface.co/first-rows?dataset=NeerajCodz/creditCardFraudDetection&config=default&split=test. There are 23 columns.

| column | type | note |
|---|---|---|
| `Unnamed: 0` | int64 | pandas row index; restarts at 0 in fraudTest. **Drop.** |
| `trans_date_trans_time` | string `YYYY-MM-DD HH:MM:SS` | naive timestamp. **Use as event time / Simulated clock.** |
| `cc_num` | int64 (float64 in some mirrors) | Account key; hash at ingestion (Q22) |
| `merchant` | string | every value has the prefix `fraud_` (1,852,394/1,852,394 rows) **[measured]**; strip it |
| `category` | string | 14 values |
| `amt` | float64 | USD |
| `first`, `last`, `gender`, `street`, `city`, `state`, `zip`, `lat`, `long`, `city_pop`, `job`, `dob` | string/int/float | customer attributes (Faker-generated PII look-alikes), static per card |
| `trans_num` | string (32-hex MD5) | unique transaction id; use as key, never as feature |
| `unix_time` | int64 | **offset by about 7 years**, see quirks |
| `merch_lat`, `merch_long` | float64 | uniform within about 1° of home (larger when travelling) |
| `is_fraud` | int64 0/1 | Label |

**Counts [measured]**

| | rows | fraud | fraud rate | first ts | last ts | cards | merchants | categories |
|---|---|---|---|---|---|---|---|---|
| fraudTrain | 1,296,675 | 7,506 | 0.579 % | 2019-01-01 00:00:18 | 2020-06-21 12:13:37 | 983 | 693 | 14 |
| fraudTest | 555,719 | 2,145 | 0.386 % | 2020-06-21 12:14:25 | 2020-12-31 23:59:34 | 924 | 693 | 14 |
| **combined** | **1,852,394** | **9,651** | **0.521 %** | | | **999** | **693** | **14** |
| 2019 (planned training year, Q10) | 924,850 | 5,220 | 0.564 % | | | | | |
| 2020 (planned replay) | 927,544 | 4,431 | 0.478 % | | | | | |

The Kaggle text says "1000 customers ... 800 merchants", but the data has 999 cards and 693 merchants. The files do not overlap in time and have no duplicate (ts, card, amt) rows. Average volume is **2,538 Transactions per sim-day** (min 1,073, max 6,530) with 13.2 frauds per day.

**Per-category stats [measured]** (2019 only, which is what the Champion will train on):

| category | rows | fraud % | median legit $ | median fraud $ | fraud at 22:00–03:59 | legit at night | legit p01–p99 $ | traffic share |
|---|---|---|---|---|---|---|---|---|
| home | 87,849 | **0.147** | 48.16 | **255.88** | 89.1 % | 16.6 % | 1.81–225.82 | 9.5 % |
| health_fitness | 61,115 | 0.157 | 42.89 | 19.73 | 77.1 % | 16.6 % | 1.54–226.39 | 6.6 % |
| food_dining | 65,461 | 0.159 | 42.06 | 118.63 | 82.7 % | 18.7 % | 1.27–222.66 | 7.1 % |
| kids_pets | 80,644 | 0.213 | 47.20 | 19.48 | 83.7 % | 16.6 % | 1.84–227.52 | 8.7 % |
| personal_care | 64,923 | 0.234 | 32.47 | 20.89 | 84.9 % | 16.5 % | 1.29–226.39 | 7.0 % |
| entertainment | 67,097 | 0.243 | 50.34 | 502.25 | 85.9 % | 19.5 % | 1.36–296.60 | 7.3 % |
| grocery_net | 32,320 | 0.291 | 51.05 | 12.75 | 80.9 % | 33.3 % | 11.00–117.57 | 3.5 % |
| travel | 29,011 | 0.296 | 6.26 | 9.68 | 83.7 % | 16.5 % | 1.10–1268.54 | 3.1 % |
| misc_pos | 56,879 | 0.299 | 13.65 | 8.90 | 82.9 % | 26.0 % | 1.12–654.10 | 6.2 % |
| gas_transport | 93,859 | 0.468 | 62.91 | 10.73 | 87.2 % | 33.2 % | **32.26–104.31** | **10.1 %** |
| shopping_pos | 83,205 | 0.701 | 7.68 | 867.28 | 80.8 % | 23.0 % | 1.09–1036.90 | 9.0 % |
| grocery_pos | 87,893 | 1.368 | 104.60 | 310.01 | 85.8 % | 33.2 % | **32.43–232.22** | **9.5 %** |
| misc_net | 45,040 | 1.397 | 9.69 | 790.45 | 85.2 % | 30.3 % | 1.12–710.51 | 4.9 % |
| shopping_net | 69,554 | 1.727 | 8.28 | 997.56 | 84.9 % | 23.7 % | 1.10–936.42 | 7.5 % |

Overall, 84.6 % of fraud happens between 22:00 and 03:59, against 23.2 % of legit Transactions **[measured]**.

**How the generator makes fraud** (from the source code; the mechanism explains most quirks)

- Each customer of a profile gets a fraud episode with probability about 99/101. The code is `fraud_flag = random.randint(0,100)` followed by `if fraud_flag < 99:`. The episode window is 1 day (`fraud_interval = random.randint(1,1) #7->1`), and **legit transactions on the fraud dates are suppressed**: `if (is_fraud == 0 and t[1] not in fraud_dates) or is_fraud == 1:` (https://github.com/namebrandon/Sparkov_Data_Generation/blob/master/datagen_transaction.py).
- Fraud is placed at night 80 % of the time. The code comment reads "#20% chance that the fraud will still occur during normal hours", and AM fraud is limited to `hr_end = 4` while PM fraud starts at `hr_start = 22` (https://github.com/namebrandon/Sparkov_Data_Generation/blob/master/profile_weights.py).
- Amounts are gamma-distributed per category, with `shape = mean²/sd²` and `scale = sd²/mean` (same file). The fraud profiles use separate means. For example, `fraud_adults_2550_female_rural.json` has `"shopping_net": {"mean": 1000, "stdev": 100}`, `"grocery_pos": {"mean": 350, "stdev": 20}`, `"gas_transport": {"mean": 10, "stdev": 2}` and `"home": {"mean": 250, "stdev": 25}`. Fraud profiles also generate 7–16 transactions a day, against 1–6 in the legit profile (https://github.com/namebrandon/Sparkov_Data_Generation/blob/master/profiles/fraud_adults_2550_female_rural.json).
- Volume follows date weights: `"holidays": {"start_date (MM-DD)": "11-30", "end_date (MM-DD)": "12-31", "weight": 200}` and `"post_holidays" ... "weight": 75` (same profile files). Fraud episode start dates are drawn uniformly (`rand_interval = random.randint(1, inter_val)`).

**Quirks and leakage, with the consequence for this project**

1. **Fraud comes in pure card-level episodes [measured].** 976 of 999 cards (97.7 %) have fraud. Each fraud card has on average 9.9 fraud Transactions (median 10) spread over 1.69 days (max 2.9). Of 1,927 card-days containing fraud, **only 1 also contains a legit Transaction**. Fraud card-days average 5.0 Transactions, against 3.3 for legit card-days. As a result:
   - Per-card velocity features and "card-day" aggregates are very strong.
   - A random (non-temporal) split leaks, because one episode lands on both sides. Keep the time split (Q10).
   - Any "card had fraud before" feature must be built from Matured labels as of event time. Otherwise it leaks the chargeback before it arrives.
2. **`unix_time` does not agree with `trans_date_trans_time` [measured].** `epoch(trans_date_trans_time) − unix_time` is always between 220,838,400 and 220,924,800 s (2,556–2,557 days, about 7 years). Example: the row at `2019-01-01 00:00:18` has `unix_time` 1325376018 = 2012-01-01 00:00:18 UTC. Use only `trans_date_trans_time` for the Simulated clock.
3. **`Unnamed: 0`** is a per-file row counter and restarts in fraudTest. It is monotonic in time, so it is a time proxy. Drop it.
4. **`trans_num`** is a random MD5. It is not leaky, but it is an id, so use it as the idempotency/join key for Labels and audit rows.
5. **Seasonality creates natural label shift [measured].** Monthly fraud rate goes from 0.963 % (2019-01) and 1.037 % (2019-02) down to 0.420 % (2019-12) and **0.185 % (2020-12)**. December volume roughly doubles: 141,060 rows in 2019-12 against about 70k in other months. The cause is that legit volume is holiday-weighted while fraud episode dates are uniform. The 2020 replay therefore contains an un-injected label shift in Dec 2020, and smaller swings elsewhere.
6. **The Kaggle train/test split is mid-day (2020-06-21 12:14).** The Q10 plan (train on 2019, replay 2020) must concatenate both files and sort by `trans_date_trans_time` with a stable sort.
7. **PII look-alikes.** `first`, `last`, `street`, `dob`, `job`, `lat/long` and `cc_num` are synthetic but shaped like real data. Hash `cc_num` (already decided in Q22). Do not use name or street fields as features. `gender` and `dob` (age) are protected attributes, so if age is used, say so in the model card.
8. **Synthetic separability.** Night-time plus category-specific amount bands make the task easy, so expect very high PR-AUC. The interesting signal is relative change under Shift, not the absolute numbers (ADR-0001 already notes this).

**Which categories to use for the injected Shifts [measured]**

- **Covariate (amount ×1.8): use `gas_transport` and `grocery_pos`.** They are the two highest-volume categories (10.1 % + 9.5 % of traffic). Their legit amount distributions are tight (p01 ≈ $32, p99 $104 and $232), whereas every other category spans about $1–$1,000, so ×1.8 really moves them. Results on two real 2020 days against the 2019 reference:

  | day (rows) | scaled categories | global `amt` PSI | global KS D (p) | per-category PSI / D |
  |---|---|---|---|---|
  | 2020-03-11 (1,535) | none | 0.010 | 0.032 (0.079) | — |
  | | gas_transport + grocery_pos | 0.103 | **0.125 (2e-21)** | gas 4.46 / 0.75; grocery_pos 1.25 / 0.49 |
  | | shopping_net + misc_net | 0.025 | 0.043 (0.007) | 0.42 / 0.26; 0.83 / 0.29 |
  | | home + kids_pets | 0.037 | 0.077 (3e-8) | 0.68 / 0.35; 0.29 / 0.25 |
  | 2020-08-12 (1,877) | gas_transport + grocery_pos | 0.113 | **0.126 (2e-26)** | 3.70 / 0.77; 1.85 / 0.59 |

  Only `gas_transport` + `grocery_pos` trips the Q19 rule on the global `amt` feature, and it does so through the KS branch (D > 0.1), **not** through PSI > 0.2. Any pair trips it on per-category `amt` (see Risks). `grocery_pos` is also a high-fraud category (1.37 %; fraud median $310), so legit ×1.8 ($105 → $188) moves toward the fraud band. That produces a visible prediction-score shift and more false positives, which makes a good demo.
- **Concept-shift "safe" category: `home`**, with `food_dining` as the alternative. `home` has the lowest fraud rate (0.147 %), its fraud median is $256 with **zero fraud under $10**, and it carries 9.5 % of traffic. That gives a high-volume category the Champion has never seen tiny fraud in. `food_dining` (0.159 %, fraud median $119, zero fraud under $10) also works. **Avoid `health_fitness`, `kids_pets`, `personal_care`, `gas_transport`, `grocery_net`, `travel` and `misc_pos`.** Their fraud profiles already contain tiny amounts (fraud medians of $9–$21, and 21–168 fraud rows under $10 in 2019), so the Champion may already treat night-time tiny amounts there as risky. Baseline legit tiny night-time activity in `home` (2019): 720 rows under $5 between 22:00 and 03:59, about 2 a day.

#### Q2. Injecting each Shift cleanly on the replayed stream

**Architecture.** Put a pure `inject(stream_chunk, active_scenarios) -> (stream_chunk, audit_rows)` stage between the replay reader and the Kafka producer.

- A Scenario is `(scenario_id, kind, start, end, seed, params)`, bounded in Simulated-clock time as `[start, end)`.
- Ground truth goes to two places that are **never on the feature path**:
  1. a `shift_events` table/topic with one row per toggle (`scenario_id, kind, params, seed, sim_start, sim_end, wall_time`);
  2. a `shift_audit` table keyed by `trans_num` (`scenario_id, op ∈ {scaled, cloned, synthetic}`).
- These two let you compute detection delay (sim-hours from `sim_start` to the first alert), false alarms (alerts outside any scenario) and per-scenario model metrics.
- **Run a null replay first** (2020 with no injection) to measure the natural false-alarm rate. December 2020 has a natural label shift (Q1 quirk 5).

**Definitions to keep straight.** These are Huyen's definitions (https://huyenchip.com/2022/02/07/data-distribution-shifts-and-monitoring.html, adapted from DMLS ch. 8):
- covariate shift: P(X) changes, P(Y|X) unchanged;
- label shift: P(Y) changes, P(X|Y) unchanged;
- concept drift: P(Y|X) changes.

**Covariate (Q13: amount ×1.8 in two categories).** Multiply `amt` in place; Labels are untouched.
- Strictly, this is not pure covariate shift. A legit $105 grocery_pos Transaction becomes $188, but its Label is still legit, so P(Y|amt, category) in the scaled region moves toward the label of the original (lower) amount. In interview terms it is "P(X) shift that also induces a P(Y|X) change in the scaled region". That is fine for the demo.
- A purer alternative is importance resampling: oversample existing high-amount rows of those categories, which preserves P(Y|X).
- **Flag:** this nuance slightly contradicts the glossary's definition of a covariate Shift. Either keep ×1.8 and say so, or switch to resampling.

**Label (Q13: ~3× fraud rate via oversampling).** Naive row duplication is wrong for Sparkov. Fraud lives in card-level episodes of about 10 Transactions (Q1 quirk 1), so duplicating single rows creates impossible per-card feature states. Two better approaches:
- **Clone whole fraud episodes (recommended).** Clone them from a past pool (2019, so nothing from the future leaks) onto synthetic cards (`clone-<hash>`) that copy the source card's static attributes. Keep the intra-episode time offsets and the hour of day. This keeps P(X|Y=1) close to the pool. Clones are new cards with no history, but new-card states exist in training too: every card starts empty in Jan 2019. 3× is roughly 26 extra fraud Transactions a day, about 2.6 cloned episodes a day.
- **Undersample legit by 1/3 (alternative).** This preserves P(X|Y=1) exactly. However, it lowers throughput and per-card velocity, which shifts P(X|Y=0).
- Clones get `is_fraud=1` and go through the normal chargeback delay.
- Prior work: Lipton et al. study label shift "where the label marginal p(y) changes but the conditional p(x|y) does not" (https://arxiv.org/abs/1802.03916). Rabanser et al. simulate shifts with "various perturbations to both covariates and label distributions with varying magnitudes and fractions of data affected" (https://arxiv.org/abs/1810.11953).

**Concept (Q13: night card-testing bursts in a "safe" category).** Generate new rows:
- category `home`, merchants sampled from `home`'s merchant list, hours 00:00–03:59, amounts Uniform($0.50, $3.00), `is_fraud=1`, attached to existing active cards;
- **few Transactions per card** (1–3) spread over many cards (for example 10–20 cards a sim-day).

Bursts concentrated on one card would trip the Champion's per-card velocity features, because Sparkov fraud cards already average 5 Transactions on a fraud day. That would turn the scenario into something the Champion already catches.

**Acceptance test:** after training, the Champion's score for the synthetic pattern must fall below the review threshold (outside the top 1 %). Otherwise it is not "a category the champion considers safe".

This is mostly real P(Y|X) drift plus a little P(X) drift, because the new night tiny `home` rows also change P(X). For a pure P(Y|X) variant, flip Labels on existing legit `home` rows under $5 at night instead. There are only about 2 a day, which is too few to be visible.

**Tooling and prior art.**
- River's `ConceptDriftStream` blends two streams with a sigmoid: f(t) = 1/(1+e^{−4(t−p)/w}), with "position ... Central position of the concept drift change" and "width ... Width of concept drift change" (River 0.26.x, https://riverml.xyz/latest/api/datasets/synth/ConceptDriftStream/). Use the same sigmoid to ramp a scenario in gradually rather than as a step. That makes "≥2 consecutive windows" meaningful.
- Seldon Alibi Detect (https://docs.seldon.ai/alibi-detect) and Evidently provide detectors but not stream injectors. Hand-rolled injection is normal.

**Tested sketch** (pandas 2.x / numpy 2.x; runs on a synthetic stream: the covariate scenario scaled 11,231 rows, the label scenario moved the fraud rate 0.0046 → 0.0137, and the concept scenario added 282 night-only rows):

```python
from dataclasses import dataclass
import hashlib, numpy as np, pandas as pd

@dataclass(frozen=True)
class Scenario:
    scenario_id: str; kind: str; start: pd.Timestamp; end: pd.Timestamp; seed: int; params: dict

def _active(df, s): return (df.ts >= s.start) & (df.ts < s.end)

def covariate(df, s):
    m = _active(df, s) & df.category.isin(s.params["categories"])
    out = df.copy(); out.loc[m, "amt"] = (out.loc[m, "amt"] * s.params["factor"]).round(2)
    return out, pd.DataFrame({"trans_num": out.loc[m, "trans_num"], "scenario_id": s.scenario_id, "op": "scaled"})

def label_by_episode_cloning(df, s, fraud_pool):
    rng = np.random.default_rng(s.seed)
    extra = (s.params["multiplier"] - 1) * df[_active(df, s)].is_fraud.sum()
    episodes = [g for _, g in fraud_pool.groupby("cc_num")]; clones, n = [], 0
    while n < extra:
        ep = episodes[rng.integers(len(episodes))].copy()
        day = s.start + pd.Timedelta(days=int(rng.integers((s.end - s.start).days)))
        ep["ts"] = day + (ep.ts - ep.ts.min().normalize())      # keep offsets + hour of day
        k = len(clones)
        ep["cc_num"] = "clone-" + hashlib.sha1(f"{s.scenario_id}-{k}".encode()).hexdigest()[:12]
        ep["trans_num"] = [hashlib.md5(f"{s.scenario_id}-{k}-{i}".encode()).hexdigest() for i in range(len(ep))]
        clones.append(ep); n += len(ep)
    add = pd.concat(clones)
    return (pd.concat([df, add]).sort_values("ts", kind="stable"),
            pd.DataFrame({"trans_num": add.trans_num, "scenario_id": s.scenario_id, "op": "cloned"}))

def concept_card_testing(df, s, merchants_by_cat):
    rng = np.random.default_rng(s.seed); p = s.params; cat = p["category"]
    cards = df.loc[_active(df, s), "cc_num"].drop_duplicates().to_numpy(); rows = []
    for d in pd.date_range(s.start.normalize(), s.end, freq="D", inclusive="left"):
        for card in rng.choice(cards, size=p["cards_per_day"], replace=False):
            for _ in range(rng.integers(1, p["max_tx_per_card"] + 1)):
                rows.append({"ts": d + pd.Timedelta(seconds=int(rng.integers(0, 4 * 3600))), "cc_num": card,
                             "category": cat, "merchant": rng.choice(merchants_by_cat[cat]),
                             "amt": round(float(rng.uniform(*p["amt_range"])), 2), "is_fraud": 1})
    add = pd.DataFrame(rows)
    add["trans_num"] = [hashlib.md5(f"{s.scenario_id}-{i}".encode()).hexdigest() for i in range(len(add))]
    return (pd.concat([df, add]).sort_values("ts", kind="stable"),
            pd.DataFrame({"trans_num": add.trans_num, "scenario_id": s.scenario_id, "op": "synthetic"}))
```

Per ADR-0002, the per-card feature state is updated in Kafka partition order. Injected rows must be produced through the same partitioner (key = hashed card) and in Simulated-clock order, or the train/serve parity test will fail on them.

#### Q3. PSI and KS

**PSI formula.** Yurdakul & Naranjo, *Statistical properties of the population stability index*, J. Risk Model Validation 14(4):89–100, 2020, DOI 10.21314/JRMV.2020.227 (https://files.wmich.edu/s3fs-public/attachments/u730/2022/PSIfinal.pdf; thesis: https://scholarworks.wmich.edu/dissertations/3208/). With reference proportions p̂ᵢ = xᵢ/n and current proportions q̂ᵢ = yᵢ/m over B bins:

PSI = Σᵢ (q̂ᵢ − p̂ᵢ) · ln(q̂ᵢ / p̂ᵢ)

This equals KL(p‖q) + KL(q‖p), the symmetric Jeffreys divergence.

**Where the thresholds come from.** The paper says:
- "In practice, the following "rule of thumb" is used: PSI < 0.10 means a "little change", 0.10 ≤ PSI < 0.25 means a "moderate change" and 0.25 ≤ PSI means a "significant change, action required". These benchmarks are used without reference to statistical type I or type II error rates."
- "A typical rule of thumb (Lewis 1994) for the extent to which a distribution has shifted is the following..." The paper also cites Siddiqi (2017) and Anderson (2007), and says Anderson "points out that the rule of thumb is used as a traffic light approach in the industry".

The project's 0.2 sits inside the "moderate" band, and it is also a common industry choice.

**Null distribution, which explains why 0.2 is insensitive here.** The paper shows that under no change, PSI ≈ (1/n + 1/m)·χ²_{B−1}. It "has expected value (1/n+1/m)(B−1)". It also notes: "the tests PSI > 0.10 and PSI > 0.25 have powers that actually decrease with sample size". For n ≈ 925k reference rows, m ≈ 2,538 rows a day and B = 10, E[PSI] ≈ 0.0036 and the 99th null percentile is ≈ 0.0085. On real 2020 days I measured a no-shift `amt` PSI of 0.0075–0.010. A **0.2 threshold is therefore about 20× above noise.** It will not false-alarm, but it only fires on large shifts.

**Binning.**
- Use **quantile bins from the reference** (10 bins; 20 for scores), with open outer edges (−∞, +∞) so production values outside the training range still land in a bin. Dedupe tied edges.
- Fix the edges at Champion-training time and store them with the model version, so every window is compared on the same grid.
- Empty bins: clip proportions to ε = 1e-4 and renormalise.
- Note that Evidently's `get_binned_data` (main branch, `src/evidently/legacy/calculations/stattests/utils.py`) actually uses `np.histogram_bin_edges(combined, bins="sturges")` on reference+current combined, even though its docstring says "Split variable into n buckets based on reference quantiles". Its empty-bin fill is 0.0001, or min/10⁶ when the minimum proportion is ≤ 1e-4. Evidently's PSI default threshold is 0.1 (https://docs.evidentlyai.com/metrics/customize_data_drift). This is a further reason to keep custom PSI (Q19).

**Categorical PSI.** Use one bin per category taken from the union of reference and current values, with the same ε floor. Hour of day should be treated as categorical (24 bins), not with KS.

**KS at large n.** Huyen: "just because the difference is statistically significant doesn't mean that it is practically important ... If it takes a huge sample, then it is probably not worth worrying about." (https://huyenchip.com/2022/02/07/data-distribution-shifts-and-monitoring.html). The two-sample KS critical value is D_crit ≈ c(α)·√(1/n + 1/m), with c(0.01) ≈ 1.628. Concretely:
- **Global features on 1-sim-day windows** (m ≈ 2,500): D_crit ≈ 0.033. So "p < 0.01 AND D > 0.1" reduces to **D > 0.1**, and the p-value adds nothing.
- **Per-category windows** (m ≈ 80–250 a day): D_crit ≈ 0.10–0.18, so the p-value condition does bite there.
- Demo: n = m = 10⁶ with a 0.005 log-mean shift gives p = 0.045 but D = 0.002. That is "significant" and meaningless.

**Effect size.**
- **The KS D statistic is the effect size**: the maximum gap between the two CDFs, in [0, 1]. D > 0.1 means "some threshold moves ≥10 percentage points of mass".
- A complementary magnitude-aware metric is **Wasserstein-1 normalised by the reference std**, which is Evidently's default for numerical columns with >1000 objects (threshold 0.1). The code is `norm = max(float(np.std(reference_data)), 0.001)` and `wd_norm_value = stats.wasserstein_distance(reference_data, current_data) / norm` (evidently `wasserstein_distance_norm.py`, main). Evidently's docs: "Default method for numerical data, if ≤ 1000 objects" is KS at p 0.05, and Wasserstein above that (https://docs.evidentlyai.com/metrics/customize_data_drift).
- Keep D (it is part of Q19) and log normalised Wasserstein next to it for explanation.
- KS assumes continuous data. With heavy ties (cents, integer hours) the asymptotic p-value is conservative, so use PSI or chi-square for discrete features.

**SciPy API (v1.18 docs, https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.ks_2samp.html):**
- Signature: `ks_2samp(data1, data2, alternative='two-sided', method='auto', *, axis=0, nan_policy='propagate', keepdims=False)`.
- With `method='auto'`, "an exact p-value computation is attempted if both sample sizes are less than 10000; otherwise, the asymptotic method is used."
- The result has `statistic`, `pvalue`, `statistic_location` and `statistic_sign`.
- Use `method="asymp"` for speed and determinism.

**Code** (tested; outputs are shown in the comments):

```python
import numpy as np
from scipy import stats

EPS = 1e-4

def quantile_edges(reference, n_bins=10):
    qs = np.quantile(reference, np.linspace(0, 1, n_bins + 1)[1:-1])
    return np.concatenate(([-np.inf], np.unique(qs), [np.inf]))   # store with the model version

def _psi(p, q, eps=EPS):
    p = np.clip(p, eps, None); q = np.clip(q, eps, None)
    p, q = p / p.sum(), q / q.sum()
    return float(np.sum((q - p) * np.log(q / p)))

def psi_numeric(reference, current, edges=None, n_bins=10):
    edges = quantile_edges(reference, n_bins) if edges is None else edges
    return _psi(np.histogram(reference, edges)[0] / len(reference),
                np.histogram(current, edges)[0] / len(current))

def psi_categorical(reference, current):
    cats = np.union1d(np.unique(reference), np.unique(current))
    r = dict(zip(*np.unique(reference, return_counts=True))); c = dict(zip(*np.unique(current, return_counts=True)))
    return _psi(np.array([r.get(k, 0) for k in cats], float) / len(reference),
                np.array([c.get(k, 0) for k in cats], float) / len(current))

def psi_null_threshold(n_ref, n_cur, n_bins, alpha=0.01):
    """Yurdakul & Naranjo 2020: under H0 PSI ~ (1/n + 1/m) chi2(B-1)."""
    return (1 / n_ref + 1 / n_cur) * stats.chi2.ppf(1 - alpha, n_bins - 1)

def ks_drift(reference, current):
    r = stats.ks_2samp(reference, current, method="asymp")
    wd = stats.wasserstein_distance(reference, current) / max(np.std(reference), 1e-3)
    return {"D": float(r.statistic), "p": float(r.pvalue), "wasserstein_norm": float(wd)}

def feature_alert(psi, ks, psi_thr=0.2, p_thr=0.01, d_thr=0.1):        # Q19 rule, per window
    return psi > psi_thr or (ks["p"] < p_thr and ks["D"] > d_thr)

# Synthetic check, lognormal reference n=1e6 vs one 20k window:
#   no shift: psi=0.0004 D=0.007 p=0.25 | x1.8 on 1/7 of rows: psi=0.0042 D=0.029 p=7e-15
#   psi_null_threshold(1e6, 2e4, 10) = 0.0011
```

The "≥ 2 consecutive windows" rule from Q19 is a small per-feature state machine (`streak = streak + 1 if alert else 0; fire when streak == 2`). Persist it with the monitor's state.

#### Q4. Monitoring under delayed Labels

**Why it matters.** The label delay here is long. Huyen: "Fraud detection is an example of a task with long feedback loops ... A typical dispute window is a month to three months. After the dispute window has passed, if there's no dispute from the user, you can presume the transaction to be legitimate." (same URL). The Q11 design compresses this to 7–45 sim days, which is a reasonable simulation choice to state in the README. Matured-label metrics for a Transaction are final only 45 sim days after it.

**Prediction-score drift is the cheapest early signal.** Huyen: "Assuming that the function that maps from input to output doesn't change — the weights and biases of your model haven't changed — then a change in the prediction distribution generally indicates a change in the underlying input distribution." Three caveats from my simulations:
- **Decile-binned score PSI misses label shift.** I simulated a 3× fraud rate (0.4 % → 1.2 %) on a score mixture with 1M reference scores and a 20k window. Score PSI was **0.0014** with decile bins and **0.008** with tail-aware bins (edges at reference quantiles 0.5/0.9/0.99/0.997/0.999). Neither gets anywhere near 0.2. Q19's "same on prediction scores" rule will therefore not catch the label-shift scenario.
- **Monitor the action rates at *fixed* score cutoffs.** In the same simulation the block rate at the fixed 0.3 % cutoff went 0.0030 → 0.0092, with a one-sided `scipy.stats.binomtest` p = 1.9e-38. This is the most sensitive cheap label-shift and concept-shift signal. It **requires that thresholds be frozen per model version**. If thresholds are re-fitted daily to hit the Alert budget, the block rate is constant by construction and the signal disappears.
- Also log the mean score and the top-tail quantiles (p99, p99.7, p99.9) per window.

**Performance estimation before Labels arrive (NannyML concepts, docs "stable", nannyml 0.13.1)** (https://nannyml.readthedocs.io/en/stable/how_it_works/performance_estimation.html)
- **CBPE.** "Assuming properly calibrated probabilities, confusion matrix elements can be estimated and then used to calculate any performance metric." Limits: "While dealing well with data drift, CBPE will not work under concept drift i.e. when P(Y|X) changes." NannyML "calibrates probabilities based on reference data and currently uses isotonic regression".
- **DLE** trains a LightGBM "nanny model" to predict the monitored model's loss. In NannyML it is aimed at regression. It has the same limit: "DLE will not work under concept drift". Skip it here.
- **Implication:** CBPE is blind to exactly the concept scenario, and under label shift P(Y|X) also changes, so the calibration breaks.

**What is cheap and credible to build** (my recommendation, not taken from a single source):
1. **CBPE-lite for the business metrics.** Fit isotonic calibration on the Champion's validation window. LightGBM with IPW weights or imbalance handling is not calibrated by default. Each sim-day, compute:
   - expected precision@block = mean(p̂ | blocked);
   - expected fraud-dollar recall@budget = Σ_flagged p̂·amt / Σ_all p̂·amt.
2. **The CBPE gap as a concept-drift detector.** When Matured labels arrive, compare the realised metric against the CBPE estimate made for the same days. A persistent gap means P(Y|X) changed. Under covariate shift CBPE should track, under concept shift it diverges, so the divergence is itself the signal.
3. **Early fraud counts from the known delay distribution.** Chargebacks arrive at 7–45 sim days, uniform if that is how Q11 is implemented. Estimate a day's eventual fraud count as `observed_by_age_k / F(k)`, where F is the known delay CDF. This is the actuarial "incurred but not reported" idea, valid because the simulator knows F exactly. It gives an unbiased fraud-rate estimate from about day 8 instead of day 45, so the label-shift alarm fires weeks earlier.
4. Keep the Q19 "performance on matured labels" check as the final arbiter. Compute it on Transactions at least 45 sim days old, including the review queue (labelled after ~1 day) and IPW-weighted Exploration rows.

#### Q5. Degenerate feedback loops, IPW and off-policy evaluation

**The problem.**
- Huyen (DMLS ch. 8, https://www.oreilly.com/library/view/designing-machine-learning/9781098107956/ch08.html; free notes version https://huyenchip.com/2022/02/07/data-distribution-shifts-and-monitoring.html): "a degenerate feedback loop is created when a system's outputs are used to create or process the same system's inputs, which, in turn, influence the system's future outputs." Remedies named: "The first one is to use randomization, and the second one is to use positional features."
- Dal Pozzolo et al., *Credit Card Fraud Detection: A Realistic Modeling and a Novel Learning Strategy*, IEEE TNNLS 2017 (https://dalpozz.github.io/static/pdf/TNNLS_2017.pdf): "the alert-feedback interaction is responsible of a sort of sample selection bias (SSB) [19] that injects further differences between the distribution of training and test data", and "feedbacks represent a sort of biased training set".
- Stripe, *A primer on machine learning for fraud detection* (https://stripe.com/guides/primer-on-machine-learning-for-fraud-protection; dated 2021-12-15 on the localised copy): "Payments that have scores above the threshold, however, are blocked, and so we can't know what their outcomes would have been. Computing the full production precision-recall or ROC curve is thus more involved than computing the validation curves because it involves counterfactual analysis – we need to obtain statistically sound estimates of what would have happened even to the payments we blocked."
- Stripe Radar docs (https://docs.stripe.com/radar/risk-evaluation?locale=en-US, retrieved 2026-10-04): "For a small subset of payments, Stripe modifies the reported risk score so we can measure the performance of our models and obtain data for subsequent model development. This allows us to make sure key metrics, such as false positive rate and recall, remain within desirable ranges".
- Michael Manapat (Stripe), *Counterfactual evaluation of machine learning models*, PyData Seattle, July 2015 (slides: https://www.slideshare.net/slideshow/counterfactual-evaluation-of-machine-learning-models/50981869; video: https://www.youtube.com/watch?v=QWCSxAKR-h0):
  - the policy is `if score > 50: if random.random() < 0.05: allow() else: block()`;
  - the weight is 1/P(allow), "1/0.05 = 20";
  - a refinement is a score-dependent propensity: "The higher the score, the lower probability we let the charge through";
  - the worked example gives weighted precision 5/9 and recall 5/6 for a candidate policy;
  - bootstrap is advised for confidence intervals.
  - This is exactly the Q12/Q18 design, with Stripe's 5 % replaced by 2 %.

**Theory behind it.** IPS / Horvitz–Thompson and its refinements:
- Bottou et al., *Counterfactual Reasoning and Learning Systems*, JMLR 14, 2013: https://jmlr.org/papers/v14/bottou13a.html
- Dudík, Langford & Li, *Doubly Robust Policy Evaluation and Learning*, 2011, https://arxiv.org/abs/1103.4601: models of rewards are "plagued by a large bias whereas the latter [models of the past policy] have a large variance".
- Swaminathan & Joachims, *The Self-Normalized Estimator for Counterfactual Learning* (SNIPS), NeurIPS 2015: https://proceedings.neurips.cc/paper/2015/hash/39027dfad5138c9ca0c474d71db915c3-Abstract.html
- Lakkaraju et al., *The Selective Labels Problem*, KDD 2017, https://www.kdd.org/kdd2017/papers/view/the-selective-labels-problem-evaluating-algorithmic-predictions-in-the-pres: the same problem in bail decisions.

**The propensity model for this platform.** Log it per Transaction at decision time; never reconstruct it later.

| Champion decision | P(Label observed) | training/eval weight |
|---|---|---|
| allow | 1 | 1 |
| review (labelled after ~1 day) | 1 | 1 |
| would-be block, explored (2 %) | 0.02 | **50** |
| would-be block, not explored | 0.02 | dropped (no Label) |

Log the fields `decision`, `would_be_decision`, `explored`, `propensity`, `champion_version` and `champion_score`.
- **Training (Q18):** `lgb.Dataset(X, y, weight=w)` (https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.Dataset.html).
- **Evaluation:** `sklearn.metrics.average_precision_score(y, s, sample_weight=w)` for PR-AUC (https://scikit-learn.org/stable/modules/generated/sklearn.metrics.average_precision_score.html), plus the weighted fraud-dollar recall below.

**Evaluating a Challenger offline when Labels are censored by the Champion.**
1. The Challenger scores **all** traffic in shadow mode, including Transactions the Champion blocked. The Challenger's Alert-budget threshold is the (1 − 0.01) quantile of its own scores over all traffic, which needs no Labels. Models are therefore compared "at equal alert budget" (Q16) without bias.
2. Metric numerators and denominators are Horvitz–Thompson sums over labelled rows with weight w = 1/propensity. Disagreements where the Challenger flags something the Champion allowed are fully labelled. Only the Champion-blocked region relies on the Exploration sample.
3. Report SNIPS (divide by Σw instead of N) as a lower-variance variant. Optionally report a doubly-robust version that imputes unexplored blocks with the Champion's calibrated p̂. Bootstrap by sim-day for confidence intervals, and report the **Kish effective sample size** of the blocked region.

```python
import numpy as np

def ipw_weights(decision, explored, explore_rate=0.02):
    labelled = (decision != "block") | explored
    return np.where(labelled, np.where(decision == "block", 1 / explore_rate, 1.0), 0.0)

def flag_top_budget(scores, budget=0.01):                 # equal alert budget, label-free
    return scores >= np.quantile(scores, 1 - budget)

def fraud_dollar_recall(flags, y, amt, w):                # w=1 -> oracle (sim truth) or naive (labelled only)
    return np.sum(w * amt * y * flags) / np.sum(w * amt * y)

def kish_ess(w):
    w = w[w > 0]; return w.sum() ** 2 / np.sum(w ** 2)

# Synthetic month (80k tx, 0.4 % fraud, overlapping scores): blocks=240, explored=10
#   oracle recall 0.804 | naive (drop unlabelled blocks) 0.666 | IPW 0.830, 95 % bootstrap CI [0.671, 0.902]
```

**How to measure naive vs IPW (Q18).** The simulator's big advantage is that it holds **oracle Labels for every Transaction**, including those the platform never sees. Use a 2 × 3 design:

| | oracle eval (all true Labels) | naive eval (labelled rows, w=1) | IPW eval (w = 1/propensity) |
|---|---|---|---|
| naive-trained Challenger | ✓ | ✓ | ✓ |
| IPW-trained Challenger | ✓ | ✓ | ✓ |

- **Training bias:** oracle(IPW model) − oracle(naive model).
- **Evaluation bias:** naive-eval − oracle and IPW-eval − oracle, for each model.

Use paired bootstrap over sim-days on the same window, repeat over several exploration seeds, and report PR-AUC and fraud-dollar recall at equal budget. Optionally add a third training variant, "unlabelled blocks treated as fraud", which is a common industry heuristic. Manapat's "trained on the hard cases" story is the expected result: the naive model loses the high-score frauds that the Champion blocked.

**Size of the effect [measured volumes + estimate].**
- Replay 2020 has 927,544 Transactions, so the 0.3 % block budget is about 2,783 blocks/year (7.6 per sim-day) against about 12 frauds per day.
- If block precision is high, **roughly half of all replay fraud is censored**. That makes the naive model's bias large.
- But 2 % exploration yields only **about 56 explored rows a year (about 4.6 per 30-sim-day retrain)**. IPW estimates of the blocked region will be extremely noisy. The synthetic month above had 10 explored rows and a CI width of about 0.23.

### Recommendation

1. **Dataset handling.**
   - Concatenate both Kaggle files, sort by `trans_date_trans_time`, and drop `Unnamed: 0`.
   - Use `trans_date_trans_time` as event time and ignore `unix_time`.
   - Strip the `fraud_` prefix from `merchant` and hash `cc_num`.
   - Keep `trans_num` as the key.
   - Train on 2019 (924,850 rows, 0.564 %) and replay 2020 (927,544 rows, 0.478 %).
   - Add a pandera schema check for the 23 columns and the time-offset invariant.
2. **Shift parameters.**
   - Covariate: `amt ×1.8` in **`gas_transport` + `grocery_pos`**.
   - Label: 3× via **fraud-episode cloning** from the 2019 pool onto synthetic cards. Do not use row duplication.
   - Concept: **`home`**, 00:00–03:59, $0.50–$3.00, 10–20 cards/day × 1–3 Transactions.
   - Ramp each scenario with a sigmoid, log ground truth to `shift_events` + `shift_audit`, and run a null replay first.
   - Avoid scheduling the label scenario in December, which has its own natural label shift.
3. **Drift detection (keeps Q19 and adds to it).**
   - Custom PSI with reference-quantile edges stored per model version, ε = 1e-4, categorical PSI for `category` and `hour`. KS D as the effect size, with normalised Wasserstein logged alongside.
   - Add **per-category `amt` monitoring** and **fixed-cutoff block/review-rate monitoring** (binomial test), and **freeze score thresholds per model version**. Without these the label and concept scenarios are not caught by the Q19 rules (see Risks).
   - Optionally log the Yurdakul–Naranjo χ² null threshold next to the 0.2 rule, as an interview talking point.
4. **Delayed Labels.**
   - Calibrated CBPE-lite estimates of precision@block and fraud-dollar recall.
   - The "CBPE vs matured" gap as the concept-drift signal.
   - Delay-CDF-corrected early fraud counts.
   - Matured-label metrics only on Transactions at least 45 sim days old.
5. **Censored Labels.**
   - Log the propensity per decision.
   - Train with `weight=50` on Exploration rows (Q18) and evaluate Challengers with HT/SNIPS weights, bootstrap CIs and Kish ESS.
   - Report the 2×3 oracle/naive/IPW table.
   - Make the exploration rate a config value and consider a "demo" profile at 10 % (weight 10). At 2 % the IPW numbers will be dominated by noise; see the flag below.

### Risks and gotchas

- **⚠ Contradicts Q19 (score drift rule):** score PSI > 0.2 will essentially never fire. Label shift ×3 gave a score PSI of 0.001–0.008 in simulation, and the concept scenario adds about 1–2 % of rows. Add fixed-cutoff action-rate monitoring, or the label and concept scenarios rely only on matured labels (7–45+ sim days late).
- **⚠ Partly contradicts Q19 (feature drift):** on global `amt`, ×1.8 in two categories gives PSI ≈ 0.10–0.11 (< 0.2). It only alerts through KS (D ≈ 0.125), and only for the `gas_transport` + `grocery_pos` pair. Other pairs give D 0.04–0.08 and no alert. Per-category monitoring catches every pair (per-category PSI 0.3–4.5).
- **⚠ Q12/Q18 exploration rate:** 2 % of a 0.3 % block budget is about 56 labelled blocked rows per replay-year. The IPW estimate and the IPW-trained model will be high-variance, and with weight-50 rows LightGBM splits can be driven by a handful of rows (`min_data_in_leaf` counts rows, not weight). Mitigations:
  - a higher demo exploration rate;
  - weight clipping or SNIPS;
  - reporting ESS and CIs;
  - several replay seeds.
- **The censoring effect starts small.** The stateless Challenger trains on 12 months of matured Labels, and at first that is mostly fully-labelled 2019 history. The naive vs IPW difference only grows once replay months make up a large part of the window. Plan the demo timeline, or simulate a pre-platform rule-based policy that also censored 2019.
- **Thresholds and the Alert budget.** Thresholds re-fitted on live traffic hide label shift. Thresholds fixed from training drift off-budget under shift. Decide which, document it, and monitor budget compliance separately.
- **CBPE needs calibrated scores.** IPW weights and class imbalance distort LightGBM probabilities, so fit isotonic calibration after training and version it with the model. CBPE is wrong by design under concept and label shift. Use that, rather than trusting it then.
- **PR-AUC depends on the base rate.** Under label shift (or in December 2020, at 0.185 %), PR-AUC moves even for an unchanged model. Compare Champion and Challenger only on the same window.
- **The concept scenario may not be "safe" for the trained Champion.** Night-time is the strongest fraud signal in Sparkov (84.6 % vs 23.2 %). Verify the Champion's scores on the synthetic pattern before claiming it is a blind spot (acceptance test in Q2).
- **Generator artefacts.** Pure fraud card-days and 97.7 % of cards having fraud make velocity features unrealistically strong. Do not over-claim real-world performance.
- **Kaggle text vs data:** "1000 customers / 800 merchants" vs the measured 999 / 693. Quote the measured numbers.
- **Mirrors.** Some Hugging Face mirrors were saved through Excel and are truncated to 1,048,575 rows, and one adds a `merch_zipcode` column. Verify the row counts (1,296,675 / 555,719) after downloading from Kaggle.
- **Huyen's real dispute window is "a month to three months"**, against Q11's 7–45 days. That is fine for a simulation, but say in the README that it is compressed.
- **Library staleness.** NannyML's last release is 0.13.1 (2025-07-12). Use it for concepts only, consistent with the custom-implementation decision.

### Sources

- Kaggle dataset page: https://www.kaggle.com/datasets/kartik2112/fraud-detection
- Kaggle API metadata: https://www.kaggle.com/api/v1/datasets/view/kartik2112/fraud-detection
- Sparkov generator repo, README and LICENSE (MIT): https://github.com/namebrandon/Sparkov_Data_Generation
- Sparkov fraud and episode logic: https://github.com/namebrandon/Sparkov_Data_Generation/blob/master/datagen_transaction.py
- Sparkov night-time fraud hours and gamma amounts: https://github.com/namebrandon/Sparkov_Data_Generation/blob/master/profile_weights.py
- Sparkov fraud profile example: https://github.com/namebrandon/Sparkov_Data_Generation/blob/master/profiles/fraud_adults_2550_female_rural.json
- HF mirror used for the measurements: https://huggingface.co/datasets/NeerajCodz/creditCardFraudDetection
- Yurdakul & Naranjo 2020, PSI statistical properties: https://files.wmich.edu/s3fs-public/attachments/u730/2022/PSIfinal.pdf
- Yurdakul 2018 thesis: https://scholarworks.wmich.edu/dissertations/3208/
- SciPy `ks_2samp` (v1.18): https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.ks_2samp.html
- SciPy `wasserstein_distance`: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.wasserstein_distance.html
- SciPy `jensenshannon`: https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.distance.jensenshannon.html
- Evidently drift defaults: https://docs.evidentlyai.com/metrics/customize_data_drift
- Evidently PSI and Wasserstein source (main): https://github.com/evidentlyai/evidently/tree/main/src/evidently/legacy/calculations/stattests
- NannyML CBPE/DLE: https://nannyml.readthedocs.io/en/stable/how_it_works/performance_estimation.html
- Chip Huyen, Data Distribution Shifts and Monitoring (CS329S notes for DMLS ch. 8): https://huyenchip.com/2022/02/07/data-distribution-shifts-and-monitoring.html
- DMLS ch. 8 (O'Reilly): https://www.oreilly.com/library/view/designing-machine-learning/9781098107956/ch08.html
- Dal Pozzolo et al. 2017, TNNLS: https://dalpozz.github.io/static/pdf/TNNLS_2017.pdf
- Fraud Detection Handbook, Precision top-k: https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_4_PerformanceMetrics/TopKBased.html
- Stripe primer on ML for fraud: https://stripe.com/guides/primer-on-machine-learning-for-fraud-protection
- Stripe Radar risk evaluation: https://docs.stripe.com/radar/risk-evaluation?locale=en-US
- Manapat 2015, counterfactual evaluation (slides): https://www.slideshare.net/slideshow/counterfactual-evaluation-of-machine-learning-models/50981869
- Manapat 2015 (video): https://www.youtube.com/watch?v=QWCSxAKR-h0
- Bottou et al. 2013, JMLR: https://jmlr.org/papers/v14/bottou13a.html
- Dudík, Langford & Li 2011, doubly robust: https://arxiv.org/abs/1103.4601
- Swaminathan & Joachims 2015, SNIPS: https://proceedings.neurips.cc/paper/2015/hash/39027dfad5138c9ca0c474d71db915c3-Abstract.html
- Lakkaraju et al. 2017, selective labels: https://www.kdd.org/kdd2017/papers/view/the-selective-labels-problem-evaluating-algorithmic-predictions-in-the-pres
- Lipton et al. 2018, BBSE / label shift: https://arxiv.org/abs/1802.03916
- Rabanser et al., Failing Loudly: https://arxiv.org/abs/1810.11953
- River `ConceptDriftStream`: https://riverml.xyz/latest/api/datasets/synth/ConceptDriftStream/
- Alibi Detect: https://docs.seldon.ai/alibi-detect
- LightGBM `Dataset(weight=...)`: https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.Dataset.html
- scikit-learn `average_precision_score(sample_weight=...)`: https://scikit-learn.org/stable/modules/generated/sklearn.metrics.average_precision_score.html
- PyPI version checks (2026-10-04): https://pypi.org/project/scipy/, https://pypi.org/project/nannyml/, https://pypi.org/project/evidently/, https://pypi.org/project/lightgbm/


---

## 02. Streaming and the scoring service

Researched 2026-10-04. Versions and dates come from PyPI, GitHub releases and project changelogs, fetched on that date.

### Questions

1. Strimzi on kind: which Strimzi version is current (KRaft, no ZooKeeper)? What are minimal single-broker `Kafka`, `KafkaNodePool` and `KafkaTopic` manifests for a laptop? What is the memory footprint? Should it be installed with Helm or the operator YAML? How do clients inside and outside the cluster connect? What changes on GKE?
2. Which Python Kafka client should we use in 2026: confluent-kafka-python, aiokafka or kafka-python? Compare maintenance, Python 3.13 wheels and performance. How does keying by card guarantee per-card ordering? Does the default partitioner differ between clients, and can that cause a mismatch?
3. Stateful consumer patterns: per-card state in memory, snapshot and restore across rebalances (`on_assign`/`on_revoke`), the order of offset commit versus state snapshot, and the trade-off between at-least-once and exactly-once for this demo.
4. Service shape: should this be one FastAPI app (a background consumer plus HTTP `/score`) or two deployables? How do we load the champion and challenger from MLflow by alias and hot-reload them? How do we write predictions to Postgres efficiently?
5. Replay producer on an accelerated clock: how do we replay by event time with a speed-up factor, and how do downstream components read "now" from event time?

### Findings

#### Q1. Strimzi on kind

**Current version.** Strimzi **1.2.0**, released 2026-08-20, supports Apache Kafka **4.3.1** ([GitHub releases API](https://api.github.com/repos/strimzi/strimzi-kafka-operator/releases); [CHANGELOG](https://github.com/strimzi/strimzi-kafka-operator/blob/main/CHANGELOG.md)). The earlier releases were 1.1.0 (2026-06-27) and 1.0.1 (2026-06-17). Version 1.3.0 is unreleased and is listed in the changelog.

Key facts from the changelog ([CHANGELOG.md](https://github.com/strimzi/strimzi-kafka-operator/blob/main/CHANGELOG.md)):
- ZooKeeper is gone. The changelog says: "**Strimzi 0.45 is the last minor Strimzi version with support for ZooKeeper-based Apache Kafka clusters and MirrorMaker 1 deployments.**" It also says: "Support for ZooKeeper-based Apache Kafka clusters and for KRaft migration has been removed" (0.46). Every 1.x cluster is KRaft, with nodes defined by `KafkaNodePool`.
- Only the v1 API remains. 1.0.0 says: "Remove the `v1beta2` API (and `v1alpha1` and `v1beta2` for `KafkaTopic` and `KafkaUser`) from the CRDs and fully move to the `v1` API". Many blog posts still show `kafka.strimzi.io/v1beta2` and the `strimzi.io/kraft: enabled` / `strimzi.io/node-pools: enabled` annotations. Those manifests are rejected by 1.x. Use `apiVersion: kafka.strimzi.io/v1`.
- Broker resources now live on node pools. The 0.48.0 entry says: "CPU and memory configuration for the Kafka nodes in `.spec.kafka.resources` is deprecated and will be removed in the `v1` CRD API. Please use the `KafkaNodePool` resources to configure CPU and memory for Kafka nodes."
- Kubernetes support:
  - The 1.2.0 deploying guide says: "You can deploy Strimzi on Kubernetes 1.30 and later" ([deploying docs](https://strimzi.io/docs/operators/latest/deploying.html)).
  - The unreleased 1.3.0 entry says: "**From Strimzi 1.3.0 on, we support only Kubernetes 1.32 and newer.**"
  - kind is at v0.33.0 ([kind releases](https://github.com/kubernetes-sigs/kind/releases/latest)), so its default node image satisfies both.
- 1.2.0 says: "The Cluster, Topic, and User Operator YAML installation files and the Cluster Operator Helm Chart now use the default container security context that matches the Restricted Kubernetes Pod Security Standard."

**Install: Helm or YAML.** Both are first-party.
- YAML quickstart ([strimzi.io/quickstarts](https://strimzi.io/quickstarts/)):
  ```bash
  kind create cluster
  kubectl create namespace kafka
  kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
  kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml -n kafka
  kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
  ```
- Helm, from the [deploying guide](https://strimzi.io/docs/operators/latest/deploying.html): "helm install strimzi-cluster-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator".
- The chart's `values.yaml` at tag 1.2.0 ([values.yaml](https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/1.2.0/helm-charts/helm3/strimzi-kafka-operator/values.yaml)) sets these defaults: `watchNamespaces: []`, `watchAnyNamespace: false`, `defaultImageTag: 1.2.0`, operator `resources: limits: memory: 384Mi, cpu: 1000m; requests: memory: 384Mi, cpu: 200m`.
- For Argo CD, use the Helm chart as an OCI source, pinned to `1.2.0`. Set `watchNamespaces: [staging, prod]` (or one `kafka` namespace that both environments share).
- CRD caveat from the Helm docs: "There is no support at this time for upgrading or deleting CRDs using Helm" ([Helm CRD best practices](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/)). Before upgrading Strimzi, apply the new CRDs separately. An Argo CD app that syncs the chart's `crds/` directory with `ServerSideApply=true` also works.
- The deploying guide warns that "each watched namespace should contain only one instance of a specific component type, such as one Kafka cluster, to avoid conflicts". So use one Kafka cluster per namespace, or one shared `kafka` namespace for both staging and prod topics with a topic prefix.

**Minimal laptop manifests.** These are adapted from the official 1.2.0 `kafka-single-node.yaml` (fetched from [strimzi.io/examples/latest](https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml)). The changes are a smaller disk, explicit resources and JVM heap, no User Operator, and one NodePort listener.
```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  labels:
    strimzi.io/cluster: fraud-kafka
spec:
  replicas: 1
  roles: [controller, broker]          # one KRaft node does both jobs
  resources:
    requests: { memory: 1Gi, cpu: 250m }
    limits:   { memory: 1536Mi }
  jvmOptions:
    "-Xms": 512m
    "-Xmx": 768m
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: 10Gi                     # official example asks for 100Gi
        deleteClaim: true
        kraftMetadata: shared
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: fraud-kafka
spec:
  kafka:
    version: 4.3.1
    metadataVersion: 4.3-IV0
    listeners:
      - name: plain                    # in-cluster clients
        port: 9092
        type: internal
        tls: false
      - name: external                 # laptop clients, see kind config below
        port: 9094
        type: nodeport
        tls: false
        configuration:
          bootstrap:
            nodePort: 32100
          brokers:
            - broker: 0
              nodePort: 32000
              advertisedHost: localhost
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
      auto.create.topics.enable: false
  entityOperator:
    topicOperator:
      resources:
        requests: { memory: 256Mi, cpu: 50m }
        limits:   { memory: 384Mi }
    # userOperator omitted: there is no auth on the local cluster (Q22)
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: transactions
  labels:
    strimzi.io/cluster: fraud-kafka    # must match, or the Topic Operator ignores it
spec:
  partitions: 6                        # fixed for life: the key-to-partition map depends on it
  replicas: 1
  config:
    retention.ms: -1                   # see Q5 gotcha about event-time timestamps
    message.timestamp.type: CreateTime
```
The `KafkaTopic` label requirement is quoted from the [deploying guide](https://strimzi.io/docs/operators/latest/deploying.html): "If the label does not match the Kafka cluster, the Topic Operator cannot see the KafkaTopic, and the topic is not created." The official topic example is `apiVersion: kafka.strimzi.io/v1`, `kind: KafkaTopic`, `spec: partitions/replicas/config` ([kafka-topic.yaml @1.2.0](https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/1.2.0/packaging/examples/topic/kafka-topic.yaml)).

The NodePort override syntax and `advertisedHost` come from the [configuring guide §10.7 "Overriding assigned node ports"](https://strimzi.io/docs/operators/latest/configuring.html): "By default, the port numbers used for the bootstrap and broker services are automatically assigned by Kubernetes. You can override the assigned node ports for nodeport listeners by specifying the desired port numbers." The `advertisedHost` per-broker override is documented in the same guide. *Unverified on this machine:* `advertisedHost: localhost` combined with the kind port mappings below is the usual laptop recipe, but nobody has run it here yet.

**Memory footprint.**
- Strimzi sets no default broker resources. The configuring guide says: "If a memory limit (and request) is not specified, a JVM's minimum heap size is set to 128M. The JVM's maximum heap size is not defined to allow the memory to increase as needed. This is ideal for single node environments in test and development." It also warns: "Total JVM memory usage can be a lot more than the maximum heap size." ([configuring docs](https://strimzi.io/docs/operators/latest/configuring.html))
- Estimate, not measured, built from the limits above plus the chart default for the operator:
  - cluster operator 384Mi
  - broker/controller ~1–1.5Gi
  - topic operator ~256–384Mi
  - **total about 2–2.5 GiB**
- That fits in the ~17 GiB available alongside the other platform components (DISCOVERY facts).

**Client connectivity.**
- *Inside the cluster*, the bootstrap is `fraud-kafka-kafka-bootstrap.<ns>.svc:9092`. The quickstart test clients use `--bootstrap-server my-cluster-kafka-bootstrap:9092` ([quickstart](https://strimzi.io/quickstarts/)). The scoring service and the replay producer should both run in-cluster, so neither needs external access.
- *Outside the cluster* (laptop debugging, notebooks), a NodePort listener plus kind port mappings works. The kind docs say: "To use port mappings with NodePort, the kind node containerPort and the service nodePort needs to be equal." ([kind configuration](https://kind.sigs.k8s.io/docs/user/configuration/))
  ```yaml
  # kind-config.yaml
  kind: Cluster
  apiVersion: kind.x-k8s.io/v1alpha4
  nodes:
    - role: control-plane
      extraPortMappings:
        - { containerPort: 32100, hostPort: 32100, listenAddress: "127.0.0.1" }  # bootstrap
        - { containerPort: 32000, hostPort: 32000, listenAddress: "127.0.0.1" }  # broker 0
  ```
  Clients then use `bootstrap.servers=localhost:32100`.
- The bootstrap address is published in the Kafka status: `kubectl get kafka fraud-kafka -o=jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}'` ([deploying docs](https://strimzi.io/docs/operators/latest/deploying.html)).
- `kubectl port-forward` to the bootstrap service does **not** work reliably. Brokers advertise their own addresses, which is why `advertisedHost` exists.

**What changes on GKE** (Terraform only, never applied; ADR-0003):
- Node pools:
  - use separate `controller` (3 replicas) and `broker` (≥3 replicas) pools
  - use replication factor 3 and `min.insync.replicas: 2`
  - set `rack.topologyKey: topology.kubernetes.io/zone` for zone spreading
- Storage: use `class: standard-rwo` or `premium-rwo` on the persistent claims.
- External listener: `type: loadbalancer` or `ingress`. NodePort is a kind-only choice. In-cluster clients keep the same `*-kafka-bootstrap:9092` address, so the scoring and replay charts are unchanged.
- The Helm/Argo CD install of the operator is the same.
- The managed alternative (Google Managed Service for Apache Kafka) belongs in the build-vs-buy section, as Q26 already says.

#### Q2. Python Kafka client

| Client | Latest (date) | Python 3.13 | Nature | Notes |
|---|---|---|---|---|
| confluent-kafka | **2.15.1** (PyPI 2026-09-10; librdkafka v2.15.1, 2026-09-09) | cp313 wheels incl. `manylinux_2_28_x86_64`; also cp314 / 3.14t | C extension over librdkafka | KIP-848 GA since 2.12.0; `AIOProducer`/`AIOConsumer` non-experimental since 2.13.0 |
| aiokafka | **0.14.0** (2026-04-29) | cp313 + cp314 wheels; requires >=3.10 | asyncio, mostly pure Python | 0.15.0 (unreleased) drops 3.10 |
| kafka-python | **3.0.11** (2026-08-16) | pure-Python wheel (works on 3.13) | pure Python | Actively maintained again; 3.0.x released June to August 2026 |

Sources: PyPI JSON ([confluent-kafka](https://pypi.org/pypi/confluent-kafka/json), [aiokafka](https://pypi.org/pypi/aiokafka/json), [kafka-python](https://pypi.org/pypi/kafka-python/json)); [confluent CHANGELOG](https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md); [aiokafka CHANGES.rst](https://github.com/aio-libs/aiokafka/blob/master/CHANGES.rst); [kafka-python CHANGES.md](https://github.com/dpkp/kafka-python/blob/master/CHANGES.md); [librdkafka releases](https://github.com/confluentinc/librdkafka/releases).

Key quotes:
- confluent 2.12.0: "Starting with __confluent-kafka-python 2.12.0__, the next generation consumer group rebalance protocol defined in **KIP-848** is **production-ready**." Also: "`group.protocol` configuration property dictates whether to use the new `consumer` protocol or older `classic` protocol. It defaults to `classic` if not provided." ([CHANGELOG](https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md))
- confluent 2.13.0 (2025-12-15): "Remove experimental module designation for Async classes (#2143)" and "Expose deterministic partitioner functions (#2116)".
- `AIOConsumer` docstring: "Every method dispatches the underlying blocking Consumer call to a thread pool executor and returns an awaitable … librdkafka's Consumer is not thread-safe, so concurrent access to the same AIOConsumer instance is serialized". On `poll()` it says: "prefer consume() over poll(): consume() can retrieve multiple messages per call and amortize the async overhead across the entire batch." ([_AIOConsumer.py](https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/aio/_AIOConsumer.py))
- Performance is a vendor claim with no independent benchmark found: "Built on `librdkafka` (C library) for maximum throughput and minimal latency, significantly outperforming pure Python implementations." ([README](https://github.com/confluentinc/confluent-kafka-python/blob/master/README.md)) It does not matter here. At 1 sim day ≈ 30 s and about 2,500 Sparkov transactions per simulated day, the stream is about 85 msg/s.
- KIP-848 requirements, from the [migration guide](https://github.com/confluentinc/confluent-kafka-python/blob/master/docs/kip-848-migration-guide.md):
  - "Broker version **4.0.0+**"
  - "Rebalance callbacks (**incremental only**)"
  - "The `partitions` list passed to `incremental_assign()` and `incremental_unassign()` contains only the **incremental changes**"
  - "**Do not** use `consumer.assign()` or `consumer.unassign()` when using `group.protocol='consumer'`"
  - Strimzi 1.2.0 ships Kafka 4.3.1, so KIP-848 is available.

**Choice:** use **confluent-kafka 2.15.x**. Reasons:
- It is the only one of the three with GA KIP-848 support.
- It has `on_assign`/`on_revoke`/`on_lost` callbacks, `store_offsets`, and seek-on-assign.
- It ships wheels for Python 3.13.
- It uses the same librdkafka as kcat.

aiokafka is a reasonable second choice for a pure-asyncio style. kafka-python is maintained again, but its speed (pure Python) and the "Performance Note" in the confluent README count against it.

**Per-card ordering.** Kafka guarantees order only within one partition. Producing every Transaction with `key = card_hash` sends all of a card's events to one partition, so one consumer sees them in order. That holds as long as:
1. every producer of the topic uses the **same partitioner**;
2. the partition count never changes;
3. the producer keeps order under retries (`enable.idempotence=true`).

**Partitioner mismatch: the question's premise is wrong for librdkafka.** The default is **not** murmur2 in confluent-kafka:
- librdkafka `partitioner` defaults to `consistent_random`, documented as "CRC32 hash of key (Empty and NULL keys are randomly partitioned)". The Java-compatible option is a separate setting: "`murmur2_random` - Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned. This is functionally equivalent to the default partitioner in the Java Producer.)" ([librdkafka CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md))
- aiokafka `DefaultPartitioner`: "Hashes key to partition using murmur2 hashing (from java client)" ([aiokafka/partitioner.py](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/partitioner.py))
- kafka-python `DefaultPartitioner`: "Hashes key to partition using murmur2 hashing (from java client)" ([kafka/partitioner/default.py](https://github.com/dpkp/kafka-python/blob/master/kafka/partitioner/default.py))
- Java (Kafka 4.3): "If no partition is specified but a key is present, choose a partition based on a hash of the key." The hash is murmur2 ([producer configs](https://kafka.apache.org/43/generated/producer_config.html)).

Consumers never hash keys. They read whatever partition a record landed in, so the producer/consumer library pairing cannot cause a mismatch. The risk is **two producers** (or a producer plus code that computes partitions itself) using different hashes. Examples:
- a Python confluent producer (CRC32) and a Java `kafka-console-producer` or aiokafka tool (murmur2) both writing card-keyed records, which splits one card across partitions and breaks ordering and state ownership;
- a test that predicts partition ownership with the wrong hash.

Fix: set `"partitioner": "murmur2_random"` explicitly on every confluent producer. The project then matches Java and aiokafka/kafka-python. Never compute partition numbers in application code; store `msg.partition()` instead.

```python
from confluent_kafka import Producer
producer = Producer({
    "bootstrap.servers": "fraud-kafka-kafka-bootstrap.kafka.svc:9092",
    "partitioner": "murmur2_random",   # Java-compatible; librdkafka default is CRC32
    "enable.idempotence": True,        # librdkafka default is false
    "linger.ms": 5,
    "compression.type": "lz4",
})
```
On the librdkafka idempotence default: "`enable.idempotence` … default false … When set to `true`, the producer will ensure that messages are successfully produced exactly once and in the original produce order." ([CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md))

#### Q3. Stateful consumer: state, rebalances, offsets

**Primary-source pattern.** The Kafka `KafkaConsumer` Javadoc (4.3), section "Storing Offsets Outside Kafka" ([Javadoc](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)):
> "If the results of the consumption are being stored in a relational database, storing the offset in the database as well can allow committing both the results and offset in a single transaction. Thus either the transaction will succeed and the offset will be updated based on what was consumed or the result will not be stored and the offset won't be updated."

and
> "If the partition assignment is done automatically special care is needed to handle the case where partition assignments change. … when partitions are taken from a consumer the consumer will want to commit its offset for those partitions … When partitions are assigned to a consumer, the consumer will want to look up the offset for those new partitions and correctly initialize the consumer to that position".

This fits the project well. Predictions and card state both go to Postgres, so **one Postgres transaction per micro-batch** writes three things together:
- the prediction rows (champion and challenger),
- the dirty card states,
- the next offset per partition.

Kafka's own committed offset then serves only lag dashboards. Postgres is the source of truth. The result is **effectively exactly-once into Postgres, with no Kafka transactions**. A crash loses nothing. On restart the consumer seeks to the stored offset and recomputes the in-flight events, and the state function is pure and deterministic (ADR-0002).

**confluent-kafka API facts** (from docstrings in [Consumer.c](https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/src/Consumer.c)):
- `on_assign` is a "callback to provide handling of customized offsets on completion of a successful partition re-assignment."
- `on_revoke` is a "callback to provide handling of offset commits to a customized store on the start of a rebalance operation."
- `on_lost` handles "the case the partition assignment has been lost. If not specified, lost partition events will be delivered to on_revoke, if specified. Partitions that have been lost may already be owned by other members in the group and therefore committing offsets, for example, may fail."
- `poll()`: "Callbacks may be called from this method, such as `on_assign`, `on_revoke`". The callbacks therefore run on the polling thread, so state can be owned by that one thread without locks.

**Sketch** (sync consumer on a dedicated thread, KIP-848 incremental protocol):
```python
# scoring/stream.py
import psycopg
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from features import update_state, empty_state   # the ADR-0002 pure function

TOPIC = "transactions"

class StreamScorer:
    def __init__(self, conf, dsn, models):
        self.c = Consumer({
            **conf,
            "group.id": "scoring",
            "group.protocol": "consumer",        # KIP-848, broker >= 4.0
            "enable.auto.commit": False,         # Postgres holds the truth
            "auto.offset.reset": "earliest",
        })
        self.db = psycopg.connect(dsn)           # owned by this thread only
        self.models = models
        self.state: dict[int, dict[bytes, dict]] = {}   # partition -> card_hash -> state

    # rebalance callbacks: they run inside poll(), on this same thread
    def on_assign(self, consumer, partitions):
        with self.db.cursor() as cur:
            for tp in partitions:
                cur.execute("SELECT card_hash, state FROM card_state WHERE topic=%s AND partition=%s",
                            (TOPIC, tp.partition))
                self.state[tp.partition] = {k: v for k, v in cur}
                cur.execute("SELECT next_offset FROM consumer_offsets WHERE topic=%s AND partition=%s",
                            (TOPIC, tp.partition))
                row = cur.fetchone()
                tp.offset = row[0] if row else OFFSET_BEGINNING
        consumer.incremental_assign(partitions)  # seeks to the offsets set above

    def on_revoke(self, consumer, partitions):
        self.flush()                             # persist the pending batch while we still own it
        for tp in partitions:
            self.state.pop(tp.partition, None)
        consumer.incremental_unassign(partitions)

    def on_lost(self, consumer, partitions):
        self.pending.clear()                     # someone else may own them now: do not write
        for tp in partitions:
            self.state.pop(tp.partition, None)
        consumer.incremental_unassign(partitions)

    def run(self, stop):
        self.c.subscribe([TOPIC], on_assign=self.on_assign,
                         on_revoke=self.on_revoke, on_lost=self.on_lost)
        self.pending = []
        while not stop.is_set():
            for msg in self.c.consume(num_messages=500, timeout=0.2):
                if msg.error():
                    continue                     # log it; partition EOF etc.
                txn = decode(msg.value())
                p = msg.partition()
                s = self.state[p].get(txn.card_hash) or empty_state()
                s, x = update_state(s, txn)      # same function as the training replay
                self.state[p][txn.card_hash] = s
                self.pending.append((msg, txn, s, self.models.score(x, txn)))
            if self.pending:
                self.flush()
        self.flush(); self.c.close()

    def flush(self):
        if not self.pending:
            return
        with self.db.transaction(), self.db.cursor() as cur:
            with cur.copy("COPY predictions (txn_id, role, model_version, score, decision, applied,"
                          " event_time, kafka_partition, kafka_offset) FROM STDIN") as cp:
                for msg, txn, _, outs in self.pending:
                    for o in outs:               # champion + challenger (shadow)
                        cp.write_row((txn.id, o.role, o.version, o.score, o.decision, o.applied,
                                      txn.event_time, msg.partition(), msg.offset()))
            last = {}
            for msg, txn, s, _ in self.pending:
                last[(msg.partition(), txn.card_hash)] = s
            cur.executemany(
                "INSERT INTO card_state (topic, partition, card_hash, state) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (card_hash) DO UPDATE SET state=EXCLUDED.state, partition=EXCLUDED.partition",
                [(TOPIC, p, k, Jsonb(v)) for (p, k), v in last.items()])
            nxt = {}
            for msg, *_ in self.pending:
                nxt[msg.partition()] = msg.offset() + 1
            cur.executemany(
                "INSERT INTO consumer_offsets (topic, partition, next_offset) VALUES (%s,%s,%s) "
                "ON CONFLICT (topic, partition) DO UPDATE SET next_offset=EXCLUDED.next_offset",
                [(TOPIC, p, o) for p, o in nxt.items()])
        self.c.commit(offsets=[TopicPartition(TOPIC, p, o) for p, o in nxt.items()],
                      asynchronous=True)        # informational only (lag metrics)
        self.pending.clear()
```
Notes on the sketch:
- It is illustrative and has not been run. `decode`, `Jsonb` (`psycopg.types.json.Jsonb`) and `models.score` are placeholders.
- Loading every card of a partition into memory on assign is fine at Sparkov scale. Sparkov has about 1,000 cards, so state is a few MB.
- If the team prefers the classic eager protocol, replace `incremental_assign`/`incremental_unassign` with `assign`/`unassign` and treat the partition lists as the full assignment.

**Ordering rule if Kafka offsets were the source of truth instead** (the periodic-snapshot variant): snapshot state *first*, commit the offset *after*. Never commit an offset past the last durable snapshot. On restart, replay from the snapshot's offset.

**At-least-once vs exactly-once:**
- *At-least-once* (auto-commit, periodic snapshot): the simplest option. Restarts can produce duplicate prediction rows, which need `ON CONFLICT DO NOTHING` on `(txn_id, role)`. COPY cannot do that directly, so you would COPY into a temp table and then `INSERT … SELECT … ON CONFLICT`.
- *Kafka EOS transactions* (`transactional.id`, `send_offsets_to_transaction`) only cover outputs **written back to Kafka**. They do not cover Postgres, so they are the wrong tool here.
- *Offsets in Postgres in the same transaction* (above) gives exactly-once effects for this sink at about the same complexity as at-least-once. **Recommended.** It is also a strong interview talking point.

#### Q4. Service shape, model loading, Postgres writes

**One deployable or two.** Recommendation: **one image and one FastAPI app** containing:
- a background stream consumer **thread**, started and stopped in `lifespan`;
- `POST /score` for contract tests and k6;
- `/healthz` and `/readyz` (ready only after models are loaded and partitions assigned);
- `/metrics`.

Run it with **one uvicorn worker per pod**. In-memory card state is per process, so multiple workers would split the state.

FastAPI says the lifespan code before `yield` "will be executed **before** the application starts" and the code after "**after** the application has finished". It also warns: "If you provide a `lifespan` parameter, `startup` and `shutdown` event handlers will no longer be called." ([FastAPI lifespan events](https://fastapi.tiangolo.com/advanced/events/); FastAPI 0.142.2, PyPI 2026-09-30)

The same image can run as two Deployments (`MODE=stream` / `MODE=api`) through Helm values if the walkthrough needs a "separate microservices" story. The platform already shows several services: replay producer, scorer, Airflow, MLflow, LLM.

Important design constraint for `/score` (Q34 contract and k6 tests):
- It must **not mutate stream state** and must **not write into the monitoring tables**. Otherwise k6 traffic corrupts the velocity features and the drift statistics.
- Make `/score` a dry run: read the card's current state if this pod owns it (else `empty_state()`), call `update_state` on a copy, score, and return the decision.
- Optionally log the call with `source='http'` to a separate table.

```python
# scoring/app.py
import threading, asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.models = ModelHolder(name="fraud-lgbm")    # loads @champion and @challenger
    app.state.models.refresh()
    app.state.stop = threading.Event()
    app.state.scorer = StreamScorer(kafka_conf(), dsn(), app.state.models)
    t = threading.Thread(target=app.state.scorer.run, args=(app.state.stop,), daemon=True)
    t.start()
    poller = asyncio.create_task(app.state.models.poll_aliases(every_s=15))
    yield
    poller.cancel()
    app.state.stop.set()
    await asyncio.to_thread(t.join, 30)                  # flush and close the consumer cleanly

app = FastAPI(lifespan=lifespan)

@app.post("/score")
def score(txn: TransactionIn) -> DecisionOut:          # sync def -> runs in the threadpool
    s = app.state.scorer.peek_state(txn.card_hash)      # read-only copy
    _, x = update_state(s, txn)
    return app.state.models.score(x, txn)[0].to_out()   # champion decision only
```
Why a plain thread rather than `AIOConsumer`:
- The rebalance callbacks need sync DB I/O.
- One thread owning the consumer and the state avoids lock juggling.
- confluent's `AIOConsumer` just "dispatches the underlying blocking Consumer call to a thread pool executor" anyway ([source](https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/aio/_AIOConsumer.py)).
- LightGBM calls its C library through `ctypes.CDLL`, which releases the GIL during the call. Python's docs describe `PyDLL` as behaving "like CDLL instances, except that the Python GIL is **not** released during the function call" ([ctypes docs](https://docs.python.org/3.13/library/ctypes.html)). So scoring in the consumer thread does not starve the HTTP event loop.

**Loading champion and challenger from MLflow by alias** (MLflow 3.16.1, PyPI 2026-09-16):
- The [Model Registry docs](https://mlflow.org/docs/latest/ml/model-registry/) say: "Model aliases allow you to assign a mutable, named reference to a particular version of a registered model." They give the URI form `models:/MyModel@champion`, and say: "You can then update the model serving production traffic by reassigning the champion alias to a different model version."
- `MlflowClient.get_model_version_by_alias(name: str, alias: str) -> ModelVersion` returns "the model version instance by name and alias" ([mlflow.client API](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.client.html)).
- Hot-reload pattern (polling; MLflow OSS has no push notification):
```python
import asyncio
import mlflow, mlflow.lightgbm
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

class ModelHolder:
    def __init__(self, name):
        self.name, self.client = name, MlflowClient()
        self.slots = {}                                   # role -> (version, booster); swapped atomically

    def _resolve(self, alias):
        try:
            return self.client.get_model_version_by_alias(self.name, alias).version
        except MlflowException:
            return None                                   # e.g. no challenger right now

    def refresh(self):
        new = dict(self.slots)
        for role in ("champion", "challenger"):
            v = self._resolve(role)
            if v is None:
                new.pop(role, None)
            elif role not in new or new[role][0] != v:
                new[role] = (v, mlflow.lightgbm.load_model(f"models:/{self.name}/{v}"))
        self.slots = new                                  # one reference assignment; readers see old or new

    async def poll_aliases(self, every_s):
        while True:
            await asyncio.sleep(every_s)
            await asyncio.to_thread(self.refresh)
```
Notes on the pattern:
- Load by **resolved version number**, not `@alias`, and log `model_version` on every prediction row. If the alias moves between resolve and load, you still know exactly which model scored what.
- Shadow mode (Q16): the challenger scores every event and its row is written with `applied=false`.
- Canary at 10%: choose deterministically, for example `int.from_bytes(card_hash[:8]) % 100 < 10`, then set `applied=true` on the challenger row and `applied=false` on the champion row for those cards. Hashing by card keeps each card in one arm.

**Writing predictions to Postgres** (psycopg 3.3.6, PyPI 2026-09-18; psycopg-pool 3.3.3):
- From the [psycopg COPY docs](https://www.psycopg.org/psycopg3/docs/basic/copy.html): "COPY is one of the most efficient ways to load data into the database". `with cursor.copy("COPY sample (col1, col2, col3) FROM STDIN") as copy: for record in records: copy.write_row(record)`. The async form is `async with cursor.copy(...) as copy: await copy.write(...)`.
- From the [pipeline docs](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html), starting from Psycopg 3.1: "`executemany()` makes use internally of the pipeline mode; as a consequence there is no need to handle a pipeline block just to call `executemany()` once."
- So: COPY for the append-only `predictions` table, and `executemany` upserts for `card_state` and `consumer_offsets`, all in one transaction per micro-batch (Q3 sketch).
- Use a sync connection in the consumer thread. Use psycopg async (`AsyncConnectionPool`) only if `/score` ever writes, which the recommendation says it should not.

#### Q5. Replay producer on an accelerated clock

**Concepts** (Flink 2.3 docs, [Time concepts](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/)):
- "Event time is the time that each individual event occurred on its producing device."
- "Processing time refers to the system time of the machine that is executing the respective operation."
- "A Watermark(t) declares that event time has reached time t in that stream, meaning that there should be no more elements from the stream with a timestamp t' <= t."
- On idle partitions ([generating watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/)): "If one of the input splits/partitions/shards does not carry events for a while this means that the `WatermarkGenerator` also does not get any new information on which to base a watermark. We call this an idle input or an idle source."

**Replay pattern (anchor-and-sleep).** Fix `(sim_t0, wall_t0, speedup)`. Each event is due at `wall_t0 + (event_time - sim_t0) / speedup`. Sort by event time, sleep until each event is due, then produce. 1 sim day ≈ 30 s gives `speedup = 86400 / 30 = 2880`. Replaying 12 months takes about 3 hours of wall time.
```python
# replay/producer.py
import time, json
from datetime import datetime, timedelta

class SimClock:
    def __init__(self, sim_t0: datetime, speedup: float):
        self.sim_t0, self.speedup, self.wall_t0 = sim_t0, speedup, time.monotonic()
    def due(self, event_time: datetime) -> float:          # monotonic wall time when the event is due
        return self.wall_t0 + (event_time - self.sim_t0).total_seconds() / self.speedup
    def now(self) -> datetime:                             # current simulated time
        return self.sim_t0 + timedelta(seconds=(time.monotonic() - self.wall_t0) * self.speedup)

def replay(rows, producer, speedup=2880.0, heartbeat_every_sim_s=3600):
    clock = SimClock(rows[0].event_time, speedup)          # rows pre-sorted by event_time
    next_hb = rows[0].event_time
    for r in rows:
        delay = clock.due(r.event_time) - time.monotonic()
        if delay > 0:
            time.sleep(delay)                              # if behind, do not sleep: catch up
        producer.produce("transactions", key=r.card_hash, value=json.dumps(r.payload()),
                         headers={"event_time": r.event_time.isoformat()})
        producer.poll(0)
        if r.event_time >= next_hb:                        # clock tick for consumers with idle partitions
            producer.produce("sim-clock", value=r.event_time.isoformat())
            next_hb += timedelta(seconds=heartbeat_every_sim_s)
    producer.flush()
```
Design notes:
- Make `speedup` and `start/end` CLI flags. Keep a **max-burst** guard: if the producer falls behind, it sends immediately rather than sleeping, so lag shows up downstream instead of the clock drifting.
- Run the producer **in-cluster** as a Kubernetes Job (or Deployment with a resume offset). Persist the last produced `event_time` to Postgres so a restarted replay resumes rather than duplicating.

**How downstream components read "now":**
- *Scoring service:* "now" **is the Transaction's event time**. Features are computed as of `txn.event_time` inside the pure state function. Training replay does the same, so wall-clock time never enters the feature path (ADR-0002 parity).
- *Per-partition watermark:* use `max(event_time seen)` per assigned partition, and the minimum across partitions for anything that needs "all events up to t have arrived" (for example closing a monitoring window). Publish it as a gauge and store it in a one-row `sim_clock` table.
- *Batch jobs, label maturation, drift windows (Airflow):* read the simulated time from that `sim_clock` table, or from the `sim-clock` heartbeat topic. Never call `datetime.now()`. Windows are defined in event time, for example "PSI over sim-day D". A job is triggered when the watermark passes the window's end, not on a wall-clock cron. At 30 s per sim day, a cron would also be far too coarse.
- *Delayed labels:* the label producer uses the same clock and emits a label when `sim_now >= event_time + label_delay`.

**Kafka record timestamps: do not stamp 2019/2020 event times as `CreateTime`.** Kafka 4.3 [topic configs](https://kafka.apache.org/43/generated/topic_config.html):
- `message.timestamp.after.max.ms` defaults to "3600000 (1 hour)", and with `CreateTime` "the message will be rejected if the difference in timestamps exceeds this specified threshold".
- `message.timestamp.before.max.ms` defaults to `9223372036854775807`, so past timestamps are accepted.
- Time-based retention uses message timestamps. KIP-32 says: "The log retention will take a look at the last time index entry in the time index file. Because the last entry will be the latest timestamp in the entire log segment. If that entry expires, the log segment will be deleted." ([KIP-32](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message)) With the default `retention.ms` of "604800000 (7 days)", segments stamped 2019 would be eligible for deletion almost at once.
- KIP-32 itself recommends: "If CreateTime is required, it can always be put into the message payload."

So leave the Kafka timestamp as the produce wall time (do not pass `timestamp=`), and carry `event_time` in the payload and a header. Alternatively set the topic to `LogAppendTime` or `retention.ms: -1`.

### Recommendation

1. **Kafka:**
   - Strimzi **1.2.0** (Kafka 4.3.1, KRaft, `kafka.strimzi.io/v1`), installed by Argo CD from the OCI Helm chart `oci://quay.io/strimzi-helm/strimzi-kafka-operator`, pinned to `1.2.0`. Manage CRDs separately for upgrades.
   - One dual-role `KafkaNodePool` replica with a 768m heap and a 1.5Gi limit, the Topic Operator only, and a `transactions` topic with **6 partitions, fixed forever**, plus `labels` and `sim-clock` topics. Budget about 2–2.5 GiB.
   - All clients in-cluster via `fraud-kafka-kafka-bootstrap:9092`. A NodePort listener (`localhost:32100`) with kind `extraPortMappings` for laptop debugging only.
   - On GKE: 3 controllers + 3 brokers, RF 3, `min.insync.replicas: 2`, a `standard-rwo` storage class and zone rack-awareness. The application charts stay the same.
2. **Client:** confluent-kafka **2.15.x** for both producer and consumer.
   - Producer: `partitioner=murmur2_random`, `enable.idempotence=true`, key = hashed `cc_num`.
   - Consumer: `group.protocol=consumer` (KIP-848) and incremental rebalance callbacks.
3. **State and offsets:** pure `update_state` over a per-partition dict owned by one consumer thread. On each micro-batch (≤500 msgs or 200 ms), **one Postgres transaction** writes the prediction rows (COPY), the dirty card states (upsert) and the next offsets. `on_assign` restores state and seeks to the stored offsets. `on_revoke` flushes and then drops state. `on_lost` drops state without writing. This gives exactly-once effects into Postgres without Kafka transactions.
4. **Service:**
   - One FastAPI image. `lifespan` starts the consumer thread and an MLflow alias poller (15 s) that loads `@champion`/`@challenger` by resolved version and swaps them atomically. Run one uvicorn worker per pod and replicas ≤ partitions.
   - `POST /score` is a side-effect-free dry run for contract and k6 tests.
   - Every prediction row records `model_version`, `role`, `applied` and `event_time`.
   - Optionally split into stream and api Deployments from the same image through Helm values.
5. **Clock:**
   - The replay producer uses anchor-and-sleep at `speedup=2880`, carries event time in the payload and headers (not as the Kafka timestamp), and emits `sim-clock` heartbeats.
   - The scorer's "now" is the transaction's event time.
   - Batch and monitoring jobs read the simulated watermark from Postgres `sim_clock` and never call `datetime.now()`.

### Risks and gotchas

- **Contradicts the question's premise (Q15 / ADR-0002 ordering):** confluent-kafka's default partitioner is CRC32 (`consistent_random`), not murmur2. A mixed-tool producer set silently splits cards across partitions. Set `partitioner=murmur2_random` everywhere.
- **Refines ADR-0002's consequence "a crash loses state since the last snapshot":**
  - With state and offsets in the same Postgres transaction, nothing is lost.
  - Even with periodic snapshots, nothing is lost *if* the offset is never committed ahead of the snapshot, because Kafka retains the events and the function is deterministic.
  - The ADR's statement is only true if offsets are committed independently (for example with auto-commit). Consider updating the ADR wording.
- **Q34 conflict risk:** if `/score` mutates card state or writes to the predictions table, k6 load corrupts velocity features and drift and shadow metrics. Keep it a dry run.
- **Q10 risk:** stamping historical event time as the Kafka `CreateTime` makes retention delete data at once (7-day default against 2019 timestamps). Shifted-to-future times over 1 h are rejected. Keep event time in the payload.
- **Never change the partition count** of `transactions`. It remaps cards to partitions, and stored `card_state.partition` and offsets become wrong. Replicas above the partition count sit idle.
- **Old Strimzi tutorials** (`v1beta2`, `strimzi.io/kraft` annotations, ZooKeeper sections, `.spec.kafka.resources`) fail on 1.x. Strimzi 1.3.0 (upcoming) requires Kubernetes ≥ 1.32.
- **Helm does not upgrade CRDs.** A Strimzi chart bump without applying new CRDs leaves the operator and the CRDs out of step.
- **Strimzi docs:** "each watched namespace should contain only one instance of a specific component type, such as one Kafka cluster". Sharing one Kafka between `staging` and `prod` namespaces needs a deliberate choice: one shared `kafka` namespace with prefixed topics or consumer groups, or two small clusters (about 2× the memory).
- **KIP-848 contract:** callbacks receive *incremental* partition lists. `assign()`/`unassign()` are forbidden with `group.protocol=consumer`. Session timeouts are broker-side (`group.consumer.session.timeout.ms`). Static membership fences the *new* member on a duplicate `group.instance.id` ([migration guide](https://github.com/confluentinc/confluent-kafka-python/blob/master/docs/kip-848-migration-guide.md)).
- **Rebalance callbacks run inside `poll()/consume()`.** A slow Postgres restore in `on_assign` counts against `max.poll.interval.ms`. That is fine at Sparkov scale (about 1k cards), but measure it.
- **`on_lost`:** do not flush pending results. Another member may already own the partition, and the docstring warns that "committing offsets, for example, may fail".
- **The uvicorn `--workers N` flag** creates N consumers with N private states, and `/score` hits a random one. Use one worker per pod and scale with pods.
- **NodePort `advertisedHost: localhost`** has not been tested on this machine. If it fails, run debugging clients in-cluster (`kubectl run … quay.io/strimzi/kafka:1.2.0-kafka-4.3.1`, from the quickstart).
- **The strongest performance claims** for confluent-kafka are vendor statements. Throughput is irrelevant at about 85 msg/s anyway.
- **Unverified estimate:** the 2–2.5 GiB Kafka footprint is built from the configured limits, not measured.

### Sources

- Strimzi releases (1.2.0 on 2026-08-20; 1.1.0 on 2026-06-27; 1.0.1 on 2026-06-17): https://github.com/strimzi/strimzi-kafka-operator/releases and https://api.github.com/repos/strimzi/strimzi-kafka-operator/releases
- Strimzi CHANGELOG: https://github.com/strimzi/strimzi-kafka-operator/blob/main/CHANGELOG.md
- Strimzi quickstart (1.2.0 / Kafka 4.3.1 images): https://strimzi.io/quickstarts/
- Strimzi single-node example: https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml
- Strimzi topic example @1.2.0: https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/1.2.0/packaging/examples/topic/kafka-topic.yaml
- Strimzi deploying guide (1.2.0): https://strimzi.io/docs/operators/latest/deploying.html
- Strimzi configuring guide (node ports, JVM memory): https://strimzi.io/docs/operators/latest/configuring.html
- Strimzi Helm values @1.2.0: https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/1.2.0/helm-charts/helm3/strimzi-kafka-operator/values.yaml
- Helm CRD caveats: https://helm.sh/docs/chart_best_practices/custom_resource_definitions/
- kind configuration (extraPortMappings, NodePort): https://kind.sigs.k8s.io/docs/user/configuration/ ; kind v0.33.0: https://github.com/kubernetes-sigs/kind/releases/latest
- confluent-kafka on PyPI (2.15.1, 2026-09-10): https://pypi.org/pypi/confluent-kafka/json
- confluent-kafka CHANGELOG: https://github.com/confluentinc/confluent-kafka-python/blob/master/CHANGELOG.md
- confluent-kafka README: https://github.com/confluentinc/confluent-kafka-python/blob/master/README.md
- confluent-kafka KIP-848 migration guide: https://github.com/confluentinc/confluent-kafka-python/blob/master/docs/kip-848-migration-guide.md
- confluent-kafka Consumer docstrings: https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/src/Consumer.c
- confluent-kafka AIOConsumer: https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/aio/_AIOConsumer.py
- librdkafka CONFIGURATION.md (partitioner, idempotence, offsets): https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md ; v2.15.1 on 2026-09-09: https://github.com/confluentinc/librdkafka/releases
- aiokafka on PyPI (0.14.0, 2026-04-29): https://pypi.org/pypi/aiokafka/json ; changelog: https://github.com/aio-libs/aiokafka/blob/master/CHANGES.rst ; partitioner: https://github.com/aio-libs/aiokafka/blob/master/aiokafka/partitioner.py
- kafka-python on PyPI (3.0.11, 2026-08-16): https://pypi.org/pypi/kafka-python/json ; changelog: https://github.com/dpkp/kafka-python/blob/master/CHANGES.md ; partitioner: https://github.com/dpkp/kafka-python/blob/master/kafka/partitioner/default.py
- Kafka 4.3 producer configs: https://kafka.apache.org/43/generated/producer_config.html
- Kafka 4.3 topic configs: https://kafka.apache.org/43/generated/topic_config.html
- Kafka 4.3 KafkaConsumer Javadoc ("Storing Offsets Outside Kafka"): https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
- KIP-32 (message timestamps, retention): https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message
- FastAPI lifespan events (FastAPI 0.142.2): https://fastapi.tiangolo.com/advanced/events/
- MLflow Model Registry (aliases; MLflow 3.16.1): https://mlflow.org/docs/latest/ml/model-registry/
- MLflow client API (get_model_version_by_alias): https://mlflow.org/docs/latest/api_reference/python_api/mlflow.client.html
- psycopg COPY: https://www.psycopg.org/psycopg3/docs/basic/copy.html
- psycopg pipeline mode / executemany: https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html
- psycopg 3.3.6 / psycopg-pool 3.3.3 on PyPI: https://pypi.org/pypi/psycopg/json , https://pypi.org/pypi/psycopg-pool/json
- Python ctypes (GIL release by CDLL vs PyDLL): https://docs.python.org/3.13/library/ctypes.html
- Flink 2.3 time concepts: https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/
- Flink idle sources: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/
- k6 latest (v2.3.0) for the Q34 load test: https://github.com/grafana/k6/releases/latest


---

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


---

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


---

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


---

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


---

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


---

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


---
