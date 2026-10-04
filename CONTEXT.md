# Context: Fraud ML Platform glossary

Definitions only. Implementation details belong in ARCHITECTURE.md; decisions in `docs/adr/`.

- **Transaction** — one card payment event from the replayed dataset: card, merchant, category, amount, location, event time.
- **Decision** — the action the platform takes on a scored Transaction: **block**, **review**, or **allow**.
- **Label** — the ground-truth fraud / not-fraud outcome of a Transaction. Arrives later than the Transaction (delayed), and never arrives for blocked Transactions outside the Exploration sample.
- **Exploration sample** — a small random fraction of Transactions that would have been blocked but are allowed through so they receive a Label; the counter-measure to the degenerate feedback loop.
- **Degenerate feedback loop** — the model's own Decisions censor the Labels it later learns from (blocked fraud is never confirmed), biasing future training data.
- **Champion** — the model version currently serving Decisions.
- **Challenger** — a candidate model version evaluated against the Champion (shadow, then canary) before it may replace it.
- **Shift** — a deliberate, injected change in the stream: **covariate** (input distribution changes), **label** (fraud base rate changes), or **concept** (relationship between inputs and fraud changes).
- **Account** — a card, identified by its card number (`cc_num`). The unit for per-account history and batch risk scores.
- **Simulated clock** — the platform's notion of "now", driven by Transaction event time and accelerated relative to wall-clock time.
- **Alert budget** — the fixed fraction of Transactions the platform may act on (block + review). Thresholds are chosen to hit it; models are compared at equal budget.
- **Matured label** — a Label that has arrived, or a legitimate outcome confirmed because the chargeback window closed without one.
- **Shadow mode** — a Challenger scores live traffic and its predictions are logged, but its Decisions are not applied.
- **Canary** — a Challenger whose Decisions are applied to a small share of live traffic before full promotion.
- **Stateless retraining** — training a new model from scratch on a recent data window.
- **Stateful retraining** — continuing to train the existing model on new data only (warm start).
- **Exploration weight** — the inverse-propensity weight (1 / exploration rate) applied to Exploration-sample rows in training so they stand in for all blocked traffic.
- **Case summary** — a short LLM-written explanation of a flagged Transaction for a fraud analyst.
- **Data snapshot** — an immutable, versioned copy of a dataset in the S3-compatible object store, whose SHA-256 content hash is recorded with every training run.
- **Bronze / silver / gold** — the raw, cleaned, and feature-ready layers of the batch data pipeline.
- **Staging / prod** — the two deployment environments (Kubernetes namespaces); changes reach prod only after passing staging.
- **Alert** — a recorded finding by the monitoring job that a rule fired for a closed sim-day window (feature drift, per-category drift, action rate, early fraud rate, concept gap, schema skew). An Alert can trigger retraining.
- **Promotion gate** — the rule a Challenger must pass on Matured labels to advance (Shadow mode → Canary → Champion): better fraud-dollar recall at equal Alert budget, no PR-AUC loss, latency in budget, no segment regression.
- **Replay window** — the span of 2020 event time the replayer streams in one run.
- **Golden set** — the hand-written evaluation cases used to gate Case summary prompt and model changes.
- **Platform owner** — the person who builds, runs and presents the platform (the candidate).
- **Data scientist** — a user who adds features, runs experiments and reads results through the platform's shared code and images.
- **Fraud analyst** — the (simulated) person who works the review queue and reads Case summaries.
- **Reviewer** — an interviewer or hiring engineer who reads the repo and watches the demo.
