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
