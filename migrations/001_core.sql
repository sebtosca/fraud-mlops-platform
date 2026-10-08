-- 001_core: the platform's core schema (PLAN P1.T4).
-- Applied by `fraud db migrate` inside one transaction; never edit after it is applied,
-- add a new numbered file instead. Times are timestamptz in UTC; card numbers appear only
-- as `card_hash`.

-- ---------------------------------------------------------------------------
-- Scoring
-- ---------------------------------------------------------------------------

-- One row per Transaction per model role: what the platform scored and decided.
-- Written in the same transaction as card_state and consumer_offsets (exactly-once).
CREATE TABLE predictions (
    txn_id            text             NOT NULL,
    card_hash         text             NOT NULL,
    event_time        timestamptz      NOT NULL,
    role              text             NOT NULL CHECK (role IN ('champion', 'challenger')),
    model_version     text             NOT NULL,
    score             double precision NOT NULL CHECK (score BETWEEN 0 AND 1),
    decision          text             NOT NULL CHECK (decision IN ('block', 'review', 'allow')),
    would_be_decision text             NOT NULL
                                       CHECK (would_be_decision IN ('block', 'review', 'allow')),
    explored          boolean          NOT NULL DEFAULT false,
    propensity        double precision NOT NULL DEFAULT 1
                                       CHECK (propensity > 0 AND propensity <= 1),
    applied           boolean          NOT NULL,
    champion_version  text,
    champion_score    double precision,
    kafka_partition   integer          NOT NULL,
    kafka_offset      bigint           NOT NULL,
    PRIMARY KEY (txn_id, role)
);

-- ---------------------------------------------------------------------------
-- State and clock
-- ---------------------------------------------------------------------------

-- Per-card feature state from update_state (ADR-0002).
CREATE TABLE card_state (
    card_hash       text        PRIMARY KEY,
    state           jsonb       NOT NULL,
    last_event_time timestamptz NOT NULL
);

-- Next Kafka offset to read per partition; the source of truth on restart.
CREATE TABLE consumer_offsets (
    topic           text    NOT NULL,
    kafka_partition integer NOT NULL CHECK (kafka_partition >= 0),
    next_offset     bigint  NOT NULL CHECK (next_offset >= 0),
    PRIMARY KEY (topic, kafka_partition)
);

-- Simulated clock watermark (single row: `id` can only be true).
CREATE TABLE sim_clock (
    id        boolean     PRIMARY KEY DEFAULT true CHECK (id),
    watermark timestamptz NOT NULL
);

-- Replay control: pause flag and live Shift scenarios (single row).
CREATE TABLE sim_control (
    id               boolean PRIMARY KEY DEFAULT true CHECK (id),
    paused           boolean NOT NULL DEFAULT false,
    active_scenarios jsonb   NOT NULL DEFAULT '[]'
);

-- Where the replayer stopped, so it can resume without duplicates (single row).
CREATE TABLE replay_progress (
    id              boolean     PRIMARY KEY DEFAULT true CHECK (id),
    last_event_time timestamptz NOT NULL,
    last_trans_num  text        NOT NULL
);

-- ---------------------------------------------------------------------------
-- Labels
-- ---------------------------------------------------------------------------

-- Released (delayed) Labels. Unexplored blocks never get a row.
CREATE TABLE labels (
    txn_id     text        PRIMARY KEY,
    label      smallint    NOT NULL CHECK (label IN (0, 1)),
    label_time timestamptz NOT NULL,
    source     text        NOT NULL CHECK (source IN ('chargeback', 'confirmed_legit', 'analyst'))
);

-- Oracle truth for every Transaction. Read only by fraud.evaluation, fraud.jobs.report
-- and the labeler; never joined into features or training.
CREATE TABLE oracle_labels (
    txn_id     text        PRIMARY KEY,
    is_fraud   smallint    NOT NULL CHECK (is_fraud IN (0, 1)),
    event_time timestamptz NOT NULL
);

-- ---------------------------------------------------------------------------
-- Shifts and monitoring
-- ---------------------------------------------------------------------------

-- One row per Shift scenario start or stop: ground truth for detection scoring.
CREATE TABLE shift_events (
    id       bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scenario text        NOT NULL,
    kind     text        NOT NULL CHECK (kind IN ('covariate', 'label', 'concept')),
    action   text        NOT NULL CHECK (action IN ('start', 'stop')),
    sim_time timestamptz NOT NULL,
    params   jsonb       NOT NULL DEFAULT '{}'
);

-- Every Transaction a Shift touched.
CREATE TABLE shift_audit (
    txn_id         text   NOT NULL,
    shift_event_id bigint NOT NULL REFERENCES shift_events (id),
    PRIMARY KEY (txn_id, shift_event_id)
);

-- One value per closed sim-day window, metric and segment ('all' or a category).
CREATE TABLE drift_metrics (
    window_start date             NOT NULL,
    metric       text             NOT NULL,
    segment      text             NOT NULL DEFAULT 'all',
    value        double precision NOT NULL,
    threshold    double precision,
    PRIMARY KEY (window_start, metric, segment)
);

-- Alerts raised by monitoring.
CREATE TABLE alerts (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    window_start date        NOT NULL,
    rule         text        NOT NULL,
    details      jsonb       NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Consecutive-breach streak per monitoring rule.
CREATE TABLE monitor_state (
    rule        text    PRIMARY KEY,
    streak      integer NOT NULL DEFAULT 0 CHECK (streak >= 0),
    last_window date
);

-- ---------------------------------------------------------------------------
-- Pipelines and models
-- ---------------------------------------------------------------------------

-- PipelineTrigger outbox. One request per (kind, sim_date): duplicates are dropped.
CREATE TABLE pipeline_requests (
    id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind       text        NOT NULL
                           CHECK (kind IN ('drift', 'retrain_due', 'sim_week_closed',
                                           'shadow_window_done')),
    sim_date   date        NOT NULL,
    payload    jsonb       NOT NULL DEFAULT '{}',
    status     text        NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending', 'running', 'done', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kind, sim_date)
);

-- Per-Account risk score every K sim days.
CREATE TABLE batch_scores (
    card_hash     text             NOT NULL,
    as_of         date             NOT NULL,
    score         double precision NOT NULL CHECK (score BETWEEN 0 AND 1),
    model_version text             NOT NULL,
    PRIMARY KEY (card_hash, as_of)
);

-- Alias history. Append-only: the trigger below rejects UPDATE and DELETE.
CREATE TABLE model_promotions (
    id               bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alias            text        NOT NULL
                                 CHECK (alias IN ('champion', 'challenger', 'previous_champion')),
    model_version    text        NOT NULL,
    previous_version text,
    reason           text        NOT NULL,
    mlflow_run_id    text,
    promoted_at      timestamptz NOT NULL DEFAULT now()
);

-- Audit trail: rows can be added, never changed or deleted (THREAT-MODEL TM-007).
CREATE FUNCTION reject_change() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER model_promotions_append_only
    BEFORE UPDATE OR DELETE ON model_promotions
    FOR EACH ROW EXECUTE FUNCTION reject_change();

-- One row per Challenger training run.
CREATE TABLE retrain_runs (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trigger_kind  text        NOT NULL,
    mode          text        NOT NULL CHECK (mode IN ('stateless', 'stateful', 'naive')),
    sim_date      date        NOT NULL,
    status        text        NOT NULL CHECK (status IN ('running', 'done', 'failed')),
    mlflow_run_id text,
    model_version text,
    metrics       jsonb       NOT NULL DEFAULT '{}',
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz
);
