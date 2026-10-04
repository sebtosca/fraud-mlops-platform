# ADR-0002: One pure per-card state function for training and serving; no external feature store

Status: accepted (2026-10-02, discovery Q15)

## Context
Velocity and history features (e.g. transactions per card in the last hour, amount vs card's running mean) need per-card state at serving time. Computing them one way in training (pandas over history) and another way in serving is the classic source of train/serve skew. Options: an online store (Redis / Feast), recomputing from Postgres per event, or a shared in-process state function.

## Decision
`features/` exposes a pure function that takes a card's current state and a Transaction and returns the updated state plus the feature vector. Training builds its dataset by replaying history through this function in event-time order. Serving holds the same state in memory; Kafka is partitioned by card so each card's events reach one consumer in order; state is snapshotted to Postgres for restart.

## Consequences
- Train/serve parity is structural and testable with a single parity test.
- One fewer service than Redis/Feast; fits the 2-day timebox.
- Scaling out consumers requires partition-aware state ownership; a crash loses state since the last snapshot. Acceptable for a demo; the README notes a real deployment would use a managed online store.
- Building training data is a sequential replay, slower than vectorised pandas; ~1.8M rows is fine.

## Amendment (2026-10-04, discovery Q44)
Research (RESEARCH.md §02) showed the "crash loses state since the last snapshot" consequence is avoidable: each micro-batch writes prediction rows, dirty card states and the next Kafka offsets in **one Postgres transaction**; on partition assignment the consumer restores state and seeks to the stored offsets. Effects into Postgres are exactly-once and no state is lost on crash. Producers must use `partitioner=murmur2_random` and the partition count of `transactions` is fixed. Categoricals produced by the function use stable integer codes from a frozen vocabulary (RESEARCH.md §03).
