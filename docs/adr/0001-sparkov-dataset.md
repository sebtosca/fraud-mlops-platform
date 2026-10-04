# ADR-0001: Use the Sparkov synthetic dataset instead of ULB creditcard.csv

Status: accepted (2026-10-02, discovery Q4)

## Context
The platform needs per-account history (velocity features, nightly per-account risk scores), a time span long enough for a time-based train/test split and for streaming replay with injected shifts, and interpretable features so concept shift can be designed and explained. The best-known public fraud dataset (ULB / Kaggle `creditcard.csv`) has anonymised PCA features V1–V28, covers only two days, and has no card or account identifier.

## Decision
Use the Sparkov synthetic credit-card dataset (Kaggle `fraudTrain.csv` / `fraudTest.csv`): ~1.8M transactions with card number, merchant, category, amount, location and timestamps over roughly two years. The user downloads it manually; the repo does not redistribute it.

## Consequences
- Per-account features and batch scoring are possible; concept shift can be expressed in real terms (category, amount, time of day).
- Data is synthetic, so absolute metrics are not comparable to real-world fraud rates; the README should say so.
- Interviewers may recognise ULB as the "standard" dataset; the README explains why it was rejected.
- Switching datasets later would invalidate the feature package, drift scenarios and benchmark numbers.
