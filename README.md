# Fraud ML Platform

A local MLOps platform for real-time card-fraud detection. Sparkov 2020 transactions are replayed
through Kafka on a simulated clock and scored by a LightGBM Champion. Labels arrive late, and
injected Shifts trigger drift alerts, retraining, Shadow evaluation and alias-based promotion.

Work in progress.

## Quickstart (dev)

```sh
uv sync
uv run pre-commit install
make test
make lint
```
