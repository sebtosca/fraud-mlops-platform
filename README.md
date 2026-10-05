# Fraud ML Platform

A local MLOps platform for real-time card-fraud detection. Sparkov 2020 transactions are replayed
through Kafka on a simulated clock and scored by a LightGBM Champion. Labels arrive late, and
injected Shifts trigger drift alerts, retraining, Shadow evaluation and alias-based promotion.

Work in progress. See:

- [DISCOVERY.md](DISCOVERY.md): requirements and decisions
- [RESEARCH.md](RESEARCH.md): research findings
- [PLAN.md](PLAN.md): build plan, phases and tasks
- [ARCHITECTURE.md](ARCHITECTURE.md): how the system fits together
- [CONTEXT.md](CONTEXT.md): glossary

## Quickstart (dev)

```sh
uv sync
uv run pre-commit install
make test
make lint
```
