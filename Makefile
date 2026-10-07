.PHONY: help up down ps logs ingest test lint demo

COMPOSE = docker compose --env-file .env -f deploy/compose/docker-compose.yml

help:
	@echo "Targets: up down ps logs ingest test lint demo"

# Start Postgres, SeaweedFS, Kafka and MLflow; returns once all are healthy.
# `--wait` only names long-running services: a one-shot job nothing depends on (kafka-init)
# that exits while others are still starting makes `--wait` fail. It runs explicitly after.
up: .env
	$(COMPOSE) up -d --build --wait postgres s3 kafka mlflow
	$(COMPOSE) run --rm -T kafka-init

# Stop the stack. Data volumes are kept; `$(COMPOSE) down -v` deletes them.
down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps -a

logs:
	$(COMPOSE) logs -f --tail=100

.env:
	@echo "Missing .env: copy .env.example to .env and replace every change-me value." && exit 1

ingest:
	@echo "TODO (P1.T7): build bronze/silver snapshots"

test:
	uv run pytest

lint:
	uv run pre-commit run --all-files

demo:
	@echo "TODO (P5.T9): run the end-to-end demo"
