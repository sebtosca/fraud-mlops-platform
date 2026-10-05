.PHONY: help up down ingest test lint demo

help:
	@echo "Targets: up down ingest test lint demo"

up:
	@echo "TODO (P1.T3): start the Compose stack"

down:
	@echo "TODO (P1.T3): stop the Compose stack"

ingest:
	@echo "TODO (P1.T7): build bronze/silver snapshots"

test:
	uv run pytest

lint:
	uv run pre-commit run --all-files

demo:
	@echo "TODO (P5.T9): run the end-to-end demo"
