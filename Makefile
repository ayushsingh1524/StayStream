# StayStream – convenience targets
# Assumes: .venv is active, Docker is running.

PYTHON := .venv/bin/python
PYTEST  := .venv/bin/pytest

# Java 21 (Homebrew ARM; adjust if on Intel)
JAVA21 := /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
export JAVA_HOME := $(JAVA21)

.PHONY: help up topic test lint producer job dashboard clean

help:
	@echo "make up        – start Kafka + Postgres"
	@echo "make topic     – create booking-events topic"
	@echo "make test      – run pytest suite"
	@echo "make lint      – run ruff linter"
	@echo "make producer  – run event generator (rate=10/s, 60s)"
	@echo "make job       – start the PySpark streaming job"
	@echo "make dashboard – start the live FastAPI dashboard on :8000"
	@echo "make clean     – remove checkpoints and pyc files"

up:
	docker compose up -d
	@echo "Waiting 15 s for Kafka to initialise…"
	@sleep 15

topic:
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
	  --create --if-not-exists \
	  --topic booking-events \
	  --partitions 3 --replication-factor 1 \
	  --bootstrap-server localhost:9092
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
	  --list --bootstrap-server localhost:9092

test:
	$(PYTEST) -q --tb=short

lint:
	.venv/bin/ruff check src tests

producer:
	$(PYTHON) -m src.producer.generate_events \
	  --rate $(or $(rate),10) --duration $(or $(duration),60) \
	  --dup-rate $(or $(dup),0) --late-rate $(or $(late),0) --bad-rate $(or $(bad),0)

job:
	$(PYTHON) -m src.streaming.job

dashboard:
	.venv/bin/uvicorn src.dashboard.app:app --host 0.0.0.0 --port 8000 --reload

clean:
	rm -rf checkpoints/ && find . -name "*.pyc" -delete

