# StayStream

> Real-time hotel booking events pipeline · Apache Kafka → PySpark Structured Streaming → PostgreSQL

[![CI](https://github.com/ayushsingh1524/StayStream/actions/workflows/ci.yml/badge.svg)](https://github.com/ayushsingh1524/StayStream/actions/workflows/ci.yml)

---

## Architecture

```
 StayScout API ──┐
                 ├──▶  Kafka topic: booking-events  ──▶  PySpark Structured Streaming
 Event generator ┘        (key = city, 3 partitions)       │
                                                           ├─ parse & validate
                                                           ├─ deduplicate (event_id)
                                                           ├─ watermark (2 min)
                                                           ├─ tumbling window (1 min / city)
                                                           ├─ anomaly detection
                                                           │
                                                           ├──▶ booking_metrics_per_minute  (Postgres Upsert)
                                                           ├──▶ booking_events_dlq          (Postgres DLQ)
                                                           └──▶ booking_alerts              (Postgres Alerts)
                                                                       │
                                                                       ▼
                                                            FastAPI Live Dashboard (:8000)
                                                            Prometheus /metrics → Grafana (:3000)
```

---

## Features

| Concern | Implementation | Verified Behavior |
|---|---|---|
| **Partitioning & Ordering** | Events keyed by `city` | Order guaranteed per city partition across 3 partitions |
| **Multi-Event State** | `BOOKING_CREATED` & `BOOKING_CANCELLED` | Tracks bookings, cancellations, and net revenue per city/minute |
| **Deduplication** | `dropDuplicates(["event_id"])` within the watermark window | Injected 20% duplicate events; duplicate IDs counted only once |
| **Late Arrival & Watermarking** | `withWatermark("event_time", "2 minutes")` | Events delayed up to 2 minutes correctly aggregate into historical tumbling windows |
| **Malformed Data (DLQ)** | Two independent streaming pipelines; classified reasons | Routes `invalid_json`, `missing_required_field`, and `non_positive_price` to `booking_events_dlq` |
| **Anomaly Detection** | Per-batch cancel-rate threshold with cooldown | Alerts when any city's cancellation rate exceeds 25% in a micro-batch window |
| **Idempotent Writes** | `INSERT … ON CONFLICT (window_start, city) DO UPDATE` | Replaying batches overwrites state idempotently without double-counting revenue |
| **Observability** | Prometheus `/metrics` → Grafana dashboard + FastAPI live UI | 8 Prometheus metrics, pre-built Grafana dashboard, real-time Chart.js UI |
| **Secure Config** | `.env` + `python-dotenv`; `.env.example` committed | No hardcoded credentials; all tunables via environment variables |
| **Crash Recovery** | Checkpoint location `./checkpoints/metrics` | Pipeline stops & resumes from exact committed Kafka offset; aggregates remain identical |

---

## Event Contract

Events are emitted in JSON format over the Kafka topic `booking-events`:

```json
{
  "event_id":    "b3d1f0a2-9bc5-40b8-9363-2287f61c3671",
  "event_type":  "BOOKING_CREATED",
  "booking_id":  1042,
  "hotel_id":    77,
  "city":        "Goa",
  "room_type":   "DELUXE",
  "nights":      3,
  "total_price": 15000.00,
  "user_id":     "u-991204",
  "event_time":  "2026-11-27T10:15:30Z",
  "produced_at": "2026-11-27T10:15:31Z"
}
```

---

## Performance Benchmarks

Benchmarked locally on Apple Silicon (M-series, local Kafka + PySpark 3.5.3 + PostgreSQL):

| Generator Target (msg/s) | Producer Sustained Rate | PySpark `processedRowsPerSecond` | Batch Duration | Pipeline Status |
|:---:|:---:|:---:|:---:|:---:|
| **200** | ~145 msg/s | **210 – 246 rows/s** | ~1.2s | Zero lag, sub-second latency |
| **500** | ~330 msg/s | **1,195 – 1,220 rows/s** | ~0.8s | Steady state, catches up instantaneously |
| **1,000** | ~681 msg/s | **4,815+ rows/s** | ~0.7s | Peak throughput, handles burst effortlessly |

Over **17,600+ booking events** processed with zero data loss and exact aggregate parity in PostgreSQL.

---

## Tech Stack

| Layer | Choice | Rationale |
|---|---|---|
| **Broker** | Apache Kafka (KRaft mode) | High-throughput distributed log, partitioned by `city` |
| **Stream Processing** | PySpark 3.5.3 (Structured Streaming) | Micro-batching, event-time windowing, watermarks & dedup |
| **Storage & Sinks** | PostgreSQL 16 / 14 | Relational aggregate storage with composite primary key upserts |
| **Monitoring** | Prometheus + Grafana | Industry-standard metrics scraping with pre-built dashboards |
| **Language & Environment** | Python 3.9+ / 3.11 + Java 21 | Modern runtime with automated JVM reflection exports |
| **Testing & CI** | pytest (28 tests) + Ruff + GitHub Actions | Automated unit, streaming, API, and anomaly detection tests |
| **Dashboard & Metrics** | FastAPI + Chart.js + Prometheus `/metrics` | Real-time web UI on `:8000` with live polling |

---

## Project Structure

```
StayStream/
├── .github/workflows/ci.yml        # GitHub Actions CI (lint + test)
├── .env.example                     # Environment variable template (copy to .env)
├── Dockerfile                       # Container image for the pipeline
├── docker-compose.yml               # Kafka + Postgres + Spark + Dashboard + Prometheus + Grafana
├── Makefile                         # Convenience targets (make job, make dashboard, etc.)
├── requirements.txt
├── sql/init.sql                     # DDL schema (metrics, DLQ, alerts tables)
│
├── src/
│   ├── common/config.py             # .env-aware config with python-dotenv
│   ├── producer/generate_events.py  # Fake event producer with --rate, --dup-rate, --cancel-rate, etc.
│   ├── streaming/
│   │   ├── schema.py                # Spark StructType for booking event JSON
│   │   ├── transforms.py            # parse_events() + aggregate_per_minute()
│   │   ├── sinks.py                 # Idempotent Postgres upsert & DLQ writer
│   │   ├── alerts.py                # Real-time anomaly detection (cancel rate spikes)
│   │   └── job.py                   # Main streaming entry point (metrics + DLQ + alerts)
│   └── dashboard/
│       ├── app.py                   # FastAPI application factory
│       ├── db.py                    # DB query helpers & Spark progress reader
│       ├── routes/
│       │   ├── api.py               # /api/summary, /api/cities, /api/timeseries, /api/dlq, /api/alerts
│       │   ├── metrics.py           # /metrics (Prometheus exposition format)
│       │   └── pages.py             # HTML page routes (/, /cities, /dlq)
│       └── static/
│           ├── index.html           # Overview dashboard (KPIs + alerts banner)
│           ├── cities.html          # City analytics page
│           ├── dlq.html             # DLQ inspector with reason filter
│           └── js/                  # Modular JS (common.js, overview.js, cities.js, dlq.js)
│
├── monitoring/
│   ├── prometheus.yml               # Prometheus scrape config for /metrics
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/         # Auto-configure Prometheus datasource
│       │   └── dashboards/          # Auto-load dashboard JSON
│       └── dashboards/
│           └── staystream.json      # Pre-built Grafana dashboard (8 panels)
│
└── tests/
    ├── test_transforms.py           # 15 unit tests (parse, aggregate, dedup, cancel, DLQ)
    ├── test_pipeline_local.py       # 3 integration tests (batch mode)
    ├── test_dashboard.py            # 5 API & page tests (mocked DB, incl. alerts API)
    └── test_alerts.py               # 5 anomaly detection tests (threshold, cooldown, multi-city)
```

---

## Getting Started

### Option A — Docker Compose (recommended)

```bash
git clone https://github.com/ayushsingh1524/StayStream.git
cd StayStream

# Configure environment (edit .env to change defaults)
cp .env.example .env

# Start everything: Kafka, Postgres, Spark, Dashboard, Prometheus, Grafana
docker compose up -d

# Wait ~20s for services to initialise, then produce events
pip install confluent-kafka faker
python -m src.producer.generate_events --rate 20 --duration 60

# Open the dashboards
open http://localhost:8000       # StayStream Live Dashboard
open http://localhost:3000       # Grafana (admin / staystream)
open http://localhost:9090       # Prometheus
```

### Option B — Local Setup

#### 1 · Prerequisites

- **Python 3.9+** — create a virtualenv: `python3 -m venv .venv && source .venv/bin/activate`
- **Java 21** — required by PySpark 3.5.x
  - macOS: `brew install openjdk@21` then `export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home`
  - Linux: `sudo apt install openjdk-21-jdk`
- **Apache Kafka** running on `localhost:9092`
- **PostgreSQL** running on `localhost:5432`

#### 2 · Configure Environment

```bash
cp .env.example .env
# Edit .env to set your Postgres password, Kafka host, alert thresholds, etc.
```

#### 3 · Install Dependencies

```bash
pip install -r requirements.txt
```

#### 4 · Create Kafka Topic & Database Schema

```bash
kafka-topics.sh --create --topic booking-events \
  --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

psql -h localhost -p 5432 -U stream -d staystream -f sql/init.sql
```

#### 5 · Run Lint & Tests

```bash
ruff check src tests
pytest -q --tb=short
# 28 passed
```

#### 6 · Start the Streaming Job

```bash
python -m src.streaming.job
```

#### 7 · Start the Dashboard (new terminal)

```bash
uvicorn src.dashboard.app:app --host 0.0.0.0 --port 8000 --reload
# Overview:   http://localhost:8000
# Cities:     http://localhost:8000/cities
# DLQ:        http://localhost:8000/dlq
# Alerts API: http://localhost:8000/api/alerts
# Prometheus: http://localhost:8000/metrics
```

#### 8 · Produce Events (new terminal)

```bash
# Normal traffic
python -m src.producer.generate_events --rate 20 --duration 60

# Stress test with high cancellation rate to trigger anomaly alerts
python -m src.producer.generate_events \
  --rate 100 --duration 30 \
  --dup-rate 0.10 \
  --late-rate 0.10 \
  --cancel-rate 0.30 \
  --bad-rate 0.05
```

#### 9 · Query Results

```bash
# Metrics
psql -h localhost -p 5432 -U stream -d staystream -c \
  "SELECT city, bookings, cancellations, revenue, window_start
   FROM booking_metrics_per_minute ORDER BY window_start DESC LIMIT 10;"

# Dead-Letter Queue
psql -h localhost -p 5432 -U stream -d staystream -c \
  "SELECT id, reason, received_at FROM booking_events_dlq ORDER BY id DESC LIMIT 5;"

# Anomaly Alerts
psql -h localhost -p 5432 -U stream -d staystream -c \
  "SELECT city, alert_type, details, created_at FROM booking_alerts ORDER BY created_at DESC LIMIT 5;"
```

---

## Anomaly Detection

The streaming job runs real-time anomaly detection on every micro-batch:

| Alert Type | Trigger | Cooldown |
|---|---|---|
| `high_cancellation_rate` | City cancel rate ≥ 25% in a tumbling window | 5 min per city (configurable) |

Alerts are written to `booking_alerts` in Postgres, exposed via `/api/alerts`, and displayed as a live banner on the overview dashboard. The threshold and cooldown are configurable via `.env`:

```env
ALERT_CANCEL_RATE_THRESHOLD=0.25
ALERT_COOLDOWN_MINUTES=5
```

---

## Monitoring with Grafana

The `docker-compose.yml` includes a full observability stack:

| Service | URL | Credentials |
|---|---|---|
| **Grafana** | http://localhost:3000 | `admin` / `staystream` |
| **Prometheus** | http://localhost:9090 | — |
| **StayStream Dashboard** | http://localhost:8000 | — |

The pre-built Grafana dashboard (`monitoring/grafana/dashboards/staystream.json`) includes 8 panels:
- Total Bookings, Cancellations, Revenue, DLQ counters
- Spark throughput (input vs processed rows/sec)
- Micro-batch duration timeline
- End-to-end latency timeline
- Active anomaly alerts gauge

---

## License

This project is for educational and portfolio purposes.
