# StayStream

> Real-time hotel booking events pipeline · Kafka → PySpark Structured Streaming → PostgreSQL

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
                                                           │
                                                           ├──▶ booking_metrics_per_minute  (Postgres)
                                                           └──▶ booking_events_dlq          (Postgres)
```

## Event contract

```json
{
  "event_id":    "b3d1f0a2-...",
  "event_type":  "BOOKING_CREATED",
  "booking_id":  1042,
  "hotel_id":    77,
  "city":        "Goa",
  "room_type":   "DELUXE",
  "nights":      3,
  "total_price": 15000.00,
  "user_id":     "hashed-or-numeric",
  "event_time":  "2026-11-22T10:15:30Z",
  "produced_at": "2026-11-22T10:15:31Z"
}
```

## Stack

| Layer | Choice |
|---|---|
| Language | Python 3.11 |
| Broker | Apache Kafka 3.8 (KRaft, Docker) |
| Producer | `confluent-kafka` |
| Processing | PySpark 3.5.3 Structured Streaming |
| Database | PostgreSQL 16 (Docker, port 5433) |
| Tests | pytest |
| CI | GitHub Actions |

## Prerequisites

- macOS, Java 17 or 21 (`java -version`), Docker Desktop running
- Python 3.11 venv

## How to run

### 1 · Start infrastructure

```bash
docker compose up -d
# Create the topic (wait ~10 s for Kafka to be ready first)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic booking-events \
  --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 2 · Install Python dependencies

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 3 · Start the Spark streaming job

```bash
python -m src.streaming.job
```

The first run downloads the Kafka connector (~30 s). After that you will see
`Streaming queries started` in the logs.

### 4 · Start the event generator (new terminal)

```bash
source .venv/bin/activate
# Normal events at 50/s for 120 s
python -m src.producer.generate_events --rate 50 --duration 120

# With duplicates, late events and malformed JSON (Phase 5-6)
python -m src.producer.generate_events \
  --rate 50 --duration 120 \
  --dup-rate 0.05 --late-rate 0.05 --bad-rate 0.02
```

### 5 · Query results

```bash
psql -h localhost -p 5433 -U stream -d staystream -c \
  "SELECT * FROM booking_metrics_per_minute ORDER BY window_start DESC LIMIT 10;"

psql -h localhost -p 5433 -U stream -d staystream -c \
  "SELECT count(*) FROM booking_events_dlq;"
```

## Run tests

```bash
pytest -q
```

## Guarantees

| Concern | Implementation |
|---|---|
| **Duplicate events** | `dropDuplicates(["event_id"])` within the watermark window |
| **Late events** | `withWatermark("event_time", "2 minutes")` — events ≤ 2 min late land in the right window |
| **Malformed JSON** | Routed to `booking_events_dlq`; job never crashes |
| **Idempotent sink** | `INSERT … ON CONFLICT DO UPDATE` — restarting the job gives the same totals |
| **Restart safety** | Spark checkpoint at `./checkpoints/metrics` — resumes from last offset |

## Throughput

| Rate (events/s) | processedRowsPerSecond | Notes |
|---|---|---|
| _run Phase 9_ | ___ | Local Docker on MacBook Air |

## Limitations & next steps

- Single-node Kafka and local Spark — not production scale
- No Schema Registry (Avro/Protobuf contract enforcement)
- Exactly-once sink would need a transactional connector or idempotent Kafka producer + Spark savepoints
- Add Grafana / Prometheus for real-time lag monitoring
- Phase 10: StayScout publishes live `BookingCreatedEvent` to the same topic

