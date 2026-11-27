"""
generate_events.py – Fake booking-event producer for StayStream.

Usage:
    python -m src.producer.generate_events [OPTIONS]

Options:
    --rate INT        Events per second (default: 10)
    --duration INT    Total seconds to run; 0 = run forever (default: 60)
    --dup-rate FLOAT  Fraction of events to resend as duplicates (default: 0.0)
    --late-rate FLOAT Fraction of events with event_time 1-3 min in the past (default: 0.0)
    --bad-rate FLOAT  Fraction of events replaced with malformed JSON (default: 0.0)
    --topic TEXT      Kafka topic (default: booking-events)
    --bootstrap TEXT  Kafka bootstrap server (default: localhost:9092)
"""
import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from faker import Faker

fake = Faker()

CITIES     = ["Goa", "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Jaipur", "Kolkata"]
ROOM_TYPES = ["STANDARD", "DELUXE", "SUITE", "ECONOMY"]
PRICE_RANGE = {
    "ECONOMY":  (1500, 3000),
    "STANDARD": (3000, 7000),
    "DELUXE":   (7000, 15000),
    "SUITE":    (15000, 40000),
}


def make_event(late: bool = False, cancel: bool = False) -> dict:
    room_type   = random.choice(ROOM_TYPES)
    nights      = random.randint(1, 7)
    price_night = random.randint(*PRICE_RANGE[room_type])
    total_price = round(price_night * nights, 2)

    if late:
        offset_seconds = random.randint(60, 180)   # 1–3 minutes late
        event_time = datetime.now(timezone.utc) - timedelta(seconds=offset_seconds)
    else:
        event_time = datetime.now(timezone.utc)

    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "BOOKING_CANCELLED" if cancel else "BOOKING_CREATED",
        "booking_id":  random.randint(1000, 9_999_999),
        "hotel_id":    random.randint(1, 500),
        "city":        random.choice(CITIES),
        "room_type":   room_type,
        "nights":      nights,
        "total_price": total_price,
        "user_id":     str(fake.uuid4()),
        "event_time":  event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "produced_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] delivery failed: {err}", file=sys.stderr)


def run(args: argparse.Namespace) -> None:
    producer = Producer({"bootstrap.servers": args.bootstrap})

    start      = time.monotonic()
    total_sent = 0
    duplicate_pool: list[tuple[str, bytes]] = []   # (city, payload)
    sleep_per_event = 1.0 / args.rate

    print(f"[StayStream producer] rate={args.rate}/s  duration={args.duration}s  "
          f"dup={args.dup_rate:.0%}  late={args.late_rate:.0%}  "
          f"bad={args.bad_rate:.0%}  cancel={args.cancel_rate:.0%}")

    try:
        while True:
            if args.duration > 0 and (time.monotonic() - start) >= args.duration:
                break

            roll = random.random()

            # ── Bad event ─────────────────────────────────────────────────────
            if roll < args.bad_rate:
                payload = b'{"event_id": null, "BROKEN": true, bad json'
                city    = random.choice(CITIES).encode()
                producer.produce(args.topic, value=payload, key=city,
                                 on_delivery=delivery_report)

            # ── Duplicate ─────────────────────────────────────────────────────
            elif roll < args.bad_rate + args.dup_rate and duplicate_pool:
                city, payload = random.choice(duplicate_pool)
                producer.produce(args.topic, value=payload, key=city.encode(),
                                 on_delivery=delivery_report)

            # ── Normal (or late / cancel) event ───────────────────────────────
            else:
                is_late   = random.random() < args.late_rate
                is_cancel = random.random() < args.cancel_rate
                event     = make_event(late=is_late, cancel=is_cancel)
                payload = json.dumps(event).encode()
                city    = event["city"]
                producer.produce(args.topic, value=payload, key=city.encode(),
                                 on_delivery=delivery_report)

                # Keep a small pool for duplicates
                if len(duplicate_pool) < 500:
                    duplicate_pool.append((city, payload))

            total_sent += 1
            producer.poll(0)
            time.sleep(sleep_per_event)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        elapsed = time.monotonic() - start
        print(f"[StayStream producer] sent {total_sent} messages in {elapsed:.1f}s "
              f"({total_sent / elapsed:.1f} msg/s)")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="StayStream fake booking event producer")
    p.add_argument("--rate",      type=int,   default=10,         help="events/sec")
    p.add_argument("--duration",  type=int,   default=60,         help="seconds (0=forever)")
    p.add_argument("--dup-rate",  type=float, default=0.0,        dest="dup_rate")
    p.add_argument("--late-rate", type=float, default=0.0,        dest="late_rate")
    p.add_argument("--bad-rate",    type=float, default=0.0,        dest="bad_rate")
    p.add_argument("--cancel-rate", type=float, default=0.0,        dest="cancel_rate")
    p.add_argument("--topic",     type=str,   default="booking-events")
    p.add_argument("--bootstrap", type=str,   default="localhost:9092")
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())

