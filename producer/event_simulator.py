"""
Phase 1 - Careem-style ride event simulator.

Generates realistic ride-sharing events for Karachi and pushes them into a
real Kafka topic. Each ride moves through a lifecycle:

    ride_requested -> ride_accepted -> ride_started -> ride_ended -> payment_completed

The producer is keyed by ride_id so all events for one ride land on the same
Kafka partition (preserves ordering downstream in Spark).

Run a Kafka broker first (see docker-compose.yml), then:

    python event_simulator.py
"""

import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# --------------------------------------------------------------------------- #
# Configuration (override with environment variables)
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ride-events")
EVENTS_PER_MIN = int(os.getenv("EVENTS_PER_MIN", "30"))   # new rides started per minute
MAX_RIDES = int(os.getenv("MAX_RIDES", "0"))              # 0 = run forever

# Karachi bounding box (lat/lon) for plausible pickup / dropoff points
KARACHI_BBOX = {
    "lat_min": 24.78,
    "lat_max": 25.05,
    "lon_min": 66.95,
    "lon_max": 67.20,
}
CITY = "Karachi"

# Reusable pools so riders/drivers repeat across rides (realistic for joins later)
RIDER_IDS = [f"rider_{i:04d}" for i in range(1, 501)]
DRIVER_IDS = [f"driver_{i:04d}" for i in range(1, 201)]

VEHICLE_TYPES = ["go", "go_plus", "business", "bike"]
FARE_PER_TYPE = {        # base fare, per-minute rate (PKR)
    "bike": (60, 8),
    "go": (120, 14),
    "go_plus": (160, 18),
    "business": (250, 28),
}
PAYMENT_METHODS = ["cash", "card", "wallet"]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def now_iso() -> str:
    """Current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def random_point() -> dict:
    """Random lat/lon inside the Karachi bounding box."""
    return {
        "lat": round(random.uniform(KARACHI_BBOX["lat_min"], KARACHI_BBOX["lat_max"]), 6),
        "lon": round(random.uniform(KARACHI_BBOX["lon_min"], KARACHI_BBOX["lon_max"]), 6),
    }


def base_event(ride_id: str, event_type: str, ctx: dict) -> dict:
    """Common envelope shared by every event for a ride."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": now_iso(),
        "ride_id": ride_id,
        "city": CITY,
        "rider_id": ctx["rider_id"],
        "driver_id": ctx["driver_id"],
        "vehicle_type": ctx["vehicle_type"],
    }


def make_ride_context() -> dict:
    """Fixed attributes that stay constant for the whole ride lifecycle."""
    return {
        "ride_id": str(uuid.uuid4()),
        "rider_id": random.choice(RIDER_IDS),
        "driver_id": random.choice(DRIVER_IDS),
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "pickup": random_point(),
        "dropoff": random_point(),
    }


def build_lifecycle(ctx: dict) -> list:
    """
    Build the ordered list of events for a single ride.

    Returns a list of (event_dict, delay_seconds_before_emitting) tuples.
    A small fraction of rides are cancelled to create 'invalid'/incomplete
    data that Phase 2 (Spark) will have to filter.
    """
    ride_id = ctx["ride_id"]
    events = []

    # 1. Ride requested
    events.append((
        {**base_event(ride_id, "ride_requested", ctx),
         "pickup": ctx["pickup"],
         "dropoff": ctx["dropoff"]},
        0,
    ))

    # ~8% of requests never get accepted (rider cancels) -> incomplete ride
    if random.random() < 0.08:
        events.append((
            {**base_event(ride_id, "ride_cancelled", ctx),
             "cancelled_by": "rider",
             "reason": "no_driver_found"},
            random.uniform(1, 3),
        ))
        return events

    # 2. Driver accepts
    events.append((base_event(ride_id, "ride_accepted", ctx), random.uniform(1, 4)))

    # 3. Ride starts
    events.append((base_event(ride_id, "ride_started", ctx), random.uniform(2, 6)))

    # 4. Ride ends -> compute duration + distance + fare
    duration_min = round(random.uniform(5, 45), 1)
    distance_km = round(duration_min * random.uniform(0.3, 0.8), 2)
    base_fare, per_min = FARE_PER_TYPE[ctx["vehicle_type"]]
    fare = round(base_fare + per_min * duration_min, 2)

    end_event = base_event(ride_id, "ride_ended", ctx)
    end_event.update({
        "duration_min": duration_min,
        "distance_km": distance_km,
        "fare_amount": fare,
        "currency": "PKR",
    })
    events.append((end_event, random.uniform(3, 8)))

    # 5. Payment completed
    pay_event = base_event(ride_id, "payment_completed", ctx)
    pay_event.update({
        "amount": fare,
        "currency": "PKR",
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": "success",
    })
    events.append((pay_event, random.uniform(1, 3)))

    return events


# --------------------------------------------------------------------------- #
# Kafka producer
# --------------------------------------------------------------------------- #
def build_producer() -> KafkaProducer:
    """Create a KafkaProducer that serializes dicts to JSON bytes."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",            # wait for full commit (durability over speed)
        retries=3,
        linger_ms=50,
    )


def send(producer: KafkaProducer, event: dict) -> None:
    """Send one event, keyed by ride_id, and log it to the console."""
    future = producer.send(KAFKA_TOPIC, key=event["ride_id"], value=event)
    try:
        future.get(timeout=10)  # surface broker errors early
    except KafkaError as exc:
        print(f"[ERROR] failed to send {event['event_type']}: {exc}")
        return
    print(f"[{event['event_time']}] {event['event_type']:<18} "
          f"ride={event['ride_id'][:8]} "
          f"amount={event.get('amount', event.get('fare_amount', '-'))}")


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #
_running = True


def _stop(signum, frame):
    global _running
    _running = False
    print("\n[INFO] shutting down...")


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer = build_producer()
    print(f"[INFO] producing to topic '{KAFKA_TOPIC}' on {KAFKA_BOOTSTRAP}")
    print(f"[INFO] ~{EVENTS_PER_MIN} new rides/min "
          f"({'unlimited' if MAX_RIDES == 0 else MAX_RIDES} total)")

    interval = 60.0 / max(EVENTS_PER_MIN, 1)  # seconds between new rides
    rides_started = 0

    while _running:
        ctx = make_ride_context()
        for event, delay in build_lifecycle(ctx):
            if not _running:
                break
            if delay:
                time.sleep(delay)
            send(producer, event)

        rides_started += 1
        if MAX_RIDES and rides_started >= MAX_RIDES:
            break

        time.sleep(max(interval - 1, 0.5))  # pace new rides

    producer.flush()
    producer.close()
    print(f"[INFO] done. rides started: {rides_started}")


if __name__ == "__main__":
    main()
