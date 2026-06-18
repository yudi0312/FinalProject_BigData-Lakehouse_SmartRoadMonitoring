import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC = "traffic_data"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def build_event() -> dict:
    return {
        "event_id": f"traffic-{int(time.time())}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "road_name": "Jl. Ahmad Yani",
        "district": "Wonocolo",
        "average_speed_kmh": 22.4,
        "congestion_level": "high",
        "vehicle_count": 1840,
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    event = build_event()
    producer.send(TOPIC, key=event["road_name"], value=event)
    producer.flush()
    print(f"Sent event to {TOPIC}: {event['event_id']}")


if __name__ == "__main__":
    main()
