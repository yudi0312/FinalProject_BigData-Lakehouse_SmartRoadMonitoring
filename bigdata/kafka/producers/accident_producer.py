"""
Accident Data Producer
Mengirim sample accident data ke Kafka topic 'accident_data'.
"""
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC = "accident_data"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def build_event() -> dict:
    """Build sample accident event"""
    return {
        "event_id": f"accident-{int(time.time())}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "accident_id": f"ACC{int(time.time())}",
        "road_name": "Jl. Ahmad Yani",
        "district": "Wonocolo",
        "latitude": -7.335,
        "longitude": 112.734,
        "severity": "High",
        "vehicle_count": 3,
        "casualties": 1,
    }


def main() -> None:
    """Send sample accident event to Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    
    event = build_event()
    producer.send(TOPIC, key=event["accident_id"], value=event)
    producer.flush()
    print(f"Sent event to {TOPIC}: {event['event_id']}")


if __name__ == "__main__":
    main()
