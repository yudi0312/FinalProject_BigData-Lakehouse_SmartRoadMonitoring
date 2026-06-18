import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC = "road_reports"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def build_event() -> dict:
    return {
        "event_id": f"report-{int(time.time())}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "report_id": "R001",
        "road_name": "Jl. Ahmad Yani",
        "district": "Wonocolo",
        "village": "Jemur Wonosari",
        "damage_type": "D40_Pothole",
        "severity_score": 100,
        "confidence": 0.89,
        "status": "Pending",
        "latitude": -7.335,
        "longitude": 112.734,
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    event = build_event()
    producer.send(TOPIC, key=event["report_id"], value=event)
    producer.flush()
    print(f"Sent event to {TOPIC}: {event['event_id']}")


if __name__ == "__main__":
    main()
