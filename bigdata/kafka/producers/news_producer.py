import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC = "news_data"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def build_event() -> dict:
    return {
        "event_id": f"news-{int(time.time())}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "sample_news_feed",
        "city": "Surabaya",
        "district": "Wonocolo",
        "title": "Warga Melaporkan Kerusakan Jalan di Koridor Selatan",
        "sentiment_score": -0.34,
        "keyword": "jalan rusak",
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    event = build_event()
    producer.send(TOPIC, key=event["district"], value=event)
    producer.flush()
    print(f"Sent event to {TOPIC}: {event['event_id']}")


if __name__ == "__main__":
    main()
