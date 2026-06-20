import os
import json
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_WEATHER", "weather_data")


def produce_weather(records: list[dict]) -> dict:
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )

        sent = 0
        for rec in records:
            producer.send(KAFKA_TOPIC, key=rec["date"], value=rec)
            sent += 1

        producer.flush()
        producer.close()
        print(f"[kafka] Sent {sent} records ke topic {KAFKA_TOPIC}")
        return {"sent": sent}

    except Exception as e:
        print(f"[kafka] Error: {e}")
        return {"sent": 0, "error": str(e)}