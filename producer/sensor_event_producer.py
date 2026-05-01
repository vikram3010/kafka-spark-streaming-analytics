import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


TOPIC_NAME = "sensor-events"
BOOTSTRAP_SERVERS = "localhost:9092"


def create_event(sensor_id: int) -> dict:
    return {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(60.0, 100.0), 2),
        "humidity": round(random.uniform(20.0, 80.0), 2),
        "pressure": round(random.uniform(950.0, 1050.0), 2),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Publishing events to Kafka topic: {TOPIC_NAME}")

    try:
        while True:
            event = create_event(sensor_id=random.randint(1, 5))
            producer.send(TOPIC_NAME, event)
            producer.flush()
            print(event)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
