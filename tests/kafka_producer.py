
import json
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "vessel-events"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# Controlled test messages.
# These are NOT genuine AIS observations.

test_messages = [
    {
        "event_id": "test-event-001",
        "mmsi": "999000001",
        "source": "kafka-connectivity-test",
        "message": "Testing Kafka producer",
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    },
    {
        "event_id": "test-event-002",
        "mmsi": "999000002",
        "source": "kafka-connectivity-test",
        "message": "Testing Kafka message delivery",
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    },
    {
        "event_id": "test-event-003",
        "mmsi": "999000003",
        "source": "kafka-connectivity-test",
        "message": "Testing Kafka partition assignment",
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }
]

try:
    for event in test_messages:
        future = producer.send(
            TOPIC_NAME,
            key=event["mmsi"],
            value=event
        )

        metadata = future.get(timeout=10)

        print("Message published successfully!")
        print(f"Event ID: {event['event_id']}")
        print(f"MMSI: {event['mmsi']}")
        print(f"Topic: {metadata.topic}")
        print(f"Partition: {metadata.partition}")
        print(f"Offset: {metadata.offset}")
        print("-" * 40)

    producer.flush()

except Exception as error:
    print(f"Kafka producer error: {error}")

finally:
    producer.close()
    print("Producer connection closed.")
