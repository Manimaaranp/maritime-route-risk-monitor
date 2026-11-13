
import json
import logging

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vessel-events"

logger = logging.getLogger(__name__)


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        enable_idempotence=True,
        retries=5,
        max_in_flight_requests_per_connection=1,
    )


def publish_event(producer, event):
    """Publish one validated vessel event to Kafka."""

    future = producer.send(
        KAFKA_TOPIC,
        key=event["mmsi"],
        value=event,
    )

    metadata = future.get(timeout=10)

    logger.info(
        "Published MMSI %s | topic=%s | partition=%s | offset=%s",
        event["mmsi"],
        metadata.topic,
        metadata.partition,
        metadata.offset,
    )
