
import asyncio
import json
import logging
import os
import random
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from websockets.asyncio.client import connect

from app.weather import fetch_weather


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vessel-events"

WEBSOCKET_URL = "wss://stream.aisstream.io/v0/stream"

NORTH_SEA_BOUNDING_BOX = [
    [
        [61.0, -4.0],
        [51.0, 9.0],
    ]
]

WEATHER_REFRESH_SECONDS = 15 * 60
WEATHER_MAX_AGE_SECONDS = 30 * 60

MAX_RECONNECT_DELAY = 60


# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("maritime-ingestion")


# --------------------------------------------------
# Weather cache
# --------------------------------------------------

weather_cache = None


async def refresh_weather():
    """Refresh the cached forecast periodically."""

    global weather_cache

    while True:
        try:
            logger.info("Fetching latest Open-Meteo forecast...")

            forecast = await asyncio.to_thread(fetch_weather)

            weather_cache = forecast

            logger.info(
                "Weather forecast updated. Valid time: %s",
                forecast["forecast_timestamp"],
            )

        except Exception as error:
            logger.error(
                "Weather refresh failed: %s: %s",
                type(error).__name__,
                error,
            )

        await asyncio.sleep(WEATHER_REFRESH_SECONDS)



def get_fresh_weather():
    """Return cached weather only if it is fresh and still valid."""

    if weather_cache is None:
        return None

    now = datetime.now(timezone.utc)

    retrieved_at = datetime.fromisoformat(
        weather_cache["retrieved_at"]
    )

    forecast_timestamp = datetime.fromisoformat(
        weather_cache["forecast_timestamp"]
    )

    retrieval_age = (now - retrieved_at).total_seconds()

    if retrieval_age > WEATHER_MAX_AGE_SECONDS:
        logger.warning("Cached weather forecast is stale.")
        return None

    if forecast_timestamp <= now:
        logger.warning(
            "Cached forecast validity has expired. "
            "Waiting for the next weather refresh."
        )
        return None

    return weather_cache



# --------------------------------------------------
# AIS validation and event transformation
# --------------------------------------------------

def is_valid_number(value):
    """Check that a value is numeric and not a boolean."""

    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
    )


def build_vessel_event(message):
    """Validate an AIS position report and build a Kafka event."""

    if message.get("MessageType") != "PositionReport":
        return None

    metadata = message.get("MetaData") or {}
    ais_message = (
        message.get("Message", {}).get("PositionReport") or {}
    )

    mmsi = metadata.get("MMSI") or ais_message.get("UserID")

    latitude = metadata.get("Latitude")
    longitude = metadata.get("Longitude")

    # Fall back to coordinates inside the AIS payload
    if latitude is None:
        latitude = ais_message.get("Latitude")

    if longitude is None:
        longitude = ais_message.get("Longitude")

    if mmsi is None:
        logger.warning("Discarding AIS event: MMSI is missing.")
        return None

    try:
        mmsi = str(int(mmsi))
    except (TypeError, ValueError):
        logger.warning("Discarding AIS event: invalid MMSI.")
        return None

    if not is_valid_number(latitude) or not is_valid_number(longitude):
        logger.warning(
            "Discarding AIS event for MMSI %s: coordinates missing.",
            mmsi,
        )
        return None

    latitude = float(latitude)
    longitude = float(longitude)

    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        logger.warning(
            "Discarding AIS event for MMSI %s: coordinates out of range.",
            mmsi,
        )
        return None

    # AISStream metadata time_utc is preserved as received.
    # PositionReport.Timestamp is only the AIS UTC second field,
    # not a complete date/time.
    source_timestamp = metadata.get("time_utc")

    ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    event = {
        "event_id": str(uuid.uuid4()),
        "mmsi": mmsi,
        "vessel_name": metadata.get("ShipName"),
        "latitude": latitude,
        "longitude": longitude,
        "speed_over_ground": ais_message.get("Sog"),
        "course_over_ground": ais_message.get("Cog"),
        "heading": ais_message.get("TrueHeading"),
        "event_timestamp": source_timestamp,
        "ais_timestamp_second": ais_message.get("Timestamp"),
        "ingestion_timestamp": ingestion_timestamp,
        "weather": get_fresh_weather(),
    }

    return event


# --------------------------------------------------
# Kafka producer
# --------------------------------------------------

def create_kafka_producer():
    """Create a Kafka producer with JSON serialization."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
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


# --------------------------------------------------
# AISStream connection
# --------------------------------------------------

async def consume_ais(producer):
    """Connect to AISStream and continuously publish vessel events."""

    subscription = {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": NORTH_SEA_BOUNDING_BOX,
        "FilterMessageTypes": ["PositionReport"],
    }

    reconnect_attempt = 0

    while True:
        try:
            logger.info("Connecting to AISStream...")

            async with connect(
                WEBSOCKET_URL,
                compression="deflate",
                open_timeout=15,
                ping_interval=20,
                ping_timeout=20,
            ) as websocket:

                await websocket.send(json.dumps(subscription))

                logger.info("AISStream subscription sent.")

                reconnect_attempt = 0

                async for raw_message in websocket:

                    if isinstance(raw_message, bytes):
                        raw_message = raw_message.decode("utf-8")

                    message = json.loads(raw_message)

                    message_type = message.get("MessageType")

                    if message_type == "SubscriptionConfirmation":
                        compression = message.get(
                            "Message", {}
                        ).get("CompressionEnabled")

                        logger.info(
                            "Subscription confirmed. Compression: %s",
                            compression,
                        )
                        continue

                    if message_type != "PositionReport":
                        continue

                    event = build_vessel_event(message)

                    if event is None:
                        continue

                    try:
                        await asyncio.to_thread(
                            publish_event,
                            producer,
                            event,
                        )
                    except Exception:
                        logger.exception(
                            "Kafka publish failed for MMSI %s.",
                            event["mmsi"],
                        )
                        raise
                raise ConnectionError("AISStream WebSocket closed.")

        except asyncio.CancelledError:
            raise

        except Exception as error:
            reconnect_attempt += 1

            backoff = min(
                2 ** min(reconnect_attempt, 6),
                MAX_RECONNECT_DELAY,
            )

            delay = random.uniform(backoff * 0.5, backoff)

            logger.error(
                "AIS connection or publishing failed: %s: %s",
                type(error).__name__,
                error,
            )

            logger.warning(
                "Reconnecting in %.1f seconds (attempt %s).",
                delay,
                reconnect_attempt,
            )

            await asyncio.sleep(delay)


# --------------------------------------------------
# Application entry point
# --------------------------------------------------

async def main():
    if not AISSTREAM_API_KEY:
        raise RuntimeError(
            "AISSTREAM_API_KEY is missing from the root .env file."
        )

    logger.info("Starting maritime ingestion service.")

    producer = create_kafka_producer()

    weather_task = asyncio.create_task(refresh_weather())

    try:
        await consume_ais(producer)

    finally:
        weather_task.cancel()

        try:
            await weather_task
        except asyncio.CancelledError:
            pass

        producer.flush()
        producer.close()

        logger.info("Ingestion service stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
