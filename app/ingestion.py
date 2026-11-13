
import asyncio
import json
import logging
import os
import random
import time
import hashlib
import re
import math
from app.spool import initialize_spool, save_event
from datetime import datetime, timezone
from pathlib import Path
from app.kafka_client import create_kafka_producer, publish_event
from app.publisher import publish_pending_events
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
# Multi-location weather cache
# --------------------------------------------------

WEATHER_CELLS = {
    "southern": {
        "latitude": 52.5,
        "longitude": 3.5,
    },
    "central": {
        "latitude": 56.0,
        "longitude": 3.0,
    },
    "northern": {
        "latitude": 59.0,
        "longitude": 2.0,
    },
    "german_bight": {
        "latitude": 54.0,
        "longitude": 7.0,
    },
}

weather_cache = {}


async def refresh_weather():
    """Refresh each geographical weather cell independently."""

    global weather_cache

    while True:
        for cell_id, location in WEATHER_CELLS.items():
            try:
                logger.info(
                    "Fetching weather for cell: %s",
                    cell_id,
                )

                forecast = await asyncio.to_thread(
                    fetch_weather,
                    location["latitude"],
                    location["longitude"],
                )

                weather_cache[cell_id] = forecast

                logger.info(
                    "Weather updated for %s. Valid time: %s",
                    cell_id,
                    forecast["forecast_timestamp"],
                )

            except Exception as error:
                logger.error(
                    "Weather refresh failed for %s: %s: %s",
                    cell_id,
                    type(error).__name__,
                    error,
                )

        await asyncio.sleep(WEATHER_REFRESH_SECONDS)



def get_fresh_weather(latitude, longitude):
    """Return the nearest weather forecast that is currently valid."""

    now = datetime.now(timezone.utc)

    nearest_cell = None
    nearest_forecast = None
    nearest_distance = float("inf")

    for cell_id, location in WEATHER_CELLS.items():
        forecast = weather_cache.get(cell_id)

        # Skip cells without cached forecasts.
        if forecast is None:
            continue

        try:
            retrieved_at = datetime.fromisoformat(
                forecast["retrieved_at"]
            )
            forecast_timestamp = datetime.fromisoformat(
                forecast["forecast_timestamp"]
            )
            if retrieved_at.tzinfo is None or forecast_timestamp.tzinfo is None:
                logger.warning(
                    "Weather timestamps lack timezone information for cell %s.",
                    cell_id,
                )
                continue
        except (KeyError, ValueError, TypeError):
            logger.warning(
                "Invalid weather timestamps for cell %s.",
                cell_id,
            )
            continue

        # Skip forecasts with invalid or stale retrieval times.
        retrieval_age = (now - retrieved_at).total_seconds()

        if retrieval_age < 0 or retrieval_age > WEATHER_MAX_AGE_SECONDS:
            logger.warning("Weather cache is stale for cell %s.", cell_id)
            continue

        # Skip forecasts that have already expired.
        # Accept the current hourly forecast and reject forecasts
# older than one hour.
        forecast_age = (now - forecast_timestamp).total_seconds()

        if forecast_age > 3600:
            logger.warning(
                "Weather forecast is too old for cell %s.",
                cell_id,
            )
            continue

        # Do not accept forecasts more than one hour in the future.
        if forecast_age < -3600:
            logger.warning(
                "Weather forecast is too far in the future for cell %s.",
                cell_id,
            )
            continue

        lat_difference = latitude - location["latitude"]
        lon_difference = longitude - location["longitude"]

        adjusted_lon_difference = (
            lon_difference * math.cos(math.radians(latitude))
        )

        distance = (
            lat_difference ** 2
            + adjusted_lon_difference ** 2
        )

        if distance < nearest_distance:
            nearest_distance = distance
            nearest_cell = cell_id
            nearest_forecast = forecast

    if nearest_forecast is None:
        return None

    return {
        "cell_id": nearest_cell,
        **nearest_forecast,
    }





# --------------------------------------------------
# AIS validation and event transformation
# --------------------------------------------------

def is_valid_number(value):
    """Check that a value is numeric and not a boolean."""

    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
    )



def parse_source_timestamp(value):
    """Normalize an AISStream source timestamp to UTC ISO 8601."""

    if not isinstance(value, str) or not value.strip():
        return None

    try:
        normalized = value.strip()

        # Remove AISStream's trailing UTC label.
        if normalized.endswith(" UTC"):
            normalized = normalized[:-4]

        # Convert timezone offsets such as +0000 to +00:00.
        normalized = re.sub(
            r"\s([+-]\d{2})(\d{2})$",
            r"\1:\2",
            normalized,
        )

        # Python datetime supports microsecond precision.
        normalized = re.sub(
            r"([.,]\d{6})\d+",
            r"\1",
            normalized,
        )

        parsed = datetime.fromisoformat(normalized)

        if parsed.tzinfo is None:
            logger.warning(
                "AIS source timestamp has no timezone: %s",
                value,
            )
            return None

        return parsed.astimezone(timezone.utc).isoformat()

    except ValueError:
        logger.warning(
            "Invalid AIS source timestamp: %s",
            value,
        )
        return None



def build_vessel_event(message):
    """Validate an AIS position report and build a traceable Kafka event."""

    if message.get("MessageType") != "PositionReport":
        return None

    metadata = message.get("MetaData") or {}
    ais_message = (
        message.get("Message", {}).get("PositionReport") or {}
    )

    mmsi = metadata.get("MMSI") or ais_message.get("UserID")

    latitude = metadata.get("Latitude")
    longitude = metadata.get("Longitude")

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

    if not (
        -90 <= latitude <= 90
        and -180 <= longitude <= 180
    ):
        logger.warning(
            "Discarding AIS event for MMSI %s: coordinates out of range.",
            mmsi,
        )
        return None

    # Canonical representation of the original AISStream message.
    canonical_message = json.dumps(
        message,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    # Deterministic event identity.
    event_id = hashlib.sha256(
        canonical_message.encode("utf-8")
    ).hexdigest()

    source_timestamp_raw = metadata.get("time_utc")
    source_timestamp = parse_source_timestamp(source_timestamp_raw)
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    event = {
        "event_id": event_id,
        "schema_version": 1,
        "mmsi": mmsi,
        "vessel_name": metadata.get("ShipName"),
        "latitude": latitude,
        "longitude": longitude,
        "speed_over_ground": (
            ais_message.get("Sog")
             if is_valid_number(ais_message.get("Sog"))
            and 0 <= ais_message.get("Sog") <= 102.2
            else None
        ),

        "course_over_ground": (
            ais_message.get("Cog")
            if is_valid_number(ais_message.get("Cog"))
            and 0 <= ais_message.get("Cog") < 360
            else None
        ),

        "heading": (
            ais_message.get("TrueHeading")
            if isinstance(ais_message.get("TrueHeading"), int)
            and not isinstance(ais_message.get("TrueHeading"), bool)
            and 0 <= ais_message.get("TrueHeading") <= 359
            else None
        ),
        "event_timestamp": source_timestamp,
        "source_timestamp_raw": source_timestamp_raw,
        "ais_timestamp_second": ais_message.get("Timestamp"),
        "ingestion_timestamp": ingestion_timestamp,
        "raw_message": message,
        "weather": get_fresh_weather(latitude, longitude),
    }

    return event





# --------------------------------------------------
# AISStream connection
# --------------------------------------------------


async def consume_ais():
    """Receive AIS events and persist them to the local SQLite spool."""

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

                    # Persist the event before attempting Kafka delivery.
                    inserted = await asyncio.to_thread(
                        save_event,
                        event,
                    )

                    if inserted:
                        logger.info(
                            "Saved AIS event to SQLite spool | "
                            "MMSI=%s | event_id=%s",
                            event["mmsi"],
                            event["event_id"],
                        )
                    else:
                        logger.debug(
                            "Duplicate AIS event already in spool | "
                            "MMSI=%s | event_id=%s",
                            event["mmsi"],
                            event["event_id"],
                        )

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
                "AIS connection or spool persistence failed: %s: %s",
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

    initialize_spool()

    producer = create_kafka_producer()

    weather_task = asyncio.create_task(refresh_weather())

    publisher_task = asyncio.create_task(
        publish_pending_events(producer)
    )

    ingestion_task = asyncio.create_task(consume_ais())

    try:
        await asyncio.gather(
            weather_task,
            publisher_task,
            ingestion_task,
        )

    finally:
        logger.info("Shutting down maritime ingestion service.")

        tasks = [
            weather_task,
            publisher_task,
            ingestion_task,
        ]

        for task in tasks:
            task.cancel()

        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        await asyncio.to_thread(producer.flush)
        await asyncio.to_thread(producer.close)

        logger.info("Maritime ingestion service stopped.")



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
