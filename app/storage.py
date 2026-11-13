
import json
import sqlite3

from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATABASE_PATH = PROJECT_ROOT / "data" / "maritime_analytics.db"


def get_connection():
    """Create a connection to the analytical database."""

    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(
        DATABASE_PATH,
        timeout=30,
    )

    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=30000")

    return connection


def initialize_database():
    """Create the analytical vessel event table."""

    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_vessel_events (
                event_id TEXT PRIMARY KEY,
                mmsi TEXT NOT NULL,
                vessel_name TEXT,

                latitude REAL NOT NULL,
                longitude REAL NOT NULL,

                speed_over_ground REAL,
                course_over_ground REAL,
                heading INTEGER,

                event_timestamp TEXT,
                ingestion_timestamp TEXT,

                weather_cell_id TEXT,
                forecast_timestamp TEXT,
                weather_retrieved_at TEXT,

                wave_height_m REAL,
                wave_period_s REAL,
                current_velocity_ms REAL,
                current_direction_deg REAL,

                wind_speed_kmh REAL,
                wind_gusts_kmh REAL,

                kafka_topic TEXT NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset INTEGER NOT NULL,

                raw_message_json TEXT,
                processed_at TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_processed_events_mmsi
            ON processed_vessel_events(mmsi)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_processed_events_timestamp
            ON processed_vessel_events(event_timestamp)
            """
        )


def save_processed_event(event):
    """Insert a normalized vessel event without creating duplicates."""

    processed_at = datetime.now(timezone.utc).isoformat()

    raw_message = event.get("raw_message")

    if raw_message is not None:
        raw_message = json.dumps(
            raw_message,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    values = (
        event["event_id"],
        event["mmsi"],
        event.get("vessel_name"),
        event["latitude"],
        event["longitude"],
        event.get("speed_over_ground"),
        event.get("course_over_ground"),
        event.get("heading"),
        event.get("event_timestamp"),
        event.get("ingestion_timestamp"),
        event.get("weather_cell_id"),
        event.get("forecast_timestamp"),
        event.get("retrieved_at"),
        event.get("wave_height_m"),
        event.get("wave_period_s"),
        event.get("current_velocity_ms"),
        event.get("current_direction_deg"),
        event.get("wind_speed_kmh"),
        event.get("wind_gusts_kmh"),
        event["topic"],
        event["partition"],
        event["offset"],
        raw_message,
        processed_at,
    )

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO processed_vessel_events (
                event_id,
                mmsi,
                vessel_name,
                latitude,
                longitude,
                speed_over_ground,
                course_over_ground,
                heading,
                event_timestamp,
                ingestion_timestamp,
                weather_cell_id,
                forecast_timestamp,
                weather_retrieved_at,
                wave_height_m,
                wave_period_s,
                current_velocity_ms,
                current_direction_deg,
                wind_speed_kmh,
                wind_gusts_kmh,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                raw_message_json,
                processed_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            values,
        )

        return cursor.rowcount == 1
