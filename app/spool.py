
import json
import sqlite3

from datetime import datetime, timezone
from pathlib import Path


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATABASE_PATH = PROJECT_ROOT / "data" / "ingestion_spool.db"


# --------------------------------------------------
# Database connection
# --------------------------------------------------

def get_connection():
    """Create a connection to the durable event spool."""

    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(
        DATABASE_PATH,
        timeout=30,
    )

    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=30000")

    return connection


# --------------------------------------------------
# Initialize spool
# --------------------------------------------------

def initialize_spool():
    """Create the event spool table if it does not exist."""

    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_events (
                event_id TEXT PRIMARY KEY,
                mmsi TEXT NOT NULL,
                event_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'published')),
                created_at TEXT NOT NULL,
                published_at TEXT
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ingestion_events_status
            ON ingestion_events(status, created_at)
            """
        )


# --------------------------------------------------
# Save incoming event
# --------------------------------------------------

def save_event(event):
    """Persist an event before attempting Kafka publication."""

    event_json = json.dumps(
        event,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    created_at = datetime.now(timezone.utc).isoformat()

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO ingestion_events (
                event_id,
                mmsi,
                event_json,
                created_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                event["event_id"],
                event["mmsi"],
                event_json,
                created_at,
            ),
        )

        return cursor.rowcount == 1


# --------------------------------------------------
# Retrieve pending events
# --------------------------------------------------

def get_pending_events(limit=100):
    """Retrieve events that have not been acknowledged by Kafka."""

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT event_id, event_json
            FROM ingestion_events
            WHERE status = 'pending'
            ORDER BY created_at, event_id
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        {
            "event_id": event_id,
            "event": json.loads(event_json),
        }
        for event_id, event_json in rows
    ]


# --------------------------------------------------
# Mark event as published
# --------------------------------------------------

def mark_event_published(event_id):
    """Mark an event as published after Kafka acknowledgement."""

    published_at = datetime.now(timezone.utc).isoformat()

    with get_connection() as connection:
        cursor = connection.execute(
            """
            UPDATE ingestion_events
            SET status = 'published',
                published_at = ?
            WHERE event_id = ?
              AND status = 'pending'
            """,
            (published_at, event_id),
        )

        return cursor.rowcount == 1
