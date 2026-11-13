
import asyncio
import logging

from app.kafka_client import (
    create_kafka_producer,
    publish_event,
)
from app.spool import (
    get_pending_events,
    mark_event_published,
)


logger = logging.getLogger("maritime-publisher")

PUBLISH_BATCH_SIZE = 100
PUBLISH_RETRY_SECONDS = 5


async def publish_pending_events(producer):
    """Publish pending SQLite events to Kafka."""

    while True:
        try:
            pending_events = get_pending_events(
                limit=PUBLISH_BATCH_SIZE
            )

            if not pending_events:
                await asyncio.sleep(1)
                continue

            for item in pending_events:
                event_id = item["event_id"]
                event = item["event"]

                try:
                    await asyncio.to_thread(
                        publish_event,
                        producer,
                        event,
                    )

                    await asyncio.to_thread(
                        mark_event_published,
                        event_id,
                    )

                    logger.info(
                        "Event %s marked as published.",
                        event_id,
                    )

                except Exception:
                    logger.exception(
                        "Failed to publish event %s. "
                        "It remains pending.",
                        event_id,
                    )

                    await asyncio.sleep(
                        PUBLISH_RETRY_SECONDS
                    )

                    break

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Publisher worker encountered an error."
            )

            await asyncio.sleep(
                PUBLISH_RETRY_SECONDS
            )


async def main():
    """Run the independent Kafka publisher."""

    logger.info("Starting Kafka publisher worker.")

    producer = create_kafka_producer()

    try:
        await publish_pending_events(producer)

    finally:
        await asyncio.to_thread(producer.flush)
        producer.close()

        logger.info("Kafka publisher stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Publisher shutdown requested.")
