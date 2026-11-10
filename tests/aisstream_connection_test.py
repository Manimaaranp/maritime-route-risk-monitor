
import asyncio
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from websockets.asyncio.client import connect


# Load the API key from the project's root .env file
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")

WEBSOCKET_URL = "wss://stream.aisstream.io/v0/stream"

# Broad initial North Sea test area:
# Northwest corner: 61°N, 4°W
# Southeast corner: 51°N, 9°E
NORTH_SEA_BOUNDING_BOX = [
    [
        [61.0, -4.0],
        [51.0, 9.0],
    ]
]


async def main():
    if not AISSTREAM_API_KEY:
        raise SystemExit(
            "ERROR: AISSTREAM_API_KEY was not found in the root .env file."
        )

    subscription = {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": NORTH_SEA_BOUNDING_BOX,
        "FilterMessageTypes": ["PositionReport"],
    }

    print("Connecting to AISStream...")

    async with connect(
        WEBSOCKET_URL,
        compression="deflate",
        open_timeout=15,
        ping_interval=20,
        ping_timeout=20,
    ) as websocket:

        # Send the subscription immediately after connecting
        await websocket.send(json.dumps(subscription))
        print("Subscription sent. Waiting for AIS messages...\n")

        received_count = 0

        while received_count < 10:
            try:
                raw_message = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=60,
                )
            except TimeoutError:
                print("No message received for 60 seconds. Ending test.")
                break

            # AISStream sends binary frames containing UTF-8 JSON
            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode("utf-8")

            event = json.loads(raw_message)
            message_type = event.get("MessageType")

            if message_type == "SubscriptionConfirmation":
                compression = event.get("Message", {}).get(
                    "CompressionEnabled"
                )
                print(
                    f"Subscription confirmed. "
                    f"Compression enabled: {compression}\n"
                )

            elif message_type == "PositionReport":
                metadata = event.get("MetaData", {})
                position = event.get("Message", {}).get(
                    "PositionReport", {}
                )

                received_count += 1

                print(f"Vessel event {received_count}/10")
                print(f"MMSI:      {metadata.get('MMSI')}")
                print(f"Ship name: {metadata.get('ShipName')}")
                print(f"Latitude:  {metadata.get('Latitude')}")
                print(f"Longitude: {metadata.get('Longitude')}")
                print(f"Speed:     {position.get('Sog')} knots")
                print(f"Course:    {position.get('Cog')} degrees")
                print("-" * 45)

            else:
                print(f"Other message received: {message_type}")

        print(f"\nTest finished. Position reports received: {received_count}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest stopped by user.")
    except Exception as error:
        print(f"\nConnection test failed: {type(error).__name__}: {error}")
