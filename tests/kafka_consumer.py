
import json
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "vessel-events"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="maritime-test-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    key_deserializer=lambda key: key.decode("utf-8") if key else None
)

print("Connected to Kafka successfully!")
print(f"Listening to topic: {TOPIC_NAME}")
print("Waiting for messages...\n")

try:
    for message in consumer:
        print("Received Kafka Message")
        print("-----------------------")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key (MMSI): {message.key}")
        print("Message:")
        print(json.dumps(message.value, indent=4))
        print()

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
