import time
import logging
from pathlib import Path
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    BooleanType,
)
from app.storage import (initialize_database,save_processed_event)
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vessel-events"

SPARK_KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
)

CHECKPOINT_PATH = (
    PROJECT_ROOT / "data" / "spark_checkpoints" / "vessel_events"
)


def create_spark_session():
    """Create the Spark session with the Kafka connector."""

    spark = (
        SparkSession.builder
        .appName("MaritimeRouteRiskMonitor")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            SPARK_KAFKA_PACKAGE,
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def read_kafka_stream(spark):
    """Read continuous vessel events from the Kafka topic."""

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )



VESSEL_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("schema_version", IntegerType(), True),
    StructField("mmsi", StringType(), True),
    StructField("vessel_name", StringType(), True),

    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_over_ground", DoubleType(), True),
    StructField("course_over_ground", DoubleType(), True),
    StructField("heading", IntegerType(), True),

    StructField("event_timestamp", StringType(), True),
    StructField("source_timestamp_raw", StringType(), True),
    StructField("ais_timestamp_second", IntegerType(), True),
    StructField("ingestion_timestamp", StringType(), True),

    StructField("raw_message", StructType([
        StructField("MetaData", StructType([
            StructField("MMSI", LongType(), True),
            StructField("MMSI_String", LongType(), True),
            StructField("ShipName", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("time_utc", StringType(), True),
        ]), True),

        StructField("MessageType", StringType(), True),

        StructField("Message", StructType([
            StructField("PositionReport", StructType([
                StructField("MessageID", IntegerType(), True),
                StructField("RepeatIndicator", IntegerType(), True),
                StructField("UserID", LongType(), True),
                StructField("Valid", BooleanType(), True),
                StructField("NavigationalStatus", IntegerType(), True),
                StructField("RateOfTurn", IntegerType(), True),
                StructField("Sog", DoubleType(), True),
                StructField("PositionAccuracy", BooleanType(), True),
                StructField("Longitude", DoubleType(), True),
                StructField("Latitude", DoubleType(), True),
                StructField("Cog", DoubleType(), True),
                StructField("TrueHeading", IntegerType(), True),
                StructField("Timestamp", IntegerType(), True),
                StructField("SpecialManoeuvreIndicator", IntegerType(), True),
                StructField("Spare", IntegerType(), True),
                StructField("Raim", BooleanType(), True),
                StructField("CommunicationState", LongType(), True),
            ]), True),
        ]), True),
    ]), True),

    StructField("weather", StructType([
        StructField("cell_id", StringType(), True),

        StructField("reference_location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]), True),

        StructField("forecast_timestamp", StringType(), True),
        StructField("retrieved_at", StringType(), True),

        StructField("marine", StructType([
            StructField("wave_height_m", DoubleType(), True),
            StructField("wave_period_s", DoubleType(), True),
            StructField("current_velocity_ms", DoubleType(), True),
            StructField("current_direction_deg", DoubleType(), True),
        ]), True),

        StructField("weather", StructType([
            StructField("wind_speed_kmh", DoubleType(), True),
            StructField("wind_gusts_kmh", DoubleType(), True),
        ]), True),

        StructField("units", StructType([
            StructField("wave_height", StringType(), True),
            StructField("wave_period", StringType(), True),
            StructField("current_velocity", StringType(), True),
            StructField("wind_speed", StringType(), True),
            StructField("wind_gusts", StringType(), True),
        ]), True),
    ]), True),
])


def parse_vessel_events(kafka_stream):
    """Convert Kafka binary JSON values into structured vessel events."""

    parsed_stream = (
        kafka_stream
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(
                col("value").cast("string"),
                VESSEL_EVENT_SCHEMA,
            ).alias("event"),
        )
    )

    return parsed_stream.select(
        "kafka_key",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "event.*",
    )


def build_normalized_vessel_stream(parsed_stream):
    """Flatten valid AIS position reports and their weather enrichment."""

    return (
        parsed_stream
        .filter(
            (col("raw_message.MessageType") == "PositionReport")
            & col("event_id").isNotNull()
            & col("mmsi").isNotNull()
            & col("latitude").isNotNull()
            & col("longitude").isNotNull()
            & col("event_timestamp").isNotNull()
        )
        .select(
            # Event identity and Kafka traceability
            col("event_id"),
            col("schema_version"),
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),

            # Vessel identity
            col("mmsi"),
            col("vessel_name"),

            # Normalized AIS measurements
            col("latitude"),
            col("longitude"),
            col("speed_over_ground"),
            col("course_over_ground"),
            col("heading"),

            # Original AIS timestamp information
            col("event_timestamp"),
            col("source_timestamp_raw"),
            col("ais_timestamp_second"),
            col("ingestion_timestamp"),

            # Original AIS message for traceability
            col("raw_message"),

            # Weather reference and forecast
            col("weather.cell_id").alias("weather_cell_id"),
            col("weather.reference_location.latitude")
                .alias("weather_reference_latitude"),
            col("weather.reference_location.longitude")
                .alias("weather_reference_longitude"),
            col("weather.forecast_timestamp"),
            col("weather.retrieved_at"),

            # Marine conditions
            col("weather.marine.wave_height_m"),
            col("weather.marine.wave_period_s"),
            col("weather.marine.current_velocity_ms"),
            col("weather.marine.current_direction_deg"),

            # Atmospheric conditions
            col("weather.weather.wind_speed_kmh"),
            col("weather.weather.wind_gusts_kmh"),
        )
    )


def spark_row_to_storage_event(row):
    """Convert a Spark Row into a dictionary for SQLite storage."""

    return row.asDict(recursive=True)


def write_batch_to_sqlite(batch_df, batch_id):
    """Persist one Spark micro-batch into the analytical SQLite database."""

    inserted_count = 0
    duplicate_count = 0

    for row in batch_df.toLocalIterator():
        event = spark_row_to_storage_event(row)

        inserted = save_processed_event(event)

        if inserted:
            inserted_count += 1
        else:
            duplicate_count += 1

    logging.info(
        "Batch %s completed: %s inserted, %s duplicates ignored.",
        batch_id,
        inserted_count,
        duplicate_count,
    )

def start_spark_streaming(spark):
    """Start continuous Kafka-to-SQLite processing."""

    # Ensure the analytical database and tables exist.
    initialize_database()

    # Read events from Kafka.
    kafka_stream = read_kafka_stream(spark)

    # Parse the incoming JSON messages.
    parsed_stream = parse_vessel_events(kafka_stream)

    # Normalize vessel and weather information.
    normalized_stream = build_normalized_vessel_stream(
        parsed_stream
    )

    # Persist every Spark micro-batch into SQLite.
    query = (
        normalized_stream.writeStream
        .outputMode("append")
        .foreachBatch(write_batch_to_sqlite)
        .option(
            "checkpointLocation",
            str(CHECKPOINT_PATH),
        )
        .trigger(processingTime="5 seconds")
        .queryName("vessel_events_to_sqlite")
        .start()
    )

    logging.info(
        "Spark Structured Streaming started successfully."
    )

    return query


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    spark = create_spark_session()
    retry_delay = 10

    try:
        while True:
            query = None

            try:
                logging.info(
                    "Starting Spark Structured Streaming..."
                )

                query = start_spark_streaming(spark)

                logging.info(
                    "Waiting for incoming vessel events..."
                )

                query.awaitTermination()

                logging.warning(
                    "Spark streaming query terminated. "
                    "Preparing to restart..."
                )

            except KeyboardInterrupt:
                logging.info(
                    "Shutdown requested by user."
                )
                break

            except Exception:
                logging.exception(
                    "Spark streaming query failed. "
                    "Automatic recovery will be attempted."
                )

            finally:
                if query is not None:
                    try:
                        if query.isActive:
                            query.stop()
                    except Exception:
                        logging.exception(
                            "Error while stopping failed query."
                        )

            logging.warning(
                "Restarting Spark streaming in %s seconds...",
                retry_delay,
            )

            time.sleep(retry_delay)

    finally:
        logging.info("Stopping Spark session...")

        spark.stop()

        logging.info("Spark session stopped.")
