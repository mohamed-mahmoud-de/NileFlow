"""
NileFlow - Weather Streaming Processor
=======================================

Spark Structured Streaming job that:
  1. Reads raw weather events from the Kafka topic `weather_events`.
  2. Parses the JSON payload using an explicit StructType schema.
  3. Converts `event_time` (e.g. "2026-06-23T14:00") into a Spark timestamp.
  4. Applies a watermark to tolerate late-arriving data.
  5. Writes the cleaned records to the Cassandra table
     `nileflow.weather_metrics` via `foreachBatch`.

No aggregation and no anomaly detection are performed here — this job only
parses and stores weather events.

Connection settings are read from environment variables (with local
docker-compose-friendly defaults): KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
CASSANDRA_HOST, CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CHECKPOINT_LOCATION.

Run with:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
        spark/streaming/weather_processor.py
"""

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# --------------------------------------------------------------------------- #
# Configuration (env vars with sane local defaults)
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_events")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "nileflow")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "weather_metrics")

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/weather_checkpoint")

# Fixed business rules (not deployment config, so not env-driven):
WATERMARK_DELAY = "10 minutes"
EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm"  # e.g. "2026-06-23T14:00" (no "Z"/offset)

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("weather_processor")


# --------------------------------------------------------------------------- #
# Schema of the incoming Kafka JSON payload
# --------------------------------------------------------------------------- #
WEATHER_EVENT_SCHEMA = StructType(
    [
        StructField("location", StringType(), nullable=False),
        # latitude/longitude are part of the producer's payload but are not
        # stored in Cassandra; declared here so the schema documents the
        # full incoming contract.
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("event_time", StringType(), nullable=False),
        StructField("temperature_c", DoubleType(), nullable=True),
        StructField("humidity_pct", DoubleType(), nullable=True),
        StructField("precipitation_mm", DoubleType(), nullable=True),
        StructField("wind_speed_kmh", DoubleType(), nullable=True),
        StructField("weather_code", IntegerType(), nullable=True),
    ]
)

# Only these columns exist in the Cassandra target table.
CASSANDRA_COLUMNS = [
    "location",
    "event_time",
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "wind_speed_kmh",
    "weather_code",
]


def build_spark_session() -> SparkSession:
    """Create and configure the SparkSession with Kafka + Cassandra support.

    Note: `spark.jars.packages` is set here as a convenience for local/ad-hoc
    runs. For `spark-submit`, also pass the same coordinates via the
    `--packages` CLI flag — that is the reliable way to resolve dependencies
    in client/cluster deploy modes.
    """
    spark = (
        SparkSession.builder.appName("NileFlow-WeatherProcessor")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.sql.streaming.schemaInference", "false")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read raw events from the Kafka topic as a streaming DataFrame."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_weather_events(raw_df: DataFrame) -> DataFrame:
    """Cast Kafka value to STRING, parse JSON, cast event_time, add watermark.

    Records with a null `location` or unparseable `event_time` are dropped
    before the write stage, since both columns form the Cassandra primary
    key (location, event_time) and a null there would fail/corrupt the
    write. This is a data-quality guard, not aggregation or anomaly logic.
    """
    parsed_df = (
        raw_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), WEATHER_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time"), EVENT_TIME_FORMAT))
        .withWatermark("event_time", WATERMARK_DELAY)
        .filter(col("location").isNotNull() & col("event_time").isNotNull())
    )
    return parsed_df.select(*CASSANDRA_COLUMNS)


def write_to_cassandra(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: write one micro-batch to Cassandra."""
    # DataFrame.isEmpty() is stable and safe since Spark 3.3.0 (SPARK-32285);
    # it short-circuits with a `limit(1)` scan instead of a full count.
    if batch_df.isEmpty():
        logger.info("Batch %s is empty, skipping write.", batch_id)
        return

    try:
        record_count = batch_df.count()
        (
            batch_df.write.format("org.apache.spark.sql.cassandra")
            .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE)
            .mode("append")
            .save()
        )
        logger.info(
            "Batch %s: wrote %s record(s) to %s.%s",
            batch_id,
            record_count,
            CASSANDRA_KEYSPACE,
            CASSANDRA_TABLE,
        )
    except Exception:
        logger.exception("Batch %s: failed to write to Cassandra.", batch_id)
        raise


def main() -> None:
    """Build the streaming pipeline and run it until interrupted."""
    spark = build_spark_session()
    logger.info("Spark session created. Starting NileFlow weather processor.")

    try:
        raw_stream = read_kafka_stream(spark)
        weather_df = parse_weather_events(raw_stream)

        query = (
            weather_df.writeStream.foreachBatch(write_to_cassandra)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .outputMode("append")
            .start()
        )

        logger.info("Streaming query started. Awaiting termination...")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Stopping stream gracefully.")
    except Exception:
        logger.exception("Fatal error in weather_processor streaming job.")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
