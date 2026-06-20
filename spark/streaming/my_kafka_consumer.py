"""
NileFlow - Smart City Data Engineering Project
Module: Spark Structured Streaming - Traffic Events Kafka Consumer
Branch: feature/spark-consumer

Purpose:
    Minimal Spark Structured Streaming job that reads from the Kafka
    "traffic_events" topic and prints incoming events to the console.
    First step toward the full streaming pipeline: get data flowing
    from Kafka into Spark before adding parsing, transformations,
    and downstream sinks.
"""

import os
import sys
import logging

from pyspark.sql import SparkSession

# --------------------------------------------------------------------------- #
# Configuration (environment variables override defaults)
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic_events")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
APP_NAME = os.getenv("SPARK_APP_NAME", "NileFlow-TrafficEventsConsumer")

# --------------------------------------------------------------------------- #
# Logging setup
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nileflow.spark.traffic_events_consumer")


def build_spark_session() -> SparkSession:
    """Creates the SparkSession for this streaming job."""
    logger.info("Creating SparkSession '%s' ...", APP_NAME)
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created successfully.")
    return spark


def read_from_kafka(spark: SparkSession):
    """
    Opens a streaming DataFrame that continuously reads raw records
    from the traffic_events Kafka topic.
    """
    logger.info(
        "Connecting to Kafka topic '%s' on '%s' (startingOffsets=%s) ...",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, STARTING_OFFSETS,
    )
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )
    logger.info("Kafka streaming source created successfully.")
    return raw_stream


def decode_events(raw_stream):
    """
    Kafka delivers 'key' and 'value' as raw bytes. Cast 'value' (and
    'key') to STRING so events are human-readable when printed.
    """
    decoded = raw_stream.selectExpr(
        "CAST(key AS STRING) AS event_key",
        "CAST(value AS STRING) AS event_value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    )
    return decoded


def start_console_stream(decoded_stream):
    """Starts the streaming query that writes decoded events to console."""
    logger.info("Starting streaming query -> console sink ...")
    query = (
        decoded_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )
    logger.info("Streaming query started. Listening for events on topic '%s' ...", KAFKA_TOPIC)
    return query


def main() -> int:
    spark = None
    try:
        spark = build_spark_session()
        raw_stream = read_from_kafka(spark)
        decoded_stream = decode_events(raw_stream)
        query = start_console_stream(decoded_stream)

        query.awaitTermination()
        return 0

    except KeyboardInterrupt:
        logger.info("Streaming job interrupted by user. Shutting down ...")
        return 0
    except Exception as e:
        logger.exception("FAILURE: Spark streaming job crashed: %s", e)
        return 1
    finally:
        if spark is not None:
            spark.stop()
            logger.info("SparkSession stopped.")


if __name__ == "__main__":
    sys.exit(main())
