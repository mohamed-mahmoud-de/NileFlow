"""
NileFlow — Milestone 3
Vehicle Positions Stream Processor

Reads vehicle position pings from the `vehicle_position_events` Kafka topic,
computes average speed per corridor over a 5-minute tumbling window, derives
a slow-corridor anomaly flag, and writes the resulting corridor speed stats
into the `nileflow.vehicle_speed_metrics` Cassandra table.

NOTE: this job must NOT write to `congestion_metrics` — its rows share the
same (corridor_id, event_time) primary key as the traffic processor's
windows, so writing congestion_index=0.0 there overwrites the real TomTom
congestion values whenever the 5-min and 10-min window boundaries align.
"""

import logging
import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    avg,
    window,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("vehicle_positions_processor")

# ---------------------------------------------------------------------------
# Configuration (env vars with docker-compose-friendly defaults)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "vehicle_position_events")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "nileflow")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "vehicle_speed_metrics")

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/vehicle_positions_checkpoint")

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
)

WATERMARK_DELAY = "2 minutes"
WINDOW_DURATION = "5 minutes"
SLOW_CORRIDOR_SPEED_THRESHOLD_KMH = 20

VEHICLE_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), True),
        StructField("corridor_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed_kmh", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("progress_pct", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("NileFlow-VehiclePositionsProcessor")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_to_cassandra(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        logger.info("Batch %s is empty, skipping.", batch_id)
        return

    try:
        row_count = batch_df.count()

        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE) \
            .mode("append") \
            .save()

        logger.info(
            "Batch %s: wrote %s row(s) to %s.%s",
            batch_id, row_count, CASSANDRA_KEYSPACE, CASSANDRA_TABLE,
        )
    except Exception:
        logger.exception("Batch %s: failed to write to Cassandra.", batch_id)
        raise


def main() -> None:
    spark = create_spark_session()
    logger.info("Spark session created. Starting NileFlow vehicle positions processor.")

    try:
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        value_stream = raw_stream.selectExpr("CAST(value AS STRING) AS json_value")

        parsed_stream = value_stream.select(
            from_json(col("json_value"), VEHICLE_SCHEMA).alias("data")
        ).select("data.*")

        watermarked_stream = parsed_stream.withWatermark("event_time", WATERMARK_DELAY)

        aggregated_stream = (
            watermarked_stream.groupBy(
                col("corridor_id"),
                window(col("event_time"), WINDOW_DURATION),
            )
            .agg(
                avg(col("speed_kmh")).alias("avg_speed"),
            )
        )

        with_anomaly_flag = aggregated_stream.withColumn(
            "is_anomaly",
            when(col("avg_speed") < SLOW_CORRIDOR_SPEED_THRESHOLD_KMH, True).otherwise(False),
        )

        cassandra_ready_stream = with_anomaly_flag.select(
            col("corridor_id"),
            col("window.end").alias("event_time"),
            col("avg_speed").alias("avg_speed_kmh"),
            col("is_anomaly"),
        )

        query = (
            cassandra_ready_stream.writeStream
            .foreachBatch(write_to_cassandra)
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .start()
        )

        logger.info("Streaming query started. Awaiting termination...")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Stopping stream gracefully.")
    except Exception:
        logger.exception("Fatal error in vehicle_positions_processor streaming job.")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
