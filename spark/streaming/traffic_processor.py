"""
NileFlow — Milestone 3
Traffic Stream Processor

Reads traffic corridor events from the `traffic_events` Kafka topic, parses
the JSON payload, computes a rolling average congestion index and speed per
corridor over a 10-minute tumbling window, flags congestion-spike anomalies,
writes the metrics to Cassandra, and pushes alerts for anomalous windows to
Elasticsearch.
"""

import logging
import os
import sys
from datetime import datetime, timezone

import requests

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, avg, count, window, when, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("traffic_processor")

# ---------------------------------------------------------------------------
# Configuration (env vars with docker-compose-friendly defaults)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic_events")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "nileflow")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "congestion_metrics")

ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL", "http://elasticsearch:9200/congestion_alerts/_doc"
)

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/traffic_checkpoint")

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
)

WATERMARK_DELAY = "5 minutes"
WINDOW_DURATION = "10 minutes"

CONGESTION_INDEX_ANOMALY_THRESHOLD = 0.7
SPEED_KMH_ANOMALY_THRESHOLD = 15

TRAFFIC_SCHEMA = StructType(
    [
        StructField("corridor_id", StringType(), True),
        StructField("corridor_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("travel_time_sec", IntegerType(), True),
        StructField("free_flow_sec", IntegerType(), True),
        StructField("congestion_index", DoubleType(), True),
        StructField("speed_kmh", DoubleType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("event_time", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("NileFlow-TrafficProcessor")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def send_elasticsearch_alert(row) -> None:
    alert_payload = {
        "corridor_id": row["corridor_id"],
        "alert_type": "congestion_spike",
        "severity": "high",
        "congestion_index": row["congestion_index"],
        "speed_kmh": row["speed_kmh"],
        "message": (
            f"High congestion on {row['corridor_id']}: "
            f"index {row['congestion_index']:.2f}, "
            f"speed {row['speed_kmh']:.1f} km/h"
        ),
        "event_time": row["event_time"].isoformat() + "Z"
        if row["event_time"] is not None
        else None,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    try:
        response = requests.post(ELASTICSEARCH_URL, json=alert_payload, timeout=5)
        if response.status_code not in (200, 201):
            logger.warning(
                "Elasticsearch returned status %s for corridor %s: %s",
                response.status_code,
                row["corridor_id"],
                response.text,
            )
        else:
            logger.info(
                "Alert indexed for corridor %s (index=%.2f, speed=%.1f)",
                row["corridor_id"],
                row["congestion_index"],
                row["speed_kmh"],
            )
    except requests.exceptions.RequestException as exc:
        logger.warning(
            "Failed to POST alert to Elasticsearch for corridor %s: %s",
            row["corridor_id"],
            exc,
        )


def write_to_cassandra(batch_df: DataFrame, batch_id: int) -> None:
    try:
        batch_df.persist()

        if batch_df.isEmpty():
            logger.info("Batch %s is empty, skipping.", batch_id)
            return

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

        anomaly_rows = batch_df.filter(col("is_anomaly") == True).collect()  # noqa: E712

        if anomaly_rows:
            logger.info(
                "Batch %s: sending %s anomaly alert(s) to Elasticsearch.",
                batch_id, len(anomaly_rows),
            )
            for row in anomaly_rows:
                send_elasticsearch_alert(row)
        else:
            logger.info("Batch %s: no anomalies.", batch_id)

    except Exception:
        logger.exception("Batch %s: failed to write to Cassandra.", batch_id)
        raise
    finally:
        batch_df.unpersist()


def main() -> None:
    spark = create_spark_session()
    logger.info("Spark session created. Starting NileFlow traffic processor.")

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

        parsed_stream = (
            value_stream.select(
                from_json(col("json_value"), TRAFFIC_SCHEMA).alias("data")
            )
            .select("data.*")
            .withColumn("event_time", col("event_time").cast("timestamp"))
        )

        watermarked_stream = parsed_stream.withWatermark("event_time", WATERMARK_DELAY)

        aggregated_stream = (
            watermarked_stream.groupBy(
                col("corridor_id"),
                window(col("event_time"), WINDOW_DURATION),
            )
            .agg(
                avg(col("congestion_index")).alias("congestion_index"),
                avg(col("speed_kmh")).alias("speed_kmh"),
                count("*").alias("event_count"),
            )
        )

        with_anomaly_flag = aggregated_stream.withColumn(
            "is_anomaly",
            when(
                (col("congestion_index") > CONGESTION_INDEX_ANOMALY_THRESHOLD)
                | (col("speed_kmh") < SPEED_KMH_ANOMALY_THRESHOLD),
                True,
            ).otherwise(False),
        )

        cassandra_ready_stream = with_anomaly_flag.select(
            col("corridor_id"),
            col("window.end").alias("event_time"),
            lit(0).alias("travel_time_sec"),
            lit(0).alias("free_flow_sec"),
            col("congestion_index"),
            col("speed_kmh"),
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
        logger.exception("Fatal error in traffic_processor streaming job.")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
