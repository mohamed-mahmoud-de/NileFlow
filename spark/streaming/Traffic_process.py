"""
NileFlow — Milestone 3
Traffic Stream Processor

Reads traffic corridor events from the `traffic_events` Kafka topic, parses
the JSON payload, computes a rolling average congestion index and speed per
corridor over a 10-minute tumbling window, flags congestion-spike anomalies,
writes the metrics to Cassandra, and pushes alerts for anomalous windows to
Elasticsearch.

"""

import sys
import traceback
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
# Constants / configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "traffic_events"

CASSANDRA_KEYSPACE = "nileflow"
CASSANDRA_TABLE = "congestion_metrics"

ELASTICSEARCH_URL = "http://elasticsearch:9200/congestion_alerts/_doc"

CHECKPOINT_LOCATION = "/tmp/traffic_checkpoint"

WATERMARK_DELAY = "5 minutes"
WINDOW_DURATION = "10 minutes"

CONGESTION_INDEX_ANOMALY_THRESHOLD = 0.7
SPEED_KMH_ANOMALY_THRESHOLD = 15

# StructType matching the traffic_events JSON payload produced by
# producers/traffic/producer.py
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
        StructField("event_time", TimestampType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    """
    Build the SparkSession for this streaming job.

    NOTE: spark.jars.packages and spark.cassandra.connection.host/port are
    assumed to be supplied via spark-submit --packages / --conf flags (per
    the Shared Setup doc), so they are intentionally not hardcoded here.
    This keeps the job portable across environments without code changes.

    Returns:
        SparkSession: configured Spark session.
    """
    try:
        spark = (
            SparkSession.builder.appName("NileFlow-TrafficProcessor").getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as exc:
        print(f"[FATAL] Failed to create SparkSession: {exc}", file=sys.stderr)
        traceback.print_exc()
        raise


def send_elasticsearch_alert(row) -> None:
    """
    POST a single congestion-spike alert document to Elasticsearch.

    Args:
        row: a pyspark.sql.Row from the aggregated batch DataFrame,
             expected to expose corridor_id, congestion_index, speed_kmh,
             and event_time.
    """
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
        response = requests.post(
            ELASTICSEARCH_URL,
            json=alert_payload,
            timeout=5,
        )
        if response.status_code not in (200, 201):
            print(
                f"[WARN] Elasticsearch returned status "
                f"{response.status_code} for corridor "
                f"{row['corridor_id']}: {response.text}",
                file=sys.stderr,
            )
        else:
            print(
                f"[ES] Alert indexed for corridor {row['corridor_id']} "
                f"(index={row['congestion_index']:.2f}, "
                f"speed={row['speed_kmh']:.1f})"
            )
    except requests.exceptions.RequestException as exc:
        # Don't let an Elasticsearch outage crash the whole micro-batch —
        # Cassandra write (the metrics of record) should still succeed.
        print(
            f"[WARN] Failed to POST alert to Elasticsearch for corridor "
            f"{row['corridor_id']}: {exc}",
            file=sys.stderr,
        )


def write_to_cassandra(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch sink function for the traffic processor.

    For every micro-batch:
        1. Write all aggregated corridor metrics to
           nileflow.congestion_metrics in Cassandra.
        2. For rows flagged is_anomaly = True, POST an alert document to
           the congestion_alerts index in Elasticsearch.

    Args:
        batch_df: micro-batch DataFrame already shaped to match the
            congestion_metrics table columns plus is_anomaly.
        batch_id: the unique id of this micro-batch (provided by Spark).
    """
    try:
        # Cache since we scan this batch twice (Cassandra write + ES loop).
        batch_df.persist()
        row_count = batch_df.count()

        if row_count == 0:
            print(f"[batch {batch_id}] No rows to write. Skipping.")
            return

        # ------------------------------------------------------------------
        # 1. Write metrics to Cassandra
        # ------------------------------------------------------------------
        try:
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", CASSANDRA_KEYSPACE) \
                .option("table", CASSANDRA_TABLE) \
                .mode("append") \
                .save()

            print(
                f"[batch {batch_id}] Wrote {row_count} row(s) to "
                f"{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}."
            )
        except Exception as exc:
            print(
                f"[batch {batch_id}] ERROR writing to Cassandra: {exc}",
                file=sys.stderr,
            )
            traceback.print_exc()
            raise  # Cassandra is the source of truth; surface this failure.

        # ------------------------------------------------------------------
        # 2. Push alerts for anomalous windows to Elasticsearch
        # ------------------------------------------------------------------
        anomaly_rows = batch_df.filter(col("is_anomaly") == True).collect()  # noqa: E712

        if anomaly_rows:
            print(
                f"[batch {batch_id}] Sending {len(anomaly_rows)} "
                f"anomaly alert(s) to Elasticsearch."
            )
            for row in anomaly_rows:
                send_elasticsearch_alert(row)
        else:
            print(f"[batch {batch_id}] No anomalies in this batch.")

    finally:
        batch_df.unpersist()


def main() -> None:
    """
    Entry point: builds the streaming pipeline and runs it until terminated.

    Pipeline stages:
        1. Read raw bytes from Kafka topic `traffic_events`.
        2. Cast value to STRING and parse JSON using TRAFFIC_SCHEMA.
        3. Apply a 5-minute watermark on event_time.
        4. Aggregate avg(congestion_index), avg(speed_kmh), count(*) per
           corridor_id over 10-minute tumbling windows.
        5. Derive is_anomaly when avg congestion_index > 0.7 OR
           avg speed_kmh < 15.
        6. Reshape to match nileflow.congestion_metrics and write via
           foreachBatch (Cassandra + Elasticsearch alerts).
    """
    spark = create_spark_session()

    try:
        # ------------------------------------------------------------------
        # 1-2. Read from Kafka and cast value to STRING
        # ------------------------------------------------------------------
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        value_stream = raw_stream.selectExpr("CAST(value AS STRING) AS json_value")

        # Parse JSON with the traffic schema
        parsed_stream = value_stream.select(
            from_json(col("json_value"), TRAFFIC_SCHEMA).alias("data")
        ).select("data.*")

        # ------------------------------------------------------------------
        # 3. Apply watermark on event_time (5-minute threshold)
        # ------------------------------------------------------------------
        watermarked_stream = parsed_stream.withWatermark("event_time", WATERMARK_DELAY)

        # ------------------------------------------------------------------
        # 4. Rolling aggregation: corridor_id + 10-minute tumbling window
        #    avg(congestion_index), avg(speed_kmh), count(*)
        # ------------------------------------------------------------------
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

        # ------------------------------------------------------------------
        # 5. Anomaly detection: avg congestion_index > 0.7 OR avg speed < 15
        # ------------------------------------------------------------------
        with_anomaly_flag = aggregated_stream.withColumn(
            "is_anomaly",
            when(
                (col("congestion_index") > CONGESTION_INDEX_ANOMALY_THRESHOLD)
                | (col("speed_kmh") < SPEED_KMH_ANOMALY_THRESHOLD),
                True,
            ).otherwise(False),
        )

        # ------------------------------------------------------------------
        # Reshape to match nileflow.congestion_metrics schema:
        # corridor_id, event_time (= window.end), travel_time_sec,
        # free_flow_sec, congestion_index, speed_kmh, is_anomaly
        #
        # NOTE: travel_time_sec / free_flow_sec are raw per-event fields in
        # the source data, not naturally averaged like congestion_index /
        # speed_kmh. Since this is a windowed aggregate, there is no single
        # representative raw value to write, so both are set to 0 — the
        # same convention Task 3 (vehicle positions) uses for fields that
        # don't apply to its aggregation of this shared table.
        # ------------------------------------------------------------------
        cassandra_ready_stream = with_anomaly_flag.select(
            col("corridor_id"),
            col("window.end").alias("event_time"),
            lit(0).alias("travel_time_sec"),
            lit(0).alias("free_flow_sec"),
            col("congestion_index"),
            col("speed_kmh"),
            col("is_anomaly"),
        )

        # ------------------------------------------------------------------
        # 6. Write via foreachBatch (Cassandra + Elasticsearch alerts)
        # ------------------------------------------------------------------
        query = (
            cassandra_ready_stream.writeStream
            .foreachBatch(write_to_cassandra)
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .start()
        )

        print("Traffic Stream Processor started. Awaiting termination...")

        query.awaitTermination()

    except Exception as exc:
        print(f"[FATAL] Traffic Stream Processor failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
