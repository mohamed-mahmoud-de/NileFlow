"""
NileFlow — Milestone 3
Vehicle Positions Stream Processor

Reads vehicle position pings from the `vehicle_position_events` Kafka topic,
computes average speed and distinct vehicle count per corridor over a
5-minute tumbling window, derives a slow-corridor anomaly flag, and writes
the resulting corridor speed stats into the shared `nileflow.congestion_metrics`
Cassandra table (alongside the Traffic Stream Processor's output).

"""

import sys
import traceback

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    avg,
    countDistinct,
    window,
    when,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Constants / configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "vehicle_position_events"

CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = "9042"
CASSANDRA_KEYSPACE = "nileflow"
CASSANDRA_TABLE = "congestion_metrics"

CHECKPOINT_LOCATION = "/tmp/vehicle_positions_checkpoint"

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
)

WATERMARK_DELAY = "2 minutes"
WINDOW_DURATION = "5 minutes"
SLOW_CORRIDOR_SPEED_THRESHOLD_KMH = 20

# StructType matching the vehicle_position_events JSON payload produced by
# producers/vehicle_positions/producer.py
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
    """
    Build and configure the SparkSession used by this streaming job.

    Configures:
        - Kafka + Cassandra connector packages
        - Cassandra connection host/port

    Returns:
        SparkSession: configured Spark session.
    """
    try:
        spark = (
            SparkSession.builder.appName("NileFlow-VehiclePositionsProcessor")
            .config("spark.jars.packages", SPARK_PACKAGES)
            .config("spark.cassandra.connection.host", CASSANDRA_HOST)
            .config("spark.cassandra.connection.port", CASSANDRA_PORT)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as exc:
        print(f"[FATAL] Failed to create SparkSession: {exc}", file=sys.stderr)
        traceback.print_exc()
        raise


def write_to_cassandra(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch sink function: writes a micro-batch of corridor speed
    aggregates into the shared nileflow.congestion_metrics Cassandra table.

    Args:
        batch_df: micro-batch DataFrame already shaped to match the
            congestion_metrics table columns.
        batch_id: the unique id of this micro-batch (provided by Spark).
    """
    try:
        row_count = batch_df.count()

        if row_count == 0:
            print(f"[batch {batch_id}] No rows to write. Skipping.")
            return

        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", CASSANDRA_KEYSPACE) \
            .option("table", CASSANDRA_TABLE) \
            .mode("append") \
            .save()

        print(f"[batch {batch_id}] Wrote {row_count} row(s) to "
              f"{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}.")

    except Exception as exc:
        # Log and re-raise so Spark surfaces the failure rather than
        # silently dropping a micro-batch.
        print(f"[batch {batch_id}] ERROR writing to Cassandra: {exc}",
              file=sys.stderr)
        traceback.print_exc()
        raise


def main() -> None:
    """
    Entry point: builds the streaming pipeline and runs it until terminated.

    Pipeline stages:
        1. Read raw bytes from Kafka topic `vehicle_position_events`.
        2. Cast value to STRING and parse JSON using VEHICLE_SCHEMA.
        3. Apply a watermark on event_time to bound late data.
        4. Aggregate avg(speed_kmh) and countDistinct(vehicle_id) per
           corridor_id over 5-minute tumbling windows.
        5. Derive is_anomaly when avg_speed < 20 km/h (slow corridor).
        6. Reshape to match nileflow.congestion_metrics and write via
           foreachBatch.
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

        # ------------------------------------------------------------------
        # 3. Parse JSON with the vehicle schema
        # ------------------------------------------------------------------
        parsed_stream = value_stream.select(
            from_json(col("json_value"), VEHICLE_SCHEMA).alias("data")
        ).select("data.*")

        # ------------------------------------------------------------------
        # 4. Extracted fields are already top-level after the select above:
        #    vehicle_id, corridor_id, latitude, longitude, speed_kmh,
        #    heading, progress_pct, event_time
        # ------------------------------------------------------------------

        # ------------------------------------------------------------------
        # 5. Apply watermark on event_time (vehicle pings every 10 sec,
        #    so 2 minutes of lateness tolerance is plenty)
        # ------------------------------------------------------------------
        watermarked_stream = parsed_stream.withWatermark("event_time", WATERMARK_DELAY)

        # ------------------------------------------------------------------
        # 6. Aggregate: group by corridor_id + 5-minute tumbling window
        #    avg(speed_kmh) and countDistinct(vehicle_id)
        # ------------------------------------------------------------------
        aggregated_stream = (
            watermarked_stream.groupBy(
                col("corridor_id"),
                window(col("event_time"), WINDOW_DURATION),
            )
            .agg(
                avg(col("speed_kmh")).alias("avg_speed"),
                countDistinct(col("vehicle_id")).alias("vehicle_count"),
            )
        )

        # ------------------------------------------------------------------
        # 7. Derive is_anomaly: true when avg_speed < 20 km/h (slow corridor)
        # ------------------------------------------------------------------
        with_anomaly_flag = aggregated_stream.withColumn(
            "is_anomaly",
            when(col("avg_speed") < SLOW_CORRIDOR_SPEED_THRESHOLD_KMH, True).otherwise(False),
        )

        # ------------------------------------------------------------------
        # 8. Reshape to match nileflow.congestion_metrics schema:
        #    corridor_id, event_time (= window.end), travel_time_sec=0,
        #    free_flow_sec=0, congestion_index=0.0, speed_kmh=avg_speed,
        #    is_anomaly
        # ------------------------------------------------------------------
        cassandra_ready_stream = with_anomaly_flag.select(
            col("corridor_id"),
            col("window.end").alias("event_time"),
            lit(0).alias("travel_time_sec"),
            lit(0).alias("free_flow_sec"),
            lit(0.0).alias("congestion_index"),
            col("avg_speed").alias("speed_kmh"),
            col("is_anomaly"),
        )

        # ------------------------------------------------------------------
        # 9-10. Write via foreachBatch into Cassandra, outputMode("update"),
        #       with a dedicated checkpoint location.
        # ------------------------------------------------------------------
        query = (
            cassandra_ready_stream.writeStream
            .foreachBatch(write_to_cassandra)
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .start()
        )

        print("Vehicle Positions Stream Processor started. "
              "Awaiting termination...")

        # ------------------------------------------------------------------
        # 11. Block until the query terminates (or fails).
        # ------------------------------------------------------------------
        query.awaitTermination()

    except Exception as exc:
        print(f"[FATAL] Vehicle Positions Stream Processor failed: {exc}",
              file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
