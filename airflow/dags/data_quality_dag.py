"""
NileFlow — Data Quality & Pipeline Freshness DAG
==================================================
 
This DAG runs every 30 minutes and verifies that all core NileFlow
pipeline components are healthy and that data is flowing end-to-end:
 
    1. check_kafka_topics        -> traffic_events, weather_events,
                                      vehicle_position_events
    2. check_cassandra_freshness -> congestion_metrics, weather_metrics
    3. check_elasticsearch       -> cluster health endpoint
 
All three checks are independent (no inter-task dependencies) and run
in parallel. Each task is defensive: a failure in one dependency
(e.g. Kafka being down) must never crash the DAG or block the other
checks. Issues are surfaced as WARNING-level log entries so they show
up clearly in the Airflow task logs / log-based alerting, without
failing the DAG run itself.
 
File location: airflow/dags/data_quality_dag.py
"""
 
from __future__ import annotations
 
import logging
import os
from datetime import datetime, timedelta, timezone
 
from airflow import DAG
from airflow.operators.python import PythonOperator
 
# ---------------------------------------------------------------------------
# Project configuration
# ---------------------------------------------------------------------------
# CASSANDRA_HOSTS / CASSANDRA_KEYSPACE are expected to live in the existing
# NileFlow config module. Importing here (rather than re-declaring values)
# keeps this DAG aligned with whatever config the rest of the pipeline uses.
#
# Wrapped in try/except so that DAG parsing never fails just because
# config.settings is missing, misconfigured, or not yet on the PYTHONPATH
# (e.g. during local development or a partial container build). A missing
# config module should never take the whole DAG out of the Airflow UI.
try:
    from config.settings import CASSANDRA_HOSTS, CASSANDRA_KEYSPACE
except ImportError:
    logging.getLogger(__name__).warning(
        "config.settings not importable; falling back to default "
        "Cassandra connection settings (hosts=['cassandra'], "
        "keyspace='nileflow')."
    )
    CASSANDRA_HOSTS = ["cassandra"]
    CASSANDRA_KEYSPACE = "nileflow"
 
logger = logging.getLogger(__name__)
 
# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------
 
# How stale a topic / table can be before we consider it unhealthy.
FRESHNESS_THRESHOLD_MINUTES = 15
 
KAFKA_TOPICS = [
    "traffic_events",
    "weather_events",
    "vehicle_position_events",
]
 
CASSANDRA_TABLES = [
    "congestion_metrics",
    "weather_metrics",
]
 
# Kafka bootstrap servers and Elasticsearch URL are environment-driven so no
# credentials or infra hostnames are hardcoded in source control. Sensible
# defaults match the NileFlow docker-compose service names.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ELASTICSEARCH_HEALTH_URL = os.environ.get(
    "ELASTICSEARCH_HEALTH_URL", "http://elasticsearch:9200/_cluster/health"
)
ELASTICSEARCH_REQUEST_TIMEOUT_SECONDS = int(
    os.environ.get("ELASTICSEARCH_TIMEOUT_SECONDS", "10")
)
 
# Kafka consumer group used solely for freshness checks. Isolated from any
# "real" consumer groups so this DAG never interferes with actual pipeline
# consumers or offset commits.
KAFKA_HEALTHCHECK_GROUP_ID = os.environ.get(
    "KAFKA_HEALTHCHECK_GROUP_ID", "nileflow-data-quality-healthcheck"
)
 
 
# ---------------------------------------------------------------------------
# Task 1: check_kafka_topics
# ---------------------------------------------------------------------------
def check_kafka_topics(**context) -> None:
    """
    Verify that the core Kafka topics exist and have received messages
    recently.
 
    For each topic in KAFKA_TOPICS:
      - Confirm the topic exists in the cluster metadata.
      - Peek at the latest available message (without committing offsets
        or disturbing real consumer groups) and compare its timestamp
        against FRESHNESS_THRESHOLD_MINUTES.
      - Log a WARNING if the topic is missing or stale.
 
    Any Kafka connectivity issue is caught and logged; this function never
    raises, so a Kafka outage does not fail the DAG run.
    """
    # Imported inside the function so that a missing/broken confluent_kafka
    # install only affects this task, not DAG parsing as a whole.
    from confluent_kafka import Consumer, KafkaException, TopicPartition
 
    consumer = None
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_HEALTHCHECK_GROUP_ID,
                # We only ever read metadata / watermarks here, never commit.
                "enable.auto.commit": False,
                "socket.timeout.ms": 10000,
                # Bound how long the broker waits before considering this
                # consumer dead, and how long between polls is tolerated,
                # so a slow/unavailable broker can't hang the task.
                "session.timeout.ms": 10000,
                "max.poll.interval.ms": 30000,
            }
        )
 
        # Fetch cluster metadata once; reused for the existence check below.
        cluster_metadata = consumer.list_topics(timeout=10)
        available_topics = set(cluster_metadata.topics.keys())
 
        now_utc = datetime.now(timezone.utc)
        threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
 
        for topic in KAFKA_TOPICS:
            if topic not in available_topics:
                logger.warning(
                    "Kafka topic '%s' does not exist on the cluster.", topic
                )
                continue
 
            topic_metadata = cluster_metadata.topics[topic]
            if topic_metadata.error is not None:
                logger.warning(
                    "Kafka topic '%s' reported a metadata error: %s",
                    topic,
                    topic_metadata.error,
                )
                continue
 
            partitions = list(topic_metadata.partitions.keys())
            if not partitions:
                logger.warning("Kafka topic '%s' has no partitions.", topic)
                continue
 
            latest_message_time = None
 
            # Check each partition's latest offset; use the most recent
            # message timestamp across all partitions as the topic's
            # freshness indicator.
            for partition_id in partitions:
                tp = TopicPartition(topic, partition_id)
 
                try:
                    low_offset, high_offset = consumer.get_watermark_offsets(
                        tp, timeout=10, cached=False
                    )
                except KafkaException as partition_err:
                    logger.warning(
                        "Could not fetch watermark offsets for topic '%s' "
                        "partition %s: %s",
                        topic,
                        partition_id,
                        partition_err,
                    )
                    continue
 
                if high_offset <= low_offset:
                    # No messages in this partition at all.
                    continue
 
                # Seek to the last message and poll it to read its timestamp.
                tp.offset = high_offset - 1
                consumer.assign([tp])
                msg = consumer.poll(timeout=5)
 
                if msg is None or msg.error():
                    logger.warning(
                        "Could not read latest message from topic '%s' "
                        "partition %s.",
                        topic,
                        partition_id,
                    )
                    continue
 
                ts_type, ts_ms = msg.timestamp()
                if ts_ms is None or ts_ms < 0:
                    continue
 
                msg_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                if latest_message_time is None or msg_time > latest_message_time:
                    latest_message_time = msg_time
 
            if latest_message_time is None:
                logger.warning(
                    "Kafka topic '%s' has no readable messages on any partition.",
                    topic,
                )
                continue
 
            age = now_utc - latest_message_time
            if age > threshold:
                logger.warning(
                    "Kafka topic '%s' is STALE: latest message at %s "
                    "(%.1f minutes ago, threshold is %d minutes).",
                    topic,
                    latest_message_time.isoformat(),
                    age.total_seconds() / 60,
                    FRESHNESS_THRESHOLD_MINUTES,
                )
            else:
                logger.info(
                    "Kafka topic '%s' is healthy: latest message %.1f minutes ago.",
                    topic,
                    age.total_seconds() / 60,
                )
 
    except KafkaException as kafka_err:
        logger.warning(
            "Kafka health check failed due to a Kafka error; treating Kafka "
            "as unavailable for this run. Error: %s",
            kafka_err,
        )
    except Exception as unexpected_err:  # noqa: BLE001 - defensive by design
        logger.warning(
            "Kafka health check failed unexpectedly; Kafka may be "
            "unreachable. Error: %s",
            unexpected_err,
        )
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception as close_err:  # noqa: BLE001
                logger.warning("Error closing Kafka consumer: %s", close_err)
 
 
# ---------------------------------------------------------------------------
# Task 2: check_cassandra_freshness
# ---------------------------------------------------------------------------
def check_cassandra_freshness(**context) -> None:
    """
    Verify that congestion_metrics and weather_metrics tables in Cassandra
    have recent data.
 
    For each table, queries SELECT MAX(event_time) FROM <table> and logs a
    WARNING if the most recent record is older than
    FRESHNESS_THRESHOLD_MINUTES, or if the table is empty.
 
    Connection / query errors are caught and logged so a Cassandra outage
    does not fail the DAG run.
    """
    from cassandra.cluster import Cluster
    from cassandra.cluster import NoHostAvailable
 
    cluster = None
    try:
        # connect_timeout bounds how long we wait for the initial cluster
        # connection, so an unreachable Cassandra cluster fails fast instead
        # of hanging the task.
        cluster = Cluster(CASSANDRA_HOSTS, connect_timeout=10)
        session = cluster.connect(CASSANDRA_KEYSPACE)
 
        now_utc = datetime.now(timezone.utc)
        threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
 
        for table in CASSANDRA_TABLES:
            try:
                # Table names come from a fixed internal allow-list
                # (CASSANDRA_TABLES), never from user input, so building
                # the query string here is safe.
                query = f"SELECT MAX(event_time) FROM {table};"
                result = session.execute(query)
                row = result.one()
                latest_event_time = row[0] if row else None
 
                if latest_event_time is None:
                    logger.warning(
                        "Cassandra table '%s' has no records (MAX(event_time) "
                        "is NULL).",
                        table,
                    )
                    continue
 
                # Cassandra driver returns naive datetimes in UTC by default;
                # normalize to an aware UTC datetime for safe comparison.
                if latest_event_time.tzinfo is None:
                    latest_event_time = latest_event_time.replace(
                        tzinfo=timezone.utc
                    )
 
                age = now_utc - latest_event_time
                if age > threshold:
                    logger.warning(
                        "Cassandra table '%s' is STALE: latest event_time is "
                        "%s (%.1f minutes ago, threshold is %d minutes).",
                        table,
                        latest_event_time.isoformat(),
                        age.total_seconds() / 60,
                        FRESHNESS_THRESHOLD_MINUTES,
                    )
                else:
                    logger.info(
                        "Cassandra table '%s' is healthy: latest event_time "
                        "%.1f minutes ago.",
                        table,
                        age.total_seconds() / 60,
                    )
 
            except Exception as query_err:  # noqa: BLE001 - per-table isolation
                logger.warning(
                    "Failed to query freshness for Cassandra table '%s': %s",
                    table,
                    query_err,
                )
 
    except NoHostAvailable as host_err:
        logger.warning(
            "Cassandra cluster is unreachable (no host available); "
            "skipping freshness check for this run. Error: %s",
            host_err,
        )
    except Exception as unexpected_err:  # noqa: BLE001 - defensive by design
        logger.warning(
            "Cassandra health check failed unexpectedly; Cassandra may be "
            "unreachable. Error: %s",
            unexpected_err,
        )
    finally:
        if cluster is not None:
            try:
                cluster.shutdown()
            except Exception as shutdown_err:  # noqa: BLE001
                logger.warning("Error shutting down Cassandra cluster: %s", shutdown_err)
 
 
# ---------------------------------------------------------------------------
# Task 3: check_elasticsearch
# ---------------------------------------------------------------------------
def check_elasticsearch(**context) -> None:
    """
    Verify Elasticsearch cluster health by calling /_cluster/health.
 
    Logs a WARNING if the cluster status is 'red'. Connection errors and
    timeouts are caught and logged so an Elasticsearch outage does not
    fail the DAG run.
    """
    import requests
 
    try:
        response = requests.get(
            ELASTICSEARCH_HEALTH_URL,
            timeout=ELASTICSEARCH_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        health = response.json()
 
        status = health.get("status", "unknown")
        cluster_name = health.get("cluster_name", "unknown")
 
        if status == "red":
            logger.warning(
                "Elasticsearch cluster '%s' is UNHEALTHY: status is 'red'. "
                "Full response: %s",
                cluster_name,
                health,
            )
        elif status == "yellow":
            # Not an explicit requirement, but yellow indicates degraded
            # (e.g. unassigned replica shards) and is useful operational
            # signal without being a hard failure condition.
            logger.info(
                "Elasticsearch cluster '%s' status is 'yellow' "
                "(degraded but operational).",
                cluster_name,
            )
        else:
            logger.info(
                "Elasticsearch cluster '%s' is healthy: status is '%s'.",
                cluster_name,
                status,
            )
 
    except requests.exceptions.Timeout as timeout_err:
        logger.warning(
            "Elasticsearch health check timed out after %d seconds: %s",
            ELASTICSEARCH_REQUEST_TIMEOUT_SECONDS,
            timeout_err,
        )
    except requests.exceptions.ConnectionError as conn_err:
        logger.warning(
            "Could not connect to Elasticsearch at %s: %s",
            ELASTICSEARCH_HEALTH_URL,
            conn_err,
        )
    except requests.exceptions.RequestException as req_err:
        logger.warning(
            "Elasticsearch health check request failed: %s", req_err
        )
    except ValueError as json_err:
        # response.json() failed to parse.
        logger.warning(
            "Elasticsearch health check returned a non-JSON response: %s",
            json_err,
        )
    except Exception as unexpected_err:  # noqa: BLE001 - defensive by design
        logger.warning(
            "Elasticsearch health check failed unexpectedly: %s",
            unexpected_err,
        )
 
 
# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "nileflow-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # Back off exponentially between retries (2 min, 4 min, ...) so a
    # transient outage isn't hammered with retries at a fixed interval.
    "retry_exponential_backoff": True,
}
 
with DAG(
    dag_id="nileflow_data_quality_pipeline_freshness",
    description=(
        "Checks Kafka topic activity, Cassandra table freshness, and "
        "Elasticsearch cluster health every 30 minutes."
    ),
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 6, 1, tzinfo=timezone.utc),
    catchup=False,
    # Guard against a run hanging indefinitely (e.g. a stuck connection)
    # and blocking the next scheduled run.
    dagrun_timeout=timedelta(minutes=10),
    # Prevent overlapping DAG runs: if a run is still in progress when the
    # next scheduled interval fires, the new run waits instead of starting
    # concurrently.
    max_active_runs=1,
    tags=["nileflow", "data-quality", "monitoring"],
) as dag:
 
    task_check_kafka_topics = PythonOperator(
        task_id="check_kafka_topics",
        python_callable=check_kafka_topics,
    )
 
    task_check_cassandra_freshness = PythonOperator(
        task_id="check_cassandra_freshness",
        python_callable=check_cassandra_freshness,
    )
 
    task_check_elasticsearch = PythonOperator(
        task_id="check_elasticsearch",
        python_callable=check_elasticsearch,
    )
 
    # No dependencies between tasks: all three run in parallel, as require
