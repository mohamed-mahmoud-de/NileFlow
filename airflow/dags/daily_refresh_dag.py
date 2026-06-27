"""
NileFlow — Daily Data Refresh & Baseline Recalculation DAG
=============================================================

This DAG runs once per day and performs two data-maintenance jobs:

    1. refresh_corridors        -> Upsert the CORRIDORS reference list
                                     (from config.settings) into the
                                     PostgreSQL 'corridors' table.
    2. recalculate_baselines    -> Recompute rolling congestion baselines
                                     from the last 7 days of Cassandra
                                     metrics and write them to the
                                     PostgreSQL 'congestion_baselines'
                                     table.
    3. log_summary               -> Report how many baseline rows were
                                     written in this run.

Task ordering: refresh_corridors -> recalculate_baselines -> log_summary.
This is a sequential pipeline (not parallel checks like the data-quality
DAG): baselines are keyed by corridor_id, so corridors should be up to
date before baselines are recalculated, and the summary needs the row
count produced by the baseline recalculation step. XCom is used to pass
that row count between tasks.

File location: airflow/dags/daily_refresh_dag.py
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Project configuration
# ---------------------------------------------------------------------------
# All connection details and the CORRIDORS reference list are expected to
# live in the existing NileFlow config module. Wrapped in try/except so
# that DAG parsing never fails just because config.settings is missing or
# not yet on the PYTHONPATH (e.g. local development, partial container
# build). A missing config module should never take the whole DAG out of
# the Airflow UI.
try:
    from config.settings import (
        CORRIDORS,
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB,
        POSTGRES_USER,
        POSTGRES_PASSWORD,
        CASSANDRA_HOSTS,
        CASSANDRA_KEYSPACE,
    )
except ImportError:
    logging.getLogger(__name__).warning(
        "config.settings not importable; falling back to default "
        "connection settings. Update config.settings for real "
        "deployments."
    )
    CORRIDORS = []
    POSTGRES_HOST = "postgres"
    POSTGRES_PORT = 5432
    POSTGRES_DB = "nileflow"
    POSTGRES_USER = "nileflow"
    POSTGRES_PASSWORD = "nileflow"
    CASSANDRA_HOSTS = ["cassandra"]
    CASSANDRA_KEYSPACE = "nileflow"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

# How many days of Cassandra history to use when recalculating baselines.
BASELINE_LOOKBACK_DAYS = 7

# XCom key used to pass the written-row count from recalculate_baselines
# to log_summary.
XCOM_BASELINE_ROW_COUNT_KEY = "baseline_rows_written"


# ---------------------------------------------------------------------------
# Task 1: refresh_corridors
# ---------------------------------------------------------------------------
def refresh_corridors(**context) -> None:
    """
    Upsert the CORRIDORS reference list (from config.settings) into the
    PostgreSQL 'corridors' table.

    Creates the table if it does not already exist, then upserts each
    corridor by corridor_id so that re-running this task is idempotent.

    Expects each entry in CORRIDORS to be a dict-like object with at
    least 'corridor_id' and 'name' keys; any additional recognized keys
    (e.g. 'description') are also persisted if present.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    if not CORRIDORS:
        logger.warning(
            "CORRIDORS list from config.settings is empty; nothing to "
            "upsert into the 'corridors' table."
        )
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10,
        )
        conn.autocommit = False

        with conn.cursor() as cur:
            cur.execute("SET search_path TO nileflow, public;")

            rows = [
                (
                    corridor["id"],
                    corridor.get("name", corridor["id"]),
                    corridor.get("city", "Cairo"),
                    corridor["start"]["lat"],
                    corridor["start"]["lon"],
                    corridor["end"]["lat"],
                    corridor["end"]["lon"],
                )
                for corridor in CORRIDORS
            ]

            execute_values(
                cur,
                """
                INSERT INTO corridors (corridor_id, name, city, start_lat, start_lon, end_lat, end_lon)
                VALUES %s
                ON CONFLICT (corridor_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    city = EXCLUDED.city,
                    start_lat = EXCLUDED.start_lat,
                    start_lon = EXCLUDED.start_lon,
                    end_lat = EXCLUDED.end_lat,
                    end_lon = EXCLUDED.end_lon;
                """,
                rows,
            )

        conn.commit()
        logger.info(
            "Upserted %d corridor(s) into the corridors table.", len(rows)
        )

    except Exception as err:  # noqa: BLE001 - surface and re-raise for retry
        if conn is not None:
            try:
                conn.rollback()
            except Exception as rollback_err:  # noqa: BLE001
                logger.error(
                    "Rollback failed while handling corridor refresh "
                    "error: %s",
                    rollback_err,
                )
        logger.error(
            "Failed to refresh corridors in PostgreSQL: %s", err, exc_info=True
        )
        # Unlike the data-quality DAG's health checks, a failure here means
        # downstream baseline data could reference stale/missing corridors,
        # so we re-raise to mark the task failed and trigger Airflow retries.
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_err:  # noqa: BLE001
                logger.warning(
                    "Error closing PostgreSQL connection: %s", close_err
                )


# ---------------------------------------------------------------------------
# Task 2: recalculate_baselines
# ---------------------------------------------------------------------------
def _accumulate_corridor_rows(
    rows, aggregates: dict[tuple[str, int, int], dict[str, float]]
) -> None:
    """
    Fold a batch of Cassandra rows (for a single corridor partition) into
    the running per-(corridor_id, day_of_week, hour_of_day) aggregates.
    """
    for row in rows:
        corridor_id = row.corridor_id
        event_time = row.event_time
        congestion_index = row.congestion_index
        speed_kmh = row.speed_kmh

        if congestion_index is None or speed_kmh is None or event_time is None:
            continue

        day_of_week = event_time.weekday()
        hour_of_day = event_time.hour
        key = (corridor_id, day_of_week, hour_of_day)

        travel_time_estimate = (1.0 + float(congestion_index)) * 60.0

        bucket = aggregates.setdefault(
            key,
            {"travel_time_sum": 0.0, "count": 0},
        )
        bucket["travel_time_sum"] += travel_time_estimate
        bucket["count"] += 1


def recalculate_baselines(**context) -> int:
    """
    Recalculate rolling congestion baselines from the last
    BASELINE_LOOKBACK_DAYS of Cassandra metrics, and write the results to
    the PostgreSQL 'congestion_baselines' table.

    Reads from Cassandra 'nileflow.congestion_metrics', groups by
    corridor_id and hour-of-day, computes avg(congestion_index) and
    avg(speed_kmh) for each group, then upserts the results into
    PostgreSQL.

    Returns the number of baseline rows written, which is pushed to XCom
    automatically by Airflow (PythonOperator stores the return value).
    """
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    import psycopg2
    from psycopg2.extras import execute_values

    cluster = None
    session = None
    pg_conn = None
    rows_written = 0

    try:
        # ---- Step 1: read raw metrics from Cassandra ----
        cluster = Cluster(CASSANDRA_HOSTS, connect_timeout=10)
        session = cluster.connect(CASSANDRA_KEYSPACE)

        cutoff = datetime.now(timezone.utc) - timedelta(days=BASELINE_LOOKBACK_DAYS)

        # congestion_metrics is partitioned by corridor_id, with event_time
        # as a clustering column. A query that filters on event_time alone
        # (with no partition key predicate) is an unsupported full-table
        # scan in Cassandra and would require ALLOW FILTERING, which does
        # not scale and is not safe for production use.
        #
        # Instead, we query one partition at a time using the known
        # corridor_id values from config.settings.CORRIDORS. Within each
        # partition, filtering on event_time (a clustering column) is a
        # native, efficient range scan — no ALLOW FILTERING required.
        if not CORRIDORS:
            logger.warning(
                "CORRIDORS list from config.settings is empty; cannot "
                "determine which Cassandra partitions to query. No "
                "baselines will be recalculated."
            )
            return 0  # cleanup happens in the finally block below

        query = SimpleStatement(
            """
            SELECT corridor_id, event_time, congestion_index, speed_kmh
            FROM congestion_metrics
            WHERE corridor_id = %s AND event_time >= %s
            """
        )

        aggregates: dict[tuple[str, int, int], dict[str, float]] = {}

        for corridor in CORRIDORS:
            corridor_id = corridor["id"]
            try:
                rows = session.execute(query, (corridor_id, cutoff))
            except Exception as partition_err:  # noqa: BLE001 - per-partition isolation
                # One corridor's partition failing (e.g. transient
                # coordinator issue) should not abort the whole run; log
                # and continue with the remaining corridors.
                logger.error(
                    "Failed to read Cassandra congestion_metrics for "
                    "corridor_id='%s': %s",
                    corridor_id,
                    partition_err,
                )
                continue

            _accumulate_corridor_rows(rows, aggregates)

        if not aggregates:
            logger.warning(
                "No Cassandra congestion_metrics rows found in the last "
                "%d day(s); no baselines to write.",
                BASELINE_LOOKBACK_DAYS,
            )
            return 0

        now_utc = datetime.now(timezone.utc)
        baseline_rows = [
            (
                corridor_id,
                day_of_week,
                hour_of_day,
                bucket["travel_time_sum"] / bucket["count"],
                bucket["count"],
                now_utc,
            )
            for (corridor_id, day_of_week, hour_of_day), bucket in aggregates.items()
        ]

        # ---- Step 2: write aggregated baselines to PostgreSQL ----
        pg_conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10,
        )
        pg_conn.autocommit = False

        with pg_conn.cursor() as cur:
            cur.execute("SET search_path TO nileflow, public;")

            execute_values(
                cur,
                """
                INSERT INTO congestion_baselines
                    (corridor_id, day_of_week, hour_of_day, avg_travel_time_sec, sample_count, updated_at)
                VALUES %s
                ON CONFLICT (corridor_id, day_of_week, hour_of_day) DO UPDATE SET
                    avg_travel_time_sec = EXCLUDED.avg_travel_time_sec,
                    sample_count = EXCLUDED.sample_count,
                    updated_at = EXCLUDED.updated_at;
                """,
                baseline_rows,
            )

        pg_conn.commit()
        rows_written = len(baseline_rows)
        logger.info(
            "Recalculated and wrote %d congestion baseline row(s) "
            "(corridor_id x day_of_week x hour_of_day) covering the last %d day(s).",
            rows_written,
            BASELINE_LOOKBACK_DAYS,
        )

    except Exception as err:  # noqa: BLE001 - surface and re-raise for retry
        if pg_conn is not None:
            try:
                pg_conn.rollback()
            except Exception as rollback_err:  # noqa: BLE001
                logger.error(
                    "Rollback failed while handling baseline "
                    "recalculation error: %s",
                    rollback_err,
                )
        logger.error(
            "Failed to recalculate congestion baselines: %s", err, exc_info=True
        )
        raise
    finally:
        # Close the PostgreSQL connection and shut down the Cassandra
        # session/cluster independently, so a failure closing one
        # resource never prevents the others from being released
        # (no resource leaks even on partial failure).
        if pg_conn is not None:
            try:
                pg_conn.close()
            except Exception as pg_close_err:  # noqa: BLE001
                logger.warning(
                    "Error closing PostgreSQL connection: %s", pg_close_err
                )
        if session is not None:
            try:
                session.shutdown()
            except Exception as session_close_err:  # noqa: BLE001
                logger.warning(
                    "Error shutting down Cassandra session: %s",
                    session_close_err,
                )
        if cluster is not None:
            try:
                cluster.shutdown()
            except Exception as cluster_close_err:  # noqa: BLE001
                logger.warning(
                    "Error shutting down Cassandra cluster: %s",
                    cluster_close_err,
                )

    return rows_written


# ---------------------------------------------------------------------------
# Task 3: log_summary
# ---------------------------------------------------------------------------
def log_summary(**context) -> None:
    """
    Print/log a summary of how many baseline rows were written by the
    recalculate_baselines task, pulled from XCom.
    """
    ti = context["ti"]
    rows_written = ti.xcom_pull(
        task_ids="recalculate_baselines", key="return_value"
    )

    if rows_written is None:
        logger.warning(
            "Could not retrieve baseline row count from XCom; "
            "recalculate_baselines may not have run successfully."
        )
        return

    logger.info(
        "Daily refresh summary: %d congestion baseline row(s) written "
        "for run date %s.",
        rows_written,
        context.get("ds", "unknown"),
    )
    print(f"[NileFlow] Daily refresh complete — {rows_written} baseline row(s) written.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "nileflow-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="nileflow_daily_data_refresh_baseline_recalculation",
    description=(
        "Refreshes the PostgreSQL corridors reference table and "
        "recalculates rolling congestion baselines from the last 7 days "
        "of Cassandra metrics."
    ),
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1, tzinfo=timezone.utc),
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    tags=["nileflow", "daily-refresh", "baselines"],
) as dag:

    task_refresh_corridors = PythonOperator(
        task_id="refresh_corridors",
        python_callable=refresh_corridors,
    )

    task_recalculate_baselines = PythonOperator(
        task_id="recalculate_baselines",
        python_callable=recalculate_baselines,
    )

    task_log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )

    # Sequential pipeline: corridors must be current before baselines are
    # recalculated, and the summary depends on the row count produced by
    # the baseline recalculation step (passed via XCom).
    task_refresh_corridors >> task_recalculate_baselines >> task_log_summary
