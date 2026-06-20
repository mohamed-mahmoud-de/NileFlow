"""
NileFlow - Smart City Data Engineering Project
Module: Cassandra Storage Layer Validation Test
Branch: feature/cassandra-test

Purpose:
    Prove the Cassandra storage layer works end-to-end by writing a sample
    congestion_metrics record and reading it back. Performs schema bootstrap
    (keyspace + table), insert, read, field-by-field verification, and a
    clean console report. Exits 0 on success, 1 on any failure.
"""

import sys
import logging
from datetime import datetime, timezone

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import InvalidRequest, OperationTimedOut, ReadTimeout, WriteTimeout

# --------------------------------------------------------------------------- #
# Connection / schema configuration
# --------------------------------------------------------------------------- #
CASSANDRA_HOST = "127.0.0.1"
CASSANDRA_PORT = 9042
CONNECT_TIMEOUT_SECONDS = 10
KEYSPACE = "nileflow"
TABLE = "congestion_metrics"

CREATE_KEYSPACE_CQL = f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
"""

CREATE_TABLE_CQL = f"""
    CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
        corridor_id     TEXT,
        event_time      TIMESTAMP,
        travel_time_sec INT,
        free_flow_sec   INT,
        congestion_index DOUBLE,
        speed_kmh       DOUBLE,
        is_anomaly      BOOLEAN,
        PRIMARY KEY (corridor_id, event_time)
    ) WITH CLUSTERING ORDER BY (event_time DESC)
      AND default_time_to_live = 2592000
"""

INSERT_CQL = f"""
    INSERT INTO {KEYSPACE}.{TABLE}
        (corridor_id, event_time, travel_time_sec, free_flow_sec, congestion_index, speed_kmh, is_anomaly)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

SELECT_CQL = f"""
    SELECT corridor_id, event_time, travel_time_sec, free_flow_sec, congestion_index, speed_kmh, is_anomaly
    FROM {KEYSPACE}.{TABLE}
    WHERE corridor_id = %s AND event_time = %s
"""

# --------------------------------------------------------------------------- #
# Logging setup
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nileflow.cassandra_storage_test")


def truncate_to_millisecond(dt: datetime) -> datetime:
    """
    Cassandra's TIMESTAMP type only stores millisecond precision, but
    Python's datetime.now() carries microsecond precision. Truncating
    before insert ensures the in-memory value matches exactly what
    Cassandra returns on read, so equality checks are reliable.
    """
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def build_sample_record() -> dict:
    """Builds one realistic congestion_metrics record for the test run."""
    return {
        "corridor_id": "CAIRO-RING-RD-SECTION-07",
        "event_time": truncate_to_millisecond(datetime.now(timezone.utc)),
        "travel_time_sec": 540,
        "free_flow_sec": 300,
        "congestion_index": 0.78,
        "speed_kmh": 23.4,
        "is_anomaly": False,
    }


def connect_to_cassandra() -> Cluster:
    """Builds the Cluster object pointed at the local Cassandra node."""
    logger.info("Preparing connection to Cassandra at %s:%s ...", CASSANDRA_HOST, CASSANDRA_PORT)
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
    )
    return cluster


def ensure_schema(session) -> None:
    """Creates the keyspace and table if they do not already exist."""
    logger.info("Ensuring keyspace '%s' exists ...", KEYSPACE)
    session.execute(CREATE_KEYSPACE_CQL)
    logger.info("Keyspace '%s' is ready.", KEYSPACE)

    session.set_keyspace(KEYSPACE)

    logger.info("Ensuring table '%s.%s' exists ...", KEYSPACE, TABLE)
    session.execute(CREATE_TABLE_CQL)
    logger.info("Table '%s.%s' is ready.", KEYSPACE, TABLE)


def insert_record(session, record: dict) -> None:
    """Inserts the sample congestion record using a parameterized statement."""
    logger.info("Inserting sample record for corridor=%s ...", record["corridor_id"])
    session.execute(
        INSERT_CQL,
        (
            record["corridor_id"],
            record["event_time"],
            record["travel_time_sec"],
            record["free_flow_sec"],
            record["congestion_index"],
            record["speed_kmh"],
            record["is_anomaly"],
        ),
    )
    logger.info("Insert executed successfully.")


def read_record(session, corridor_id: str, event_time: datetime):
    """Reads the record back by composite primary key. Returns the row, or None."""
    logger.info("Reading record back for corridor=%s at %s ...", corridor_id, event_time)
    result = session.execute(SELECT_CQL, (corridor_id, event_time))
    row = result.one()
    if row is None:
        logger.error("No row found for corridor=%s at %s", corridor_id, event_time)
        return None
    logger.info("Record read back successfully.")
    return row


def verify_record(original: dict, retrieved) -> bool:
    """
    Compares every field of the inserted record against the retrieved row.
    Returns True only if all fields match.
    """
    logger.info("Verifying inserted record matches retrieved record ...")

    checks = {
        "corridor_id": original["corridor_id"] == retrieved.corridor_id,
        "event_time": original["event_time"] == retrieved.event_time,
        "travel_time_sec": original["travel_time_sec"] == retrieved.travel_time_sec,
        "free_flow_sec": original["free_flow_sec"] == retrieved.free_flow_sec,
        "congestion_index": abs(original["congestion_index"] - retrieved.congestion_index) < 1e-6,
        "speed_kmh": abs(original["speed_kmh"] - retrieved.speed_kmh) < 1e-6,
        "is_anomaly": original["is_anomaly"] == retrieved.is_anomaly,
    }

    for field, passed in checks.items():
        logger.info("  - %-18s : %s", field, "MATCH" if passed else "MISMATCH")

    return all(checks.values())


def print_result(record: dict, retrieved) -> None:
    """Prints a clean, human-readable summary of the round-trip test."""
    print("\n" + "=" * 70)
    print("CASSANDRA STORAGE LAYER VALIDATION - RESULT")
    print("=" * 70)
    print(f"{'Field':<20}{'Inserted':<30}{'Retrieved'}")
    print("-" * 70)
    print(f"{'corridor_id':<20}{record['corridor_id']:<30}{retrieved.corridor_id}")
    print(f"{'event_time':<20}{str(record['event_time']):<30}{str(retrieved.event_time)}")
    print(f"{'travel_time_sec':<20}{record['travel_time_sec']:<30}{retrieved.travel_time_sec}")
    print(f"{'free_flow_sec':<20}{record['free_flow_sec']:<30}{retrieved.free_flow_sec}")
    print(f"{'congestion_index':<20}{record['congestion_index']:<30}{retrieved.congestion_index}")
    print(f"{'speed_kmh':<20}{record['speed_kmh']:<30}{retrieved.speed_kmh}")
    print(f"{'is_anomaly':<20}{str(record['is_anomaly']):<30}{str(retrieved.is_anomaly)}")
    print("=" * 70 + "\n")


def main() -> int:
    cluster = None
    try:
        cluster = connect_to_cassandra()
        session = cluster.connect()
        logger.info("Connected to Cassandra cluster successfully.")

        ensure_schema(session)

        sample_record = build_sample_record()
        insert_record(session, sample_record)

        retrieved_row = read_record(session, sample_record["corridor_id"], sample_record["event_time"])
        if retrieved_row is None:
            logger.error("FAILURE: Inserted record could not be read back.")
            return 1

        print_result(sample_record, retrieved_row)

        if verify_record(sample_record, retrieved_row):
            logger.info("SUCCESS: Storage layer validated. Inserted and retrieved records match.")
            return 0

        logger.error("FAILURE: Retrieved record does not match inserted record.")
        return 1

    except NoHostAvailable as e:
        logger.error("FAILURE: Could not connect to Cassandra cluster: %s", e)
        return 1
    except (InvalidRequest, OperationTimedOut, ReadTimeout, WriteTimeout) as e:
        logger.error("FAILURE: Cassandra operation failed: %s", e)
        return 1
    except Exception as e:
        logger.exception("FAILURE: Unexpected error during storage validation: %s", e)
        return 1
    finally:
        if cluster is not None:
            cluster.shutdown()
            logger.info("Cassandra cluster connection closed.")


if __name__ == "__main__":
    exit_code = main()
    if exit_code == 0:
        print("\n[PASSED] Cassandra storage layer is working correctly.\n")
    else:
        print("\n[FAILED] Cassandra storage layer validation failed.\n")
    sys.exit(exit_code)
