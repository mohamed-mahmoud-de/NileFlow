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
import uuid
import logging
from datetime import datetime, timezone

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import SimpleStatement
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
        id UUID PRIMARY KEY,
        corridor_id TEXT,
        congestion_level FLOAT,
        average_speed FLOAT,
        event_time TIMESTAMP
    )
"""

INSERT_CQL = f"""
    INSERT INTO {KEYSPACE}.{TABLE} (id, corridor_id, congestion_level, average_speed, event_time)
    VALUES (%s, %s, %s, %s, %s)
"""

SELECT_CQL = f"""
    SELECT id, corridor_id, congestion_level, average_speed, event_time
    FROM {KEYSPACE}.{TABLE}
    WHERE id = %s
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
        "id": uuid.uuid4(),
        "corridor_id": "CAIRO-RING-RD-SECTION-07",
        "congestion_level": 0.78,            # 0.0 = free flow, 1.0 = gridlock
        "average_speed": 23.4,               # km/h
        "event_time": truncate_to_millisecond(datetime.now(timezone.utc)),
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
    logger.info("Inserting sample record with id=%s ...", record["id"])
    statement = SimpleStatement(INSERT_CQL)
    session.execute(
        statement,
        (
            record["id"],
            record["corridor_id"],
            record["congestion_level"],
            record["average_speed"],
            record["event_time"],
        ),
    )
    logger.info("Insert executed successfully.")


def read_record(session, record_id: uuid.UUID):
    """Reads the record back by primary key. Returns the row, or None."""
    logger.info("Reading record back with id=%s ...", record_id)
    result = session.execute(SELECT_CQL, (record_id,))
    row = result.one()
    if row is None:
        logger.error("No row found for id=%s", record_id)
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
        "id": original["id"] == retrieved.id,
        "corridor_id": original["corridor_id"] == retrieved.corridor_id,
        "congestion_level": abs(original["congestion_level"] - retrieved.congestion_level) < 1e-6,
        "average_speed": abs(original["average_speed"] - retrieved.average_speed) < 1e-6,
        "event_time": original["event_time"] == retrieved.event_time,
    }

    for field, passed in checks.items():
        logger.info("  - %-18s : %s", field, "MATCH" if passed else "MISMATCH")

    return all(checks.values())


def print_result(record: dict, retrieved) -> None:
    """Prints a clean, human-readable summary of the round-trip test."""
    print("\n" + "=" * 60)
    print("CASSANDRA STORAGE LAYER VALIDATION - RESULT")
    print("=" * 60)
    print(f"{'Field':<20}{'Inserted':<25}{'Retrieved'}")
    print("-" * 60)
    print(f"{'id':<20}{str(record['id']):<25}{str(retrieved.id)}")
    print(f"{'corridor_id':<20}{record['corridor_id']:<25}{retrieved.corridor_id}")
    print(f"{'congestion_level':<20}{record['congestion_level']:<25}{retrieved.congestion_level}")
    print(f"{'average_speed':<20}{record['average_speed']:<25}{retrieved.average_speed}")
    print(f"{'event_time':<20}{str(record['event_time']):<25}{str(retrieved.event_time)}")
    print("=" * 60 + "\n")


def main() -> int:
    cluster = None
    try:
        cluster = connect_to_cassandra()
        session = cluster.connect()
        logger.info("Connected to Cassandra cluster successfully.")

        ensure_schema(session)

        sample_record = build_sample_record()
        insert_record(session, sample_record)

        retrieved_row = read_record(session, sample_record["id"])
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
