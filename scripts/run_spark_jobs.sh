#!/usr/bin/env bash
# =============================================================================
# NileFlow — Submit all 3 Spark streaming jobs (macOS / Linux / Git Bash)
#
# Usage:   ./scripts/run_spark_jobs.sh
#
# See run_spark_jobs.ps1 for the Windows PowerShell equivalent.
# =============================================================================
set -euo pipefail

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
JOBS="traffic_processor weather_processor vehicle_positions_processor"

for job in $JOBS; do
    echo "Submitting $job ..."
    docker exec -d -e CHECKPOINT_LOCATION=/opt/nileflow/checkpoints/$job nileflow-spark-master bash -c "mkdir -p /opt/nileflow/spark/logs && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --driver-memory 512m --executor-memory 768m --packages $PACKAGES /opt/nileflow/spark/streaming/$job.py > /opt/nileflow/spark/logs/$job.log 2>&1"
done

echo ""
echo "All 3 jobs submitted."
echo "  Logs:     spark/logs/*.log"
echo "  Spark UI: http://localhost:8081"
echo "  Stop all: ./scripts/stop_spark_jobs.sh"
