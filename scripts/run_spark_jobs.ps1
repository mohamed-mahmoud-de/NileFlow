# =============================================================================
# NileFlow — Submit all 3 Spark streaming jobs (Windows PowerShell)
#
# Usage:   .\scripts\run_spark_jobs.ps1
#
# Each job runs detached inside the spark-master container with:
#   - spark.cores.max=2  → the 6-core worker can host all 3 apps at once
#   - small heaps (512m driver / 768m executor): these streams handle a few
#     events/sec; 1g heaps x 6 JVMs crashed Docker on a 16 GB host
#   - a persistent checkpoint dir on the shared spark-checkpoints volume
#   - stdout/stderr redirected to spark/logs/<job>.log (visible on host)
#
# First run downloads connector JARs (~1-2 min); they are cached afterwards.
# Watch progress:   Get-Content spark\logs\traffic_processor.log -Tail 20 -Wait
# Spark master UI:  http://localhost:8081  (3 apps should show as RUNNING)
# =============================================================================

$packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
$jobs = @("traffic_processor", "weather_processor", "vehicle_positions_processor")

foreach ($job in $jobs) {
    Write-Host "Submitting $job ..." -ForegroundColor Cyan
    docker exec -d -e CHECKPOINT_LOCATION=/opt/nileflow/checkpoints/$job nileflow-spark-master bash -c "mkdir -p /opt/nileflow/spark/logs && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --driver-memory 512m --executor-memory 768m --packages $packages /opt/nileflow/spark/streaming/$job.py > /opt/nileflow/spark/logs/$job.log 2>&1"
}

Write-Host ""
Write-Host "All 3 jobs submitted." -ForegroundColor Green
Write-Host "  Logs:     spark\logs\*.log"
Write-Host "  Spark UI: http://localhost:8081"
Write-Host "  Stop all: .\scripts\stop_spark_jobs.ps1"
