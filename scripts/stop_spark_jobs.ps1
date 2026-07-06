# Stops all running NileFlow Spark streaming jobs.
# Usage:   .\scripts\stop_spark_jobs.ps1
docker exec nileflow-spark-master bash -c "pkill -f 'streaming/(traffic|weather|vehicle_positions)_processor.py' || echo 'No streaming jobs were running.'"
Write-Host "Done. Verify at http://localhost:8081 (Running Applications should be empty)."
