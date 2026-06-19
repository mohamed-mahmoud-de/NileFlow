-- Run this after Cassandra is fully up:
--   docker exec -it nileflow-cassandra cqlsh -f /init/create_keyspace.cql

CREATE KEYSPACE IF NOT EXISTS nileflow
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE nileflow;

-- Time-series congestion metrics (high write throughput)
CREATE TABLE IF NOT EXISTS congestion_metrics (
    corridor_id     TEXT,
    event_time      TIMESTAMP,
    travel_time_sec INT,
    free_flow_sec   INT,
    congestion_index DOUBLE,
    speed_kmh       DOUBLE,
    is_anomaly      BOOLEAN,
    PRIMARY KEY (corridor_id, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 2592000;  -- 30 days retention

-- Weather observations
CREATE TABLE IF NOT EXISTS weather_metrics (
    location        TEXT,
    event_time      TIMESTAMP,
    temperature_c   DOUBLE,
    humidity_pct    DOUBLE,
    precipitation_mm DOUBLE,
    wind_speed_kmh  DOUBLE,
    weather_code    INT,
    PRIMARY KEY (location, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 2592000;
