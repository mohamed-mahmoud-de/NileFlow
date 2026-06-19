-- NileFlow PostgreSQL Reference Data Schema
-- This runs automatically when the postgres container starts for the first time

CREATE SCHEMA IF NOT EXISTS nileflow;
SET search_path TO nileflow, public;

-- Districts / zones in Cairo and Alexandria
CREATE TABLE districts (
    district_id   SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    name_ar       VARCHAR(100),
    city          VARCHAR(50) NOT NULL,
    geometry_json TEXT
);

-- Monitored road corridors
CREATE TABLE corridors (
    corridor_id   VARCHAR(50) PRIMARY KEY,
    name          VARCHAR(150) NOT NULL,
    name_ar       VARCHAR(150),
    city          VARCHAR(50) NOT NULL,
    district_id   INTEGER REFERENCES districts(district_id),
    start_lat     DOUBLE PRECISION NOT NULL,
    start_lon     DOUBLE PRECISION NOT NULL,
    end_lat       DOUBLE PRECISION NOT NULL,
    end_lon       DOUBLE PRECISION NOT NULL,
    distance_km   DOUBLE PRECISION,
    baseline_travel_time_sec INTEGER
);

-- Transit routes (from GTFS)
CREATE TABLE routes (
    route_id      VARCHAR(100) PRIMARY KEY,
    route_name    VARCHAR(200) NOT NULL,
    route_type    INTEGER NOT NULL,
    agency_name   VARCHAR(100),
    city          VARCHAR(50)
);

-- Transit stops (from GTFS)
CREATE TABLE stops (
    stop_id       VARCHAR(100) PRIMARY KEY,
    stop_name     VARCHAR(200) NOT NULL,
    stop_lat      DOUBLE PRECISION NOT NULL,
    stop_lon      DOUBLE PRECISION NOT NULL,
    route_id      VARCHAR(100) REFERENCES routes(route_id),
    district_id   INTEGER REFERENCES districts(district_id)
);

-- Congestion baselines (updated daily by Airflow)
CREATE TABLE congestion_baselines (
    corridor_id          VARCHAR(50) REFERENCES corridors(corridor_id),
    day_of_week          INTEGER NOT NULL,
    hour_of_day          INTEGER NOT NULL,
    avg_travel_time_sec  DOUBLE PRECISION NOT NULL,
    std_travel_time_sec  DOUBLE PRECISION,
    sample_count         INTEGER,
    updated_at           TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (corridor_id, day_of_week, hour_of_day)
);
