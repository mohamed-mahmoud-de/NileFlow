"""
NileFlow — Central configuration.
All settings are loaded from environment variables with sensible defaults for local Docker development.
"""

import os

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = {
    "traffic": "traffic_events",
    "weather": "weather_events",
    "vehicle_positions": "vehicle_position_events",
}

# --- Data Source APIs ---
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "")
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# --- Polling Intervals (seconds) ---
TRAFFIC_POLL_INTERVAL = int(os.getenv("TRAFFIC_POLL_INTERVAL", "120"))
WEATHER_POLL_INTERVAL = int(os.getenv("WEATHER_POLL_INTERVAL", "300"))
VEHICLE_POSITION_INTERVAL = int(os.getenv("VEHICLE_POSITION_INTERVAL", "10"))

# --- Monitored Corridors ---
CORRIDORS = [
    {"id": "ring_road", "name": "Ring Road", "city": "Cairo",
     "start": {"lat": 30.0561, "lon": 31.3467}, "end": {"lat": 30.0131, "lon": 31.2089}},
    {"id": "corniche_cairo", "name": "Corniche El Nil", "city": "Cairo",
     "start": {"lat": 30.0459, "lon": 31.2243}, "end": {"lat": 30.0029, "lon": 31.2297}},
    {"id": "october_bridge", "name": "6th of October Bridge", "city": "Cairo",
     "start": {"lat": 30.0554, "lon": 31.2235}, "end": {"lat": 30.0434, "lon": 31.2015}},
    {"id": "salah_salem", "name": "Salah Salem Road", "city": "Cairo",
     "start": {"lat": 30.0724, "lon": 31.2834}, "end": {"lat": 30.0281, "lon": 31.2611}},
    {"id": "july26", "name": "26th of July Corridor", "city": "Cairo",
     "start": {"lat": 30.0609, "lon": 31.2003}, "end": {"lat": 30.0764, "lon": 31.1177}},
    {"id": "alex_corniche", "name": "Alexandria Corniche", "city": "Alexandria",
     "start": {"lat": 31.2135, "lon": 29.8854}, "end": {"lat": 31.2017, "lon": 29.9533}},
]

# --- Weather Locations ---
WEATHER_LOCATIONS = [
    {"name": "Cairo", "lat": 30.0444, "lon": 31.2357},
    {"name": "Alexandria", "lat": 31.2001, "lon": 29.9187},
]

# --- PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "nileflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "nileflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "nileflow_dev")

# --- Cassandra ---
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "nileflow")

# --- Redis ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# --- Discord ---
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

# --- Spark ---
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
