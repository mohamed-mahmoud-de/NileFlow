from dotenv import load_dotenv
load_dotenv()
"""
NileFlow - Vehicle Positions Producer
Simulates real-time GPS pings from vehicles traveling along monitored
corridors and publishes to the Kafka 'vehicle_position_events' topic.
"""

import json
import logging
import math
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

import redis as redis_lib

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    VEHICLE_POSITION_INTERVAL,
    CORRIDORS,
    REDIS_HOST,
    REDIS_PORT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("vehicle_positions_producer")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

redis_client = None
for _redis_host in [REDIS_HOST, "localhost"]:
    try:
        redis_client = redis_lib.Redis(host=_redis_host, port=REDIS_PORT, decode_responses=True)
        redis_client.ping()
        logger.info("Connected to Redis at %s:%s", _redis_host, REDIS_PORT)
        break
    except Exception:
        redis_client = None

if not redis_client:
    logger.warning("Redis unavailable — skipping position cache")

VEHICLES_PER_CORRIDOR = 3


def publish(topic: str, key: str, value: dict):
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
    )


def estimate_distance_km(start: dict, end: dict) -> float:
    """Haversine distance between two lat/lon points."""
    R = 6371.0
    lat1, lon1 = math.radians(start["lat"]), math.radians(start["lon"])
    lat2, lon2 = math.radians(end["lat"]), math.radians(end["lon"])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def compute_heading(start_lat, start_lon, end_lat, end_lon) -> float:
    """Bearing in degrees (0=North, 90=East, 180=South, 270=West)."""
    dlon = math.radians(end_lon - start_lon)
    lat1 = math.radians(start_lat)
    lat2 = math.radians(end_lat)
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    bearing = math.degrees(math.atan2(x, y))
    return (bearing + 360) % 360


def init_vehicles() -> list:
    """Create simulated vehicles spread across all corridors."""
    vehicles = []
    for corridor in CORRIDORS:
        distance_km = estimate_distance_km(corridor["start"], corridor["end"])
        for i in range(1, VEHICLES_PER_CORRIDOR + 1):
            vehicles.append({
                "vehicle_id": f"{corridor['id']}_v{i}",
                "corridor_id": corridor["id"],
                "corridor_name": corridor["name"],
                "city": corridor["city"],
                "start": corridor["start"],
                "end": corridor["end"],
                "distance_km": distance_km,
                "progress": random.uniform(0.0, 1.0),
                "speed_kmh": random.uniform(15.0, 50.0),
            })
    return vehicles


def update_vehicle(vehicle: dict, interval_sec: int):
    """Advance vehicle along the corridor by one tick."""
    vehicle["speed_kmh"] = max(5.0, vehicle["speed_kmh"] + random.uniform(-5.0, 5.0))

    distance_per_tick = (vehicle["speed_kmh"] / 3600) * interval_sec
    progress_increment = distance_per_tick / vehicle["distance_km"] if vehicle["distance_km"] > 0 else 0

    vehicle["progress"] += progress_increment

    if vehicle["progress"] >= 1.0:
        vehicle["progress"] = 0.0
        vehicle["speed_kmh"] = random.uniform(15.0, 50.0)


def build_event(vehicle: dict) -> dict:
    """Build a Kafka event from current vehicle state."""
    start = vehicle["start"]
    end = vehicle["end"]
    p = vehicle["progress"]

    lat = start["lat"] + p * (end["lat"] - start["lat"])
    lon = start["lon"] + p * (end["lon"] - start["lon"])

    # GPS jitter
    lat += random.uniform(-0.0005, 0.0005)
    lon += random.uniform(-0.0005, 0.0005)

    heading = compute_heading(start["lat"], start["lon"], end["lat"], end["lon"])

    return {
        "vehicle_id": vehicle["vehicle_id"],
        "corridor_id": vehicle["corridor_id"],
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "speed_kmh": round(vehicle["speed_kmh"], 1),
        "heading": round(heading, 1),
        "progress_pct": round(vehicle["progress"] * 100, 1),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main():
    logger.info("Starting vehicle positions producer...")
    topic = KAFKA_TOPICS["vehicle_positions"]
    vehicles = init_vehicles()
    logger.info(f"Simulating {len(vehicles)} vehicles across {len(CORRIDORS)} corridors.")

    while True:
        all_positions = []
        for vehicle in vehicles:
            update_vehicle(vehicle, VEHICLE_POSITION_INTERVAL)
            event = build_event(vehicle)
            publish(topic, key=vehicle["vehicle_id"], value=event)
            all_positions.append(event)

        producer.flush()

        if redis_client:
            try:
                redis_client.set(
                    "nileflow:vehicle_positions",
                    json.dumps(all_positions),
                    ex=60,
                )
            except Exception:
                pass

        logger.info(
            f"Published {len(vehicles)} vehicle positions. "
            f"Sample: {vehicles[0]['vehicle_id']} at {vehicles[0]['progress']*100:.1f}%"
        )
        time.sleep(VEHICLE_POSITION_INTERVAL)


if __name__ == "__main__":
    main()
