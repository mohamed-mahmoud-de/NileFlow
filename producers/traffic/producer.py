from dotenv import load_dotenv
load_dotenv()
"""
NileFlow - Traffic Producer
Polls TomTom Routing API for each monitored corridor and publishes
congestion data to the Kafka 'traffic_events' topic.
"""

import json
import logging
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    TOMTOM_API_KEY,
    TRAFFIC_POLL_INTERVAL,
    CORRIDORS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("traffic_producer")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def publish(topic: str, key: str, value: dict):
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
    )
    producer.flush()


def get_traffic_data(corridor: dict) -> dict | None:
    """Call TomTom Routing API for one corridor and compute congestion metrics."""
    start = corridor["start"]
    end = corridor["end"]
    url = (
        f"https://api.tomtom.com/routing/1/calculateRoute/"
        f"{start['lat']},{start['lon']}:{end['lat']},{end['lon']}/json"
    )
    params = {
        "key": TOMTOM_API_KEY,
        "traffic": "true",
        "travelMode": "car",
        "computeTravelTimeFor": "all",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        summary = data["routes"][0]["summary"]
        travel_time = summary["travelTimeInSeconds"]
        free_flow_time = summary["noTrafficTravelTimeInSeconds"]
        distance_m = summary["lengthInMeters"]

        # Clamp at 0: off-peak routes can beat the free-flow baseline,
        # which would otherwise produce negative congestion values
        congestion_index = max(0.0, (travel_time - free_flow_time) / free_flow_time)
        speed_kmh = (distance_m / 1000) / (travel_time / 3600)

        return {
            "corridor_id": corridor["id"],
            "corridor_name": corridor["name"],
            "city": corridor["city"],
            "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "travel_time_sec": travel_time,
            "free_flow_sec": free_flow_time,
            "congestion_index": round(congestion_index, 2),
            "speed_kmh": round(speed_kmh, 1),
            "distance_km": round(distance_m / 1000, 1),
            "is_anomaly": congestion_index > 1.5,
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"API error for corridor {corridor['id']}: {e}")
        return None
    except (KeyError, IndexError, ZeroDivisionError) as e:
        logger.error(f"Unexpected response format for corridor {corridor['id']}: {e}")
        return None


def main():
    logger.info("Starting traffic producer...")
    topic = KAFKA_TOPICS["traffic"]

    while True:
        for corridor in CORRIDORS:
            event = get_traffic_data(corridor)
            if event:
                publish(topic, key=corridor["id"], value=event)
                logger.info(
                    f"Published {corridor['id']}: "
                    f"congestion_index={event['congestion_index']}"
                )
        time.sleep(TRAFFIC_POLL_INTERVAL)


if __name__ == "__main__":
    main()