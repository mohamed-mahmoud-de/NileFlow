# producers/weather/producer.py

import json
import logging
import time
from datetime import timezone

import requests
from confluent_kafka import Producer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    OPEN_METEO_BASE_URL,
    WEATHER_LOCATIONS,
    WEATHER_POLL_INTERVAL,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("weather_producer")

# ── Kafka client ─────────────────────────────────────────────────────────────
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def publish(topic: str, key: str, value: dict) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
    )
    producer.flush()


# ── API call ─────────────────────────────────────────────────────────────────
def fetch_weather(location: dict) -> dict | None:
    """
    Call Open-Meteo for one location.
    Returns a dict ready to publish, or None on failure.
    """
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "current": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation,"
            "wind_speed_10m,"
            "weather_code"
        ),
        # UTC so the Spark processor stores event_time correctly
        # (it parses "2026-07-06T01:00" as UTC; Africa/Cairo here shifted
        # every timestamp +3h ahead of reality)
        "timezone": "UTC",
    }

    try:
        response = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logger.error("API error for %s: %s", location["name"], e)
        return None

    current = data.get("current", {})

    return {
        "location":         location["name"],
        "latitude":         location["lat"],
        "longitude":        location["lon"],
        "event_time":       current.get("time"),          # from API, not local clock
        "temperature_c":    current.get("temperature_2m"),
        "humidity_pct":     current.get("relative_humidity_2m"),
        "precipitation_mm": current.get("precipitation"),
        "wind_speed_kmh":   current.get("wind_speed_10m"),
        "weather_code":     current.get("weather_code"),
    }


# ── Main polling loop ─────────────────────────────────────────────────────────
def run() -> None:
    logger.info("Weather producer started. Poll interval: %ds", WEATHER_POLL_INTERVAL)

    while True:
        for location in WEATHER_LOCATIONS:
            event = fetch_weather(location)

            if event is None:
                # Error already logged inside fetch_weather — keep going
                continue

            publish(
                topic=KAFKA_TOPICS["weather"],
                key=location["name"],
                value=event,
            )
            logger.info(
                "Published weather event | location=%s temp=%.1f°C code=%s",
                event["location"],
                event["temperature_c"],
                event["weather_code"],
            )

        logger.info("Cycle complete. Sleeping %ds...", WEATHER_POLL_INTERVAL)
        time.sleep(WEATHER_POLL_INTERVAL)


if __name__ == "__main__":
    run()