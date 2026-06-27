"""
NileFlow — Milestone 4
Discord Alerter Service

Long-lived process that subscribes to the Redis `nileflow:alerts` pub/sub
channel and posts formatted congestion alert embeds to a Discord webhook.

Run:
    python -m alerts.discord_alerter
"""

import json
import logging
import os
import sys
import time

import redis
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("discord_alerter")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_CHANNEL = "nileflow:alerts"

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

SEVERITY_COLORS = {
    "critical": 0xFF0000,
    "high": 0xFF4444,
    "medium": 0xFFA500,
    "low": 0xFFFF00,
}

CORRIDOR_DISPLAY_NAMES = {
    "ring_road": "Ring Road",
    "corniche_cairo": "Corniche El Nil",
    "october_bridge": "6th of October Bridge",
    "salah_salem": "Salah Salem Road",
    "july26": "26th of July Corridor",
    "alex_corniche": "Alexandria Corniche",
}


def build_discord_embed(alert: dict) -> dict:
    corridor_id = alert.get("corridor_id", "unknown")
    corridor_name = CORRIDOR_DISPLAY_NAMES.get(corridor_id, corridor_id)
    severity = alert.get("severity", "high")
    congestion = alert.get("congestion_index", 0)
    speed = alert.get("speed_kmh", 0)
    event_time = alert.get("event_time", "N/A")

    if congestion > 0.85:
        status_emoji = "\U0001F534"
    elif congestion > 0.7:
        status_emoji = "\U0001F7E0"
    else:
        status_emoji = "\U0001F7E1"

    embed = {
        "title": f"{status_emoji} Congestion Alert — {corridor_name}",
        "color": SEVERITY_COLORS.get(severity, 0xFF4444),
        "fields": [
            {
                "name": "\U0001F6E3️ Corridor",
                "value": f"**{corridor_name}**\n`{corridor_id}`",
                "inline": True,
            },
            {
                "name": "\U0001F4CA Congestion Index",
                "value": f"**{congestion:.2f}**",
                "inline": True,
            },
            {
                "name": "\U0001F3CE️ Speed",
                "value": f"**{speed:.1f}** km/h",
                "inline": True,
            },
            {
                "name": "⏰ Event Time",
                "value": f"`{event_time}`",
                "inline": False,
            },
        ],
        "footer": {
            "text": "NileFlow Congestion Intelligence",
        },
    }

    if alert.get("message"):
        embed["description"] = alert["message"]

    return embed


def post_to_discord(embed: dict) -> None:
    if not DISCORD_WEBHOOK_URL:
        logger.warning("DISCORD_WEBHOOK_URL not set — skipping POST.")
        return

    payload = {
        "username": "NileFlow Alerts",
        "embeds": [embed],
    }

    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code == 204:
            logger.info("Alert posted to Discord successfully.")
        elif resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 1)
            logger.warning("Discord rate-limited. Retrying in %.1fs.", retry_after)
            time.sleep(retry_after)
            requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        else:
            logger.warning(
                "Discord returned status %s: %s", resp.status_code, resp.text
            )
    except requests.exceptions.RequestException as exc:
        logger.warning("Failed to POST to Discord: %s", exc)


def handle_message(message: dict) -> None:
    if message["type"] != "message":
        return

    try:
        alert = json.loads(message["data"])
    except (json.JSONDecodeError, TypeError) as exc:
        logger.warning("Invalid JSON from Redis: %s", exc)
        return

    logger.info(
        "Received alert for corridor %s (congestion=%.2f, speed=%.1f)",
        alert.get("corridor_id", "?"),
        alert.get("congestion_index", 0),
        alert.get("speed_kmh", 0),
    )

    embed = build_discord_embed(alert)
    post_to_discord(embed)


def main() -> None:
    if not DISCORD_WEBHOOK_URL:
        logger.error(
            "DISCORD_WEBHOOK_URL is not set. "
            "Set it in .env or as an environment variable."
        )
        sys.exit(1)

    logger.info("Connecting to Redis at %s:%s ...", REDIS_HOST, REDIS_PORT)

    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            logger.info("Connected to Redis. Subscribing to '%s'...", REDIS_CHANNEL)

            pubsub = r.pubsub()
            pubsub.subscribe(REDIS_CHANNEL)
            logger.info("Listening for alerts. Press Ctrl+C to stop.")

            for message in pubsub.listen():
                handle_message(message)

        except redis.exceptions.ConnectionError as exc:
            logger.warning("Redis connection lost: %s. Reconnecting in 5s...", exc)
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutting down Discord alerter.")
            break


if __name__ == "__main__":
    main()
