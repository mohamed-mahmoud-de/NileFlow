"""
NileFlow — Milestone 4
Redis Pub/Sub Integration Test

Publishes a fake congestion alert to the nileflow:alerts channel, then
subscribes and verifies the message round-trips correctly.

Run:
    python scripts/test_redis_alert.py
"""

import json
import os
import sys
import threading
import time

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CHANNEL = "nileflow:alerts"

FAKE_ALERT = {
    "corridor_id": "ring_road",
    "alert_type": "congestion_spike",
    "severity": "high",
    "congestion_index": 0.85,
    "speed_kmh": 12.3,
    "message": "High congestion on ring_road: index 0.85, speed 12.3 km/h",
    "event_time": "2026-06-26T19:00:00Z",
}

TIMEOUT_SECONDS = 5


def run_test():
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    try:
        client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        client.ping()
        print("Connected.")
    except Exception as e:
        print(f"FAIL — Could not connect to Redis: {e}")
        sys.exit(1)

    pubsub = client.pubsub()
    pubsub.subscribe(CHANNEL)

    # Wait for the subscription confirmation before publishing
    for msg in pubsub.listen():
        if msg["type"] == "subscribe":
            break

    # Publish from a separate thread so listen() doesn't block it
    def publish():
        time.sleep(0.2)
        client.publish(CHANNEL, json.dumps(FAKE_ALERT))

    threading.Thread(target=publish, daemon=True).start()
    print(f"Publishing fake alert to '{CHANNEL}'...")

    print("Waiting for message...")
    deadline = time.time() + TIMEOUT_SECONDS

    for message in pubsub.listen():
        if time.time() > deadline:
            print("FAIL — Timed out waiting for message.")
            pubsub.close()
            sys.exit(1)

        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
        except (json.JSONDecodeError, TypeError) as e:
            print(f"FAIL — Error parsing message: {e}")
            pubsub.close()
            sys.exit(1)

        errors = []
        for key in FAKE_ALERT:
            if data.get(key) != FAKE_ALERT[key]:
                errors.append(
                    f"  {key}: expected {FAKE_ALERT[key]!r}, got {data.get(key)!r}"
                )

        if errors:
            print("FAIL — Message received but data did not match:")
            for err in errors:
                print(err)
            pubsub.close()
            sys.exit(1)

        print("PASS — Alert received and validated successfully.")
        print(f"  Corridor   : {data['corridor_id']}")
        print(f"  Severity   : {data['severity']}")
        print(f"  Congestion : {data['congestion_index']}")
        print(f"  Speed      : {data['speed_kmh']} km/h")
        print(f"  Message    : {data['message']}")
        print(f"  Time       : {data['event_time']}")
        pubsub.close()
        break


if __name__ == "__main__":
    run_test()
