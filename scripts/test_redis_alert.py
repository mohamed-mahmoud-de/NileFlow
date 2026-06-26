import redis
import json
import time
import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CHANNEL = "nileflow:alerts"

FAKE_ALERT = {
    "corridor_id": "ring_road",
    "congestion_index": 0.85,
    "speed_kmh": 12.3,
    "event_time": "2026-06-26T19:00:00Z"
}

def run_test():
    print("Connecting to Redis...")
    try:
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        client.ping()
        print("Connected.")
    except Exception as e:
        print(f"FAIL — Could not connect to Redis: {e}")
        return

    # Subscribe first, then publish
    pubsub = client.pubsub()
    pubsub.subscribe(CHANNEL)

    print(f"Publishing fake alert to '{CHANNEL}'...")
    client.publish(CHANNEL, json.dumps(FAKE_ALERT))

    print("Waiting for message...")
    timeout = time.time() + 5  # 5 second timeout
    for message in pubsub.listen():
        if message["type"] == "message":
            try:
                data = json.loads(message["data"])
                assert data["corridor_id"] == FAKE_ALERT["corridor_id"]
                assert data["congestion_index"] == FAKE_ALERT["congestion_index"]
                assert data["speed_kmh"] == FAKE_ALERT["speed_kmh"]
                assert data["event_time"] == FAKE_ALERT["event_time"]
                print("PASS — Alert received and validated successfully.")
                print(f"  Corridor   : {data['corridor_id']}")
                print(f"  Congestion : {data['congestion_index']}")
                print(f"  Speed      : {data['speed_kmh']} km/h")
                print(f"  Time       : {data['event_time']}")
            except AssertionError:
                print("FAIL — Message received but data did not match expected values.")
            except Exception as e:
                print(f"FAIL — Error parsing message: {e}")
            break
        if time.time() > timeout:
            print("FAIL — Timed out waiting for message.")
            break

if __name__ == "__main__":
    run_test()