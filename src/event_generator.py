import uuid
import random
import time
from datetime import datetime, timezone
import json
import argparse


EVENT_TYPES = [
    "app_opened",
    "track_started",
    "track_completed",
    "search_performed",
    "playlist_created",
    "ad_loaded",
    "error_ocurred"
]

PLATFORMS = ["ios", "android", "web", "desktop"]

COUNTRIES = ["GB", "US", "DE", "FR", "BR", "IN", "JP"]

APP_VERSIONS = ["1.0.0", "1.1.0", "1.2.0", "2.0.0"]


def generate_event():
    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 1000)}",
        "session_id": f"session_{random.randint(1, 5000)}",
        "event_type": event_type,
        "platform": random.choice(PLATFORMS),
        "country": random.choice(COUNTRIES),
        "app_version": random.choice(APP_VERSIONS),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": generate_metadata(event_type)
    }

    return event


def generate_metadata(event_type):
    if event_type == "track_started":
        return {
            "track_id": f"track_{random.randint(1, 10000)}",
            "artist_id": f"artist_{random.randint(1, 500)}",
            "latency_ms": random.randint(50, 500),
            "is_premium": random.choice([True, False])
        }

    elif event_type == "track_completed":
        return {
            "track_id": f"track_{random.randint(1, 10000)}",
            "duration_seconds": random.randint(90, 300),
            "completion_percent": random.randint(70, 100)
        }

    elif event_type == "search_performed":
        return {
            "query": random.choice(["drake", "gym playlist", "lofi", "afrobeats", "taylor swift"]),
            "results_count": random.randint(0, 100),
            "latency_ms": random.randint(50, 1000)
        }

    elif event_type == "playlist_created":
        return {
            "playlist_id": f"playlist_{random.randint(1, 10000)}",
            "number_of_tracks": random.randint(1, 100)
        }

    elif event_type == "ad_loaded":
        return {
            "ad_id": f"ad_{random.randint(1, 5000)}",
            "load_time_ms": random.randint(100, 1500)
        }

    elif event_type == "error_ocurred":
        return {
            "error_code": random.choice(["500", "404", "TIMEOUT", "AUTH_FAILED"]),
            "error_message": random.choice([
                "Internal server error",
                "Resource not found",
                "Request timed out",
                "Authentication failed"
            ])
        }

    else:
        return {
            "device_memory_mb": random.choice([2048, 4096, 8192]),
            "battery_percent": random.randint(1, 100)
        }

def generate_bad_event():
    bad_event_type = random.choice ([
        "missing_event_id",
        "missing_user_id",
        "invalid_timestamp",
        "unknown_event_type"
    ])

    event = generate_event()

    if bad_event_type == "mising_event_id":
        event.pop("event_id", None)
    
    elif bad_event_type == "missing_user_id":
        event.pop("user_id", None)

    elif bad_event_type == "invalid_timestamp":
        event['event_timetamp'] = "fake-timestamp"

    elif bad_event_type == "unknown_event_type":
        event["event_type"] = "fake_event"

    return event 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type =int, default=1, help= "Event frequency")
    args = parser.parse_args()

    sleep_time =  1 / args.rate
    while True:
        if random.random() < 0.05:
            event = generate_bad_event()
        else:
            event = generate_event()

        print(json.dumps(event))
        time.sleep(1)