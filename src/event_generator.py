import uuid
import random
import time
from datetime import datetime, timezone
import json
import argparse


# Creating a list of event types to be returned in JSON format
EVENT_TYPES = [
    "app_opened",
    "track_started",
    "track_completed",
    "search_performed",
    "playlist_created",
    "ad_loaded",
    "error_occurred"
]

# Creating a list of potential platforms, app version and countries for metadata
PLATFORMS = ["ios", "android", "web", "desktop"]

COUNTRIES = ["GB", "US", "DE", "FR", "BR", "IN", "JP"]

APP_VERSIONS = ["1.0.0", "1.1.0", "1.2.0", "2.0.0"]


def generate_event():
    """
    Generates fake event data where values of each event is randomized

    Returns:
        dict: Dictionary of events with randomized values
    """
    # Randomly choosing event type to generate from list
    event_type = random.choice(EVENT_TYPES)

    # For each event creating dictionary keys refer to attribute of event
    # And values are randomized
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
    """
    Generates meta data for each event
    Args: 
        event_type(dict): The category of event listed in EVENT_TYPES
    Returns:
        dict: metadata for specific event type, with randomized values
    """
    try:
        if event_type == "app_opened":
            return {
                "is_premium": random.choice([True, False]),
                "latency_ms": random.randint(50, 1000),
                "timestamp_open": datetime.now(timezone.utc).isoformat()
            }
        
        # JSON metadata payload for 'track_started' event
        if event_type == "track_started":
            return {
                "track_id": f"track_{random.randint(1, 10000)}",
                "artist_id": f"artist_{random.randint(1, 500)}",
                "latency_ms": random.randint(50, 500),
                "is_premium": random.choice([True, False])
            }
        
        # JSON metadata payload for 'track_completed' event
        elif event_type == "track_completed":
            return {
                "track_id": f"track_{random.randint(1, 10000)}",
                "duration_seconds": random.randint(90, 300),
                "completion_percent": random.randint(70, 100)
            }
        
        # JSON metadata payload for 'search_performed' event
        elif event_type == "search_performed":
            return {
                "query": random.choice(["drake", "gym playlist", "lofi", "afrobeats", "taylor swift"]),
                "results_count": random.randint(0, 100),
                "latency_ms": random.randint(50, 1000)
            }

        # JSON metadata payload for 'playlist_created' event
        elif event_type == "playlist_created":
            return {
                "playlist_id": f"playlist_{random.randint(1, 10000)}",
                "number_of_tracks": random.randint(1, 100)
            }

        # JSON metadata payload for 'ad_loaded' event
        elif event_type == "ad_loaded":
            return {
                "ad_id": f"ad_{random.randint(1, 5000)}",
                "load_time_ms": random.randint(100, 1500)
            }

        # JSON metadata payload for 'error_occured' event
        elif event_type == "error_occurred":
            return {
                "error_code": random.choice(["500", "404", "TIMEOUT", "AUTH_FAILED"]),
                "error_message": random.choice([
                    "Internal server error",
                    "Resource not found",
                    "Request timed out",
                    "Authentication failed"
                ])
            }

    # Raising value error if argument inputted not found in EVENT_TYPES
    except:
        ValueError("Argument must be found in EVENT_TYPES")

def generate_bad_event():
    """
    Generates bad events with missing or useless data values
    Returns:
        dict: modified event dict with bad events
    """
    bad_event_type = random.choice ([
        "missing_event_id",
        "missing_user_id",
        "invalid_timestamp",
        "unknown_event_type"
    ])

    # Generates event
    event = generate_event()

    # pops event_id and replaces it with None
    if bad_event_type == "missing_event_id":
        event.pop("event_id", None)
    
    # pops user_id and replaces it with None
    elif bad_event_type == "missing_user_id":
        event.pop("user_id", None)
    
    # Replaces event_timestamp with bad_timestamp
    elif bad_event_type == "invalid_timestamp":
        event['event_timetamp'] = "bad_timestamp"

    # Replaces event_type with bad_event
    elif bad_event_type == "unknown_event_type":
        event["event_type"] = "bad_event"

    return event 

if __name__ == "__main__":
    # Creating argparser to allow for control of sleep time 
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type =int, default=1, help= "Event frequency")
    args = parser.parse_args()

    # Sleep time percentage of --rate argument 
    sleep_time =  1 / args.rate
    while True:
        # Sets 5% of bad event being generated
        if random.random() < 0.05:
            event = generate_bad_event()
        else:
        # Otherwise healthy event is generated
            event = generate_event()
            
        # Ouputs json 
        print(json.dumps(event))
        time.sleep(1)