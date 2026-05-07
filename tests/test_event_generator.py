from datetime import datetime 
import uuid
from src.event_generator import (
    generate_event,
    generate_bad_event,
    generate_metadata,
    EVENT_TYPES,
    PLATFORMS,
    COUNTRIES,
    APP_VERSIONS,
)

### Testing Events ###

def test_generate_event_has_required_fields():
    event = generate_event()

    required_fields = {
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "platform",
        "country",
        "app_version",
        "event_timestamp",
        "metadata",
    }

    assert required_fields.issubset(event.keys())

def test_generate_event_has_valid_values():
    event = generate_event()

    assert event["event_type"] in EVENT_TYPES
    assert event["platform"] in PLATFORMS
    assert event["country"] in COUNTRIES
    assert event["app_version"] in APP_VERSIONS
    assert isinstance(event["metadata"], dict)

def test_event_id_is_valid_uuid():
    event = generate_event()

    parsed_uuid = uuid.UUID(event["event_id"])
    assert str(parsed_uuid) == event["event_id"]

def test_event_timestamp_is_iso():
    event = generate_event()

    parsed_timestamp = datetime.fromisoformat(event["event_timestamp"])


    assert parsed_timestamp.tzinfo is not None

def test_user_and_session_id_format():
    event = generate_event()

    assert event["user_id"].startswith("user_")
    assert event["session_id"].startswith("session_")

### Testing Metadata ###

def test_track_started_metadata_shape():
    metadata = generate_metadata("track_started")

    assert "track_id" in metadata
    assert "artist_id" in metadata
    assert "latency_ms" in metadata
    assert "is_premium" in metadata

    assert metadata["track_id"].startswith("track_")
    assert metadata["artist_id"].startswith("artist_")
    assert 50 <= metadata["latency_ms"] <= 500
    assert isinstance(metadata["is_premium"], bool)

def test_track_completed_metadata_shape():
    metadata = generate_metadata("track_completed")

    assert "track_id" in metadata
    assert "duration_seconds" in metadata
    assert "completion_percent" in metadata

    assert metadata["track_id"].startswith("track_")
    assert 90 <= metadata["duration_seconds"] <= 300
    assert 70 <= metadata["completion_percent"] <= 100

def test_search_performed_metadata_shape():
    metadata = generate_metadata("search_performed")

    assert "query" in metadata
    assert "results_count" in metadata
    assert "latency_ms" in metadata

    assert isinstance(metadata["query"], str)
    assert 0 <= metadata["results_count"] <= 100
    assert 50 <= metadata["latency_ms"] <= 1000

def test_error_occurred_metadata_shape():
    metadata = generate_metadata("error_occurred")

    assert "error_code" in metadata
    assert "error_message" in metadata

    assert metadata["error_code"] in ["500", "404", "TIMEOUT", "AUTH_FAILED"]
    assert isinstance(metadata["error_message"], str)

def test_all_event_types_generate_metadata():
    for event_type in EVENT_TYPES:
        metadata = generate_metadata(event_type)

        assert isinstance(metadata, dict)
        assert len(metadata) > 0

### Testing Bad Events ###

def test_generate_bad_event():
    event = generate_bad_event()

    is_missing_event_id = "event_id" not in event
    is_missing_user_id = "user_id" not in event
    has_invalid_timestamp = event.get("event_timestamp") == "not-a-real-timestamp"
    has_unknown_event_type = event.get("event_type") == "random_fake_event"

    assert any([
        is_missing_event_id,
        is_missing_user_id,
        has_invalid_timestamp,
        has_unknown_event_type,
    ])

### Testing Many Generated Events ###

def test_generate_many_events_are_valid():
    for _ in range(1000):
        event = generate_event()

        assert "event_id" in event
        assert "user_id" in event
        assert "session_id" in event
        assert event["event_type"] in EVENT_TYPES
        assert event["platform"] in PLATFORMS
        assert event["country"] in COUNTRIES
        assert event["app_version"] in APP_VERSIONS
        assert isinstance(event["metadata"], dict)

        datetime.fromisoformat(event["event_timestamp"])