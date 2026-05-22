from src.subscriber import decode_data, validate, callback, run_subscriber, build_subscription_path, expected
from src.publisher import encode_data
import pytest

class MockPubSubMessage:
    def __init__(self, data):
        self.data = data

### Testing Decoding Function
def test_decode():
    mock_event = {
        "event_id": "event_123",
        "user_id": "user_456",
        "session_id": "session_789",
        "event_type": "track_started",
        "platform": "ios",
        "country": "GB",
        "app_version": "1.2.0",
        "event_timestamp": "2026-05-20T19:30:00+00:00",
        "metadata": {
            "track_id": "track_123",
            "artist_id": "artist_456",
            "latency_ms": 120,
            "is_premium": True
        }
    }

    mock_encoded_message = encode_data(mock_event)

    mock_message = MockPubSubMessage(mock_encoded_message)

    mock_decoded_message = decode_data(mock_message)

    assert mock_decoded_message == mock_event

# Testing Validation Function
def test_validate_rejects_non_dict_message():
    mock_list = (
    "user_id", "user_123",
    "event_type", "song_played"
)

    with pytest.raises(TypeError, match = 'dict'):
        validate(mock_list, expected)
    
def test_validate_rejects_non_dict_metadata():
    mock_event_metadata_is_list = {
        "event_id": "event_123",
        "user_id": "user_456",
        "session_id": "session_789",
        "event_type": "track_started",
        "platform": "ios",
        "country": "GB",
        "app_version": "1.2.0",
        "event_timestamp": "2026-05-20T19:30:00+00:00",
        "metadata": [
            {
                "track_id": "track_123",
                "artist_id": "artist_456",
                "latency_ms": 120,
                "is_premium": True
            }
        ]
    }
    
    with pytest.raises(TypeError, match='metadata'):
        validate(mock_event_metadata_is_list, expected)

test_validate_rejects_non_dict_message()
test_validate_rejects_non_dict_metadata()




