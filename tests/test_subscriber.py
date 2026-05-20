from src.subscriber import decode_data, validate, callback, run_subscriber, build_subscription_path
from src.publisher import encode_data

class MockPubSubMessage:
    def __init__(self, data):
        self.data = data



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