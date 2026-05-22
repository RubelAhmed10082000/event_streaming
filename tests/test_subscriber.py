from src.subscriber import decode_data, validate, callback, run_subscriber, build_subscription_path, expected
from src.publisher import encode_data
import pytest
import random

class MockPubSubMessage:
    def __init__(self, data, message_id="mock-message-id"):
        self.data = data
        self.message_id = message_id
        self.acked = False
        self.nacked = False

    def ack(self):
        self.acked = True

    def nack(self):
        self.nacked = True

### Testing Decoding Function ###
def test_decode():
    mock_event_before_decoding = {
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

    mock_encoded_message = encode_data(mock_event_before_decoding)

    mock_message = MockPubSubMessage(mock_encoded_message)

    mock_decoded_message = decode_data(mock_message)

    assert mock_decoded_message == mock_event_before_decoding

### Testing Validation Function ###
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

def test_validate_detects_missing_fields():
    mock_event = {
        "event_id": "event_123",
        "user_id": "user_456",
        "session_id": "session_789",
        "event_type": "track_started",
        "platform": "ios",
        "country": "GB",
        "app_version": "1.2.0",
        "event_timestamp": "2026-05-20T19:30:00+00:00",
        "metadata": 
            {
                "track_id": "track_123",
                "artist_id": "artist_456",
                "latency_ms": 120,
                "is_premium": True
            }
        
        }
    
    mock_event.pop(random.choice(expected), None)

    with pytest.raises(KeyError):
        validate(mock_event, expected)
    
def test_validate_detects_timestamp_datatype():
    mock_event_bad_timestamp = {
        "event_id": "event_123",
        "user_id": "user_456",
        "session_id": "session_789",
        "event_type": "track_started",
        "platform": "ios",
        "country": "GB",
        "app_version": "1.2.0",
        "event_timestamp": "2026--20T19:30:00+00:00",
        "metadata": 
            {
                "track_id": "track_123",
                "artist_id": "artist_456",
                "latency_ms": 120,
                "is_premium": True
            }
        
        }
    
    with pytest.raises(ValueError, match='Invalid event_timestamp'):
        validate(mock_event_bad_timestamp, expected)

def test_validate_detects_unexpected_event_type():
    mock_event_bad_event_type = {
        "event_id": "event_123",
        "user_id": "user_456",
        "session_id": "session_789",
        "event_type": "bad_event_type",
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

    with pytest.raises(ValueError, match='unexpected'):
        validate(mock_event_bad_event_type, expected)

def test_validat_passes_real_event():
    mock_event_no_errors = {
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

    assert validate(mock_event_no_errors, expected) == True

### Testing Callback Function ###

def test_callback_acks_valid_message():
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

    encoded_data = encode_data(mock_event)
    mock_message = MockPubSubMessage(encoded_data)

    callback(mock_message)

    assert mock_message.acked is True
    assert mock_message.nacked is False


def test_callback_acks_message_with_missing_field():
    mock_event = {
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

    encoded_data = encode_data(mock_event)
    mock_message = MockPubSubMessage(encoded_data)

    callback(mock_message)

    assert mock_message.acked is True
    assert mock_message.nacked is False


def test_callback_acks_message_with_invalid_timestamp():
    mock_event = {
        "event_id": "event_123",
        "user_id": "user_456",
        "session_id": "session_789",
        "event_type": "track_started",
        "platform": "ios",
        "country": "GB",
        "app_version": "1.2.0",
        "event_timestamp": "bad_timestamp",
        "metadata": {
            "track_id": "track_123",
            "artist_id": "artist_456",
            "latency_ms": 120,
            "is_premium": True
        }
    }

    encoded_data = encode_data(mock_event)
    mock_message = MockPubSubMessage(encoded_data)

    callback(mock_message)

    assert mock_message.acked is True
    assert mock_message.nacked is False


def test_callback_acks_message_with_unknown_event_type():
    mock_event = {
        "event_id": "event_123",
        "user_id": "user_456",
        "session_id": "session_789",
        "event_type": "bad_event",
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

    encoded_data = encode_data(mock_event)
    mock_message = MockPubSubMessage(encoded_data)

    callback(mock_message)

    assert mock_message.acked is True
    assert mock_message.nacked is False


def test_callback_acks_message_with_metadata_as_list():
    mock_event = {
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

    encoded_data = encode_data(mock_event)
    mock_message = MockPubSubMessage(encoded_data)

    callback(mock_message)

    assert mock_message.acked is True
    assert mock_message.nacked is False


def test_callback_acks_invalid_json_message():
    mock_message = MockPubSubMessage(b"{bad json")

    callback(mock_message)

    assert mock_message.acked is True
    assert mock_message.nacked is False


def test_callback_nacks_unexpected_error(monkeypatch):
    def broken_decode(message):
        raise RuntimeError("Unexpected failure")

    monkeypatch.setattr("src.subscriber.decode_data", broken_decode)

    mock_message = MockPubSubMessage(b"{}")

    callback(mock_message)

    assert mock_message.acked is False
    assert mock_message.nacked is True


