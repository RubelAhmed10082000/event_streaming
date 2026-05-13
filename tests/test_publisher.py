import json
from src.publisher import encode_data


def test_event_is_encoded():
    event = {
        "user_id": "user_123",
        "event_type": "song_played",
    }

    data = encode_data(event)

    assert isinstance(data, bytes)
    assert json.loads(data.decode('utf-8')) == event

    