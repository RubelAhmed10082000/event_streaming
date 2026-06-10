import apache_beam as beam
import json 
import os
from google.cloud import pubsub_v1
from src.utils import build_subscription_path
from src.subscriber import decode_data
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
from typing import Any
from datetime import datetime
from src.event_generator import EVENT_TYPES


project_id = os.getenv('PROJECT_ID')
subscription_id = os.getenv('SUB_ID')
subscriber = pubsub_v1.SubscriberClient()

EXPECTED_FIELDS = (
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "platform",
        "country",
        "app_version",
        "event_timestamp",
        "metadata"
    )

def validate(decoded_message: dict, expected_list_of_fields: tuple) -> bool:
    """
    Validates that message decoded correctly

    Args - 
        decoded_message(dict): Data to be validated
        expected_list_of_fields(list): List of fields expected in decoded data 
    """

    # Checking if decoded_message is dictionary
    if not isinstance(decoded_message, dict):
        raise TypeError("Decoded data not of type dict")

    # Checking for missing fields in decoded_message
    for ele in expected_list_of_fields:
        if ele not in decoded_message.keys():
            raise KeyError(f"{ele} not in decoded message")
    
    if not isinstance(decoded_message['metadata'],dict):
        raise TypeError("metadata data not of type dict")
    
    # Checking if event_timestamp is still in datetime format
    try:
        datetime.fromisoformat(decoded_message["event_timestamp"])
    except ValueError:
        raise ValueError(f"Invalid event_timestamp: {decoded_message['event_timestamp']}")
    
    # Checking if event type field has correct fields 
    if decoded_message['event_type'] not in EVENT_TYPES:
        raise ValueError(f"{decoded_message['event_type']}: unexpected event type value")
    
    return decoded_message


def decode_event(message: bytes) -> dict:
    """
    Decodes event data recieved from publisher
    Args -
        messages(bytes): utf-8 encoded event data
    Returns -
        dict[str|Any]: decoded event data
    """
    return json.loads(message.decode("utf-8"))


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_subscription",
        required=True,
        help="Pub/Sub subscription path",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True,
    )


    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline 
            | "Reading from Pub/Sub" >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription
            ) 
            | "Decoding event" >> beam.Map(decode_event)
            | "Validating event" >> beam.Map(validate, EXPECTED_FIELDS)
            | "Printing event" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()