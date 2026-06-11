import apache_beam as beam
import json 
import os
from google.cloud import pubsub_v1
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
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

class ValidateFn(beam.DoFn):
    def process(self, event):
        try:
            # Checking if decoded_message is dictionary
            if not isinstance(event, dict):
                raise TypeError("Decoded data not of type dict")

            # Checking for missing fields in decoded_message
            for ele in EXPECTED_FIELDS:
                if ele not in event.keys():
                    raise KeyError(f"{ele} not in decoded message")
        
            if not isinstance(event['metadata'],dict):
                raise TypeError("metadata data not of type dict")
        
            # Checking if event_timestamp is still in datetime format
            try:
                datetime.fromisoformat(event["event_timestamp"])
            except ValueError:
                raise ValueError(f"Invalid event_timestamp: {event['event_timestamp']}")
        
            # Checking if event type field has correct fields 
            if event['event_type'] not in EVENT_TYPES:
                raise ValueError(f"{event['event_type']}: unexpected event type value")
            
            # yielding event if all validations are passed
            yield event
        
        # yielding an exception if otherwise
        except Exception as error:
            yield beam.pvalue.TaggedOutput(
                "bad_events",
                {
                    "error": str(error),
                    "event": event,
                },
            )
        


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
        validated = (
            pipeline 
            | "Reading from Pub/Sub" >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription
            ) 
            | "Decoding event" >> beam.Map(decode_event)
            | "Validating event" >> beam.ParDo(ValidateFn()).with_outputs(
                "bad_events",
                main="valid_events"
            )
        )
        
        valid_events = validated.valid_events
        bad_events = validated.bad_events

        valid_events | "Printing valid events" >> beam.Map(print)
        bad_events | "Printing bad events" >> beam.Map(print)
        

if __name__ == "__main__":
    run()