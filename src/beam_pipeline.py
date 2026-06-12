import apache_beam as beam
import json 
import os
from google.cloud import pubsub_v1
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
from datetime import datetime, timezone
from src.event_generator import EVENT_TYPES


project_id = os.getenv('PROJECT_ID')
subscription_id = os.getenv('SUB_ID')
subscriber = pubsub_v1.SubscriberClient()

# Fields expected for event 
EXPECTED_FIELDS_FOR_VALIDATION = (
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

# columns, data types and cardinality for valid event for big query table
VALID_EVENTS_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "platform", "type": "STRING", "mode": "NULLABLE"},
        {"name": "country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "app_version", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "metadata", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# Schema for dead-letter queue
BAD_EVENTS_SCHEMA = {
    "fields": [
        {"name": "error", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

class ValidateFn(beam.DoFn):
    def process(self, event):
        """
        Processes beam pipeline
        """
        try:
            # Checking if decoded_message is dictionary
            if not isinstance(event, dict):
                raise TypeError("Decoded data not of type dict")

            # Checking for missing fields in decoded_message
            for ele in EXPECTED_FIELDS_FOR_VALIDATION:
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

def format_valid_event(event: dict) -> dict:
    """
    formats valid event dictionary into rows that will be accepted by BigQuery 
    Args - 
        event(dict): event data received from dataflow endpoint
    Returns - 
        dict: formatted event data
    """

    return {
        "event_id": event["event_id"],
        "user_id": event["user_id"],
        "session_id": event["session_id"],
        "event_type": event["event_type"],
        "platform": event["platform"],
        "country": event["country"],
        "app_version": event["app_version"],
        "event_timestamp": event["event_timestamp"],
        "metadata": json.dumps(event["metadata"]),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    }

def format_bad_event(bad_event: dict) -> dict:
    """
    formats bad event data into rows that will be sent to dead-letter queue 
    Args - 
        event(dict): bad event data that did not meet validation criteria
    Returns - 
        dict: formatted event data
    """
    return {
        "error": bad_event["error"],
        # Need to use json.dumps as BigQuery does not accept dictionary as a field value
        "event": json.dumps(bad_event["event"]),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    }

def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_subscription",
        required=True,
        help="Pub/Sub subscription path",
    )

    parser.add_argument(
        "--valid_table",
        required=True,
        help="BigQuery table for valid events"
    )

    parser.add_argument(
        "--invalid_table",
        required=True,
        help="BigQuery table for invalid events"
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

        (
            valid_events
            | "Format valid events" >> beam.Map(format_valid_event)
            | "Write valid events" >> beam.io.WriteToBigQuery(
                table=known_args.valid_table,
                schema=VALID_EVENTS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )
        
        (
            bad_events 
            | "Format bad events" >> beam.Map(format_bad_event)
            | "Write bad events" >> beam.io.WriteToBigQuery(
                table=known_args.invalid_table,
                schema=BAD_EVENTS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )
        

if __name__ == "__main__":
    run()