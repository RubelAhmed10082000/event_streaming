import apache_beam as beam
import json 
import os
from google.cloud import pubsub_v1
from src.utils import build_subscription_path
from src.subscriber import decode_data
from apache_beam.options.pipeline_options import PipelineOptions
import argparse


project_id = os.getenv('PROJECT_ID')
subscription_id = os.getenv('SUB_ID')
subscriber = pubsub_v1.SubscriberClient()

def decode_event(message: bytes) -> dict:
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
            | "Printing event" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()