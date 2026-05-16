from google.cloud import pubsub_v1
from event_generator import run_event_generator
from dotenv import load_dotenv
import os
import json
import time
import json
import argparse

def publish_message(publisher: str, topic_path: str, event: dict):
    """
    Publishes message to pubsub topic

    Args:
        publisher(google.cloud.pubsub_v1.PublisherClient): Identifies the publisher 
        topic_path(str): Identifies topic that message will be published to
        event(dict): Event that will be published 

    Returns:
        future.result(str): string objet that has been encoded 
    """
    # Encodes data with utf-8
    data = encode_data(event)

    # Creating a 'future' object 
    future = publisher.publish(topic_path, data)

    # returning future
    return future.result()

def encode_data(data: dict) -> bytes:
    """
    Encodes data into utf-8
    """
    return json.dumps(data).encode('utf-8')


def run_publisher(publisher: str, topic_path: str, rate=0):
    """
    Runs publish_message() function indefinetly 

    Args:
        publisher(google.cloud.pubsub_v1.PublisherClient): Identifies the publisher 
        topic_path(str): Identifies topic that message will be published to
        rate(int): Modifies sleep time between function calls 

    """

    # Setting sleep rate
    sleep_time = 1 / rate

    # Publish event until shutdown 
    while True:
        # Generates event
        event =  run_event_generator()
        print(event)

        # Publishes message
        message = publish_message(publisher, topic_path, event)
        print(message)

        # Sleeps
        time.sleep(sleep_time)

if __name__ == "__main__":
    load_dotenv()

    # Create "rate" argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=1, help="Events per second")
    args = parser.parse_args()

    # Setting up project and topic variables
    project_id = os.getenv('PROJECT_ID')
    topic_id = os.getenv('TOPIC_ID')

    # Sets up publisher and topic path 
    pubsub_publisher = pubsub_v1.PublisherClient()
    topic_path = pubsub_publisher.topic_path(project_id, topic_id)

    # Runs publisher
    run_publisher(pubsub_publisher, topic_path, rate=args.rate)
