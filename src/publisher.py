from google.cloud import pubsub_v1
from event_generator import run_event_generator
from dotenv import load_dotenv
import os
import json
import time
import json
import argparse
import logging 
import time

# Creating logger object

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def publish_message(publisher, topic_path, event):
    """
    Publishes message to pubsub topic

    Args:
        publisher(google.cloud.pubsub_v1.PublisherClient): Identifies the publisher 
        topic_path(str): Identifies topic that message will be published to
        event(dict): Event that will be published 

    Returns:
        message(str): string objet that has been encoded 
    """
    
    # Starting timer to log latency
    start_time = time.perf_counter()
    
    try:
        # Encodes data with utf-8
        data = encode_data(event)

        # Creating a 'future' object 
        future = publisher.publish(topic_path, data)

        message = future.result()

        latency_ms = (time.perf_counter() - start_time) * 1000

        # Logging latency, messagee, event_type and payload size for every message published
        logger.info(
            "event_published",
            extra={
                "event_type": event.get("event_type"),
                "message_id": message,
                "payload_size": len(data),
                "latency_ms": round(latency_ms, 2),

            },
        )

        # return message
        return message
    
    # Logging event type if event type is not None
    except Exception:
        logger.exception(
            "event failed to publish",
            extra = {
                "event_type": event.get("event_type") if isinstance(event,dict) else None,
            },
        )

def encode_data(data):
    """
    Encodes data into utf-8
    """
    return json.dumps(data).encode('utf-8')


def run_publisher(publisher, topic_path, rate=0):
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
