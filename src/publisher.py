from google.cloud import pubsub_v1
from event_generator import run_event_generator
from dotenv import load_dotenv, dotenv_values
import os
import json
import time

load_dotenv()

project_id = os.getenv('PROJECT_ID')
topic_id = os.getenv('TOPIC_ID')


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def publish_message(topic_path):
    """
    Publishes event data to topic 
    Args:
        topic_path: str - path to project and topic
    Returns:
        published message
    """
    # Creating a infinite loop - only shuts down whens manually stopped
    while True:
        data = run_event_generator()
        print(data)
        # Encodes data with utf-8
        data = json.dumps(data).encode('utf-8')
        # Creating a 'future' object 
        future = publisher.publish(topic_path, data)
        # Printing future
        print(future.result())
        # Sleep to avoid throttling
        time.sleep(1)

publish_message(topic_path)

    

