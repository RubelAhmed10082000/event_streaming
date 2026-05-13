from google.cloud import pubsub_v1
from event_generator import run_event_generator
from dotenv import load_dotenv, dotenv_values
import os
import json
import time
import json
import argparse

load_dotenv()

project_id = os.getenv('PROJECT_ID')
topic_id = os.getenv('TOPIC_ID')


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def publish_message(publisher, topic_path, event):
    # Encodes data with utf-8
    data = json.dumps(event).encode('utf-8')

    # Creating a 'future' object 
    future = publisher.publish(topic_path, data)

    # returning future
    return future.result()


def run_publisher(publisher, topic_path, rate=0):

    sleep_time = 1 / rate

    while True:
        event =  run_event_generator()
        print(event)

        message = publish_message(publisher, topic_path, event)
        print(message)

        time.sleep(sleep_time)

    
