from google.cloud import pubsub_v1
from event_generator import run_event_generator
from dotenv import load_dotenv, dotenv_values
import os
import json
import time
import json
import argparse



def publish_message(publisher, topic_path, event):
    # Encodes data with utf-8
    data = json.dumps(event).encode('utf-8')

    # Creating a 'future' object 
    future = publisher.publish(topic_path, data)

    # returning future
    return future.result()


def run_publisher(publisher, topic_path, rate=0):

    # Setting sleep rate
    sleep_time = 1 / rate

    while True:
        event =  run_event_generator()
        print(event)

        message = publish_message(publisher, topic_path, event)
        print(message)

        time.sleep(sleep_time)

if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=1, help="Events per second")
    args = parser.parse_args()

    project_id = os.getenv('PROJECT_ID')
    topic_id = os.getenv('TOPIC_ID')


    pubsub_publisher = pubsub_v1.PublisherClient()
    topic_path = pubsub_publisher.topic_path(project_id, topic_id)

    run_publisher(pubsub_publisher, topic_path, rate=args.rate)
