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

#for n in range(1, 10):
    #data_str = f"Message number {n}"
    # Data must be a bytestring
    #data = data_str.encode("utf-8")
    # When you publish a message, the client returns a future.
    #future = publisher.publish(topic_path, data)
    #print(future.result())

def publish_message(topic_path):
    while True:
        data = run_event_generator()
        print(data)
        data = json.dumps(data).encode('utf-8')
        future = publisher.publish(topic_path, data)
        print(future.result())
        time.sleep(1)

publish_message(topic_path)

    

