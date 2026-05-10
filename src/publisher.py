from google.cloud import pubsub_v1
from event_generator import run_event_generator

project_id = "event-streaming-495819"
topic_id = "MyTopic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def run_publisher(topic_path):
    while True:
        data = run_event_generator()
        future = publisher.publish(topic_path, data)
        print(future.result())

    print(f"Published messages to {topic_path}.")

run_publisher(topic_path)