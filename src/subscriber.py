from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os
from dotenv import load_dotenv


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Acknowledges that message has been received 

    Args:
        message(bytes): Message being listened to and acknowledged 
    """
    # Print statement acknowledging message
    print(f"Received {message}.")
    # acking message
    message.ack()

def future_pull(subscription_path, timeout=10):
    """
    Requests message from pub/sub server

    Args:
        subscription_path (str): Path to subscriber 
        timeout: How long subscriber listens to publisher
    """

    # reads message from subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Pulls message until timeout
    with subscriber:
        try:
            # If timeout not set then message pull until failure
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel() 
            streaming_pull_future.result()  

if __name__ == "__main__":

    load_dotenv()

    project_id = os.getenv('PROJECT_ID')
    subscription_id = os.getenv('SUB_ID')

    subscriber = pubsub_v1.SubscriberClient()

    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    future_pull(subscription_path, timeout=0)





    