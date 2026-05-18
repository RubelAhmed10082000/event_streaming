from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os
from dotenv import load_dotenv
import json




def decode(data: pubsub_v1.subscriber.message.Message) -> dict:
    """
    Args -
        data(bytes): Message sent from publisher
    Returns -
        event(dict): decoded event data 
    """
    event = json.loads(data.data.decode('utf-8'))
    return event

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Acknowledges that message has been received 

    Args:
        message(bytes): Message being listened to and acknowledged 
    """

    try:
        event = decode(message)
        validate_event = validate(event)
        # Print statement acknowledging message
        print(f"Received {message}.")
        # acking message
        message.ack()
    except Exception as e:
        print(f"{message.message_id} experieneced error: {e}")
        message.nack()


def future_pull(subscription_path, timeout=10):
    """
    Requests message from pub/sub server

    Args:
        subscription_path (str): Path to subscriber 
        timeout(int): How long subscriber listens to publisher
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

    future_pull(subscription_path, timeout=10)





    