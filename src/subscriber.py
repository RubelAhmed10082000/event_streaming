from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os
from dotenv import load_dotenv
import json

expected = (
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "platform",
        "country",
        "app_version",
        "event_timestamp",
        "metadata"
    )


def validate(decoded_message: dict, expected_list_of_fields: list) -> bool:
    """
    Validates that message decoded correctly

    Args - 
        decoded_message(dict): Data to be validated
        expected_list_of_fields(list): List of fields expected in decoded data 
    """

    # Checking if decoded_message is dictionary
    if isinstance(decoded_message, dict) == False:
        raise TypeError("Decoded data not of type dict")

    # Checking for missing fields in decoded_message
    for ele in expected_list_of_fields:
        if ele not in decoded_message.keys():
            raise KeyError(f"{ele} not in decoded message")
    return True
    
def decode(encoded_message: pubsub_v1.subscriber.message.Message) -> dict:
    """
    Args -
        encoded_message(bytes): Encoded mesasge sent from publisher
    Returns -
        event(dict): decoded message 
    """
    event = json.loads(encoded_message.data.decode('utf-8'))
    return event

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Acknowledges that message has been received 

    Args:
        message(dict): Message being listened to and acknowledged 
    """

    try:
        # Decodes event then validates that decoding has gone as expected
        event = decode(message)
        validate(event)
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





    