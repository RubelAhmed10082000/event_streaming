
from google.cloud import pubsub_v1

def build_subscription_path(subscriber: pubsub_v1.SubscriberClient, project_id: str, 
                            subscription_id: str) -> str:
    """
    Builds Pub/Sub subscription path
    """
    return subscriber.subscription_path(project_id, subscription_id)