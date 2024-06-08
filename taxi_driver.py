import os
import time
import json
import threading
from solace.messaging.messaging_service import MessagingService, RetryStrategy, ServiceInterruptionListener, \
    ReconnectionAttemptListener, ReconnectionListener, ServiceEvent
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from dotenv import load_dotenv

load_dotenv()

TOPIC_PREFIX = "solace/taxi_samples"
SHUTDOWN = False

def driver_main(driver_id, wait_time):
    solace_host = os.getenv('SOLACE_HOST')
    solace_vpn = os.getenv('SOLACE_VPN')
    solace_username = os.getenv('DRIVER_USERNAME')
    solace_password = os.getenv('PASSWORD')

    if not solace_host or not solace_vpn or not solace_username or not solace_password:
        raise ValueError("Missing one or more required environment variables.")

    broker_props = {
        "solace.messaging.transport.host": solace_host,
        "solace.messaging.service.vpn-name": solace_vpn,
        "solace.messaging.authentication.scheme.basic.username": solace_username,
        "solace.messaging.authentication.scheme.basic.password": solace_password
    }

    messaging_service = MessagingService.builder().from_properties(broker_props) \
        .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3)) \
        .build()

    class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
        def on_reconnected(self, e: ServiceEvent):
            print("\non_reconnected: ", e)

        def on_reconnecting(self, e: ServiceEvent):
            print("\non_reconnecting: ", e)

        def on_service_interrupted(self, e: ServiceEvent):
            print("\non_service_interrupted: ", e)

    event_handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(event_handler)
    messaging_service.add_reconnection_attempt_listener(event_handler)
    messaging_service.add_service_interruption_listener(event_handler)
    messaging_service.connect()

    direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
    direct_publisher.start()

    topic_to_subscribe = f"{TOPIC_PREFIX}/DriverBroadcast"
    topics_sub = [TopicSubscription.of(topic_to_subscribe)]

    class RideBroadcastHandler(MessageHandler):
        def on_message(self, message: 'InboundMessage'):
            payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                payload = payload.decode()

            print(f"\nDriver {driver_id} received broadcast: {payload}\n")
            time.sleep(wait_time)  # wait for the specified time before accepting
            request_data = json.loads(payload)
            user_id = request_data["userId"]

            # Send acceptance response to platform
            response_topic = f"{TOPIC_PREFIX}/DriverResponse"
            response_body = json.dumps({
                "driverId": driver_id,
                "userId": user_id,
                "carNumber": "1234",
                "currentLocation": "10 Downing St",
                "estimatedTime": "5 minutes"
            })
            message_builder = messaging_service.message_builder() \
                .with_application_message_id("driver_response") \
                .with_property("application", "taxi_service") \
                .with_property("language", "Python")

            outbound_message = message_builder.build(response_body)
            direct_publisher.publish(destination=Topic.of(response_topic), message=outbound_message)
            print(f"Driver {driver_id} sent acceptance response")

    try:
        print(f"Driver {driver_id} subscribed to: {topics_sub}")
        direct_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            topics_sub).build()
        direct_receiver.start()
        direct_receiver.receive_async(RideBroadcastHandler())
        if direct_receiver.is_running:
            print(f"Driver {driver_id} connected and ready to receive broadcasts\n")
        while not SHUTDOWN:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
    except PubSubPlusClientError as exception:
        print(f'Received a PubSubPlusClientException: {exception}')
    finally:
        print('Terminating Publisher and Receiver')
        direct_publisher.terminate()
        direct_receiver.terminate()
        print('Disconnecting Messaging Service')
        messaging_service.disconnect()

if __name__ == "__main__":
    # Start three driver threads with different wait times
    threading.Thread(target=driver_main, args=("driver1", 3)).start()
    threading.Thread(target=driver_main, args=("driver2", 4)).start()
    threading.Thread(target=driver_main, args=("driver3", 10)).start()
