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
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.config.solace_properties.message_properties import APPLICATION_MESSAGE_ID
from dotenv import load_dotenv

load_dotenv()

TOPIC_PREFIX = "solace/taxi_samples"
SHUTDOWN = False
RESPONSE_RECEIVED = False

persistent_publisher = None  # 전역 변수 선언
messaging_service = None  # 전역 변수 선언


class UserResponseHandler(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        global RESPONSE_RECEIVED, SHUTDOWN
        payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            payload = payload.decode()

        print("\nResponse payload: {}\n".format(payload))
        RESPONSE_RECEIVED = True
        SHUTDOWN = True


class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected: ", e)

    def on_reconnecting(self, e: ServiceEvent):
        print("\non_reconnecting: ", e)

    def on_service_interrupted(self, e: ServiceEvent):
        print("\non_service_interrupted: ", e)


def send_ride_request(user_id, current_location, destination):
    global persistent_publisher, messaging_service

    topic_to_publish = f"{TOPIC_PREFIX}/RideRequest"
    message_body = json.dumps({
        "userId": user_id,
        "currentLocation": current_location,
        "destination": destination,
        "timestamp": time.time()
    })

    message_builder = messaging_service.message_builder() \
        .with_application_message_id("ride_request") \
        .with_property("application", "taxi_service") \
        .with_property("language", "Python")

    additional_properties = {APPLICATION_MESSAGE_ID: 'ride_request_1'}
    outbound_message = message_builder.build(message_body, additional_message_properties=additional_properties)
    persistent_publisher.publish(destination=Topic.of(topic_to_publish), message=outbound_message)
    print("Sent ride request")


def main():
    global SHUTDOWN, persistent_publisher, messaging_service

    user_id = "user123"
    current_location = "123 Main St"
    destination = "456 Elm St"

    solace_host = os.getenv('SOLACE_HOST')
    solace_vpn = os.getenv('SOLACE_VPN')
    solace_username = os.getenv('USER_USERNAME')
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

    event_handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(event_handler)
    messaging_service.add_reconnection_attempt_listener(event_handler)
    messaging_service.add_service_interruption_listener(event_handler)
    messaging_service.connect()

    persistent_publisher = messaging_service.create_persistent_message_publisher_builder().build()
    persistent_publisher.start()

    topic_to_subscribe = f"{TOPIC_PREFIX}/RideRequestResponse/{user_id}"
    topics_sub = [TopicSubscription.of(topic_to_subscribe)]

    try:
        print(f"Subscribed to: {topics_sub}")
        direct_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            topics_sub).build()
        direct_receiver.start()
        direct_receiver.receive_async(UserResponseHandler())
        if direct_receiver.is_running:
            print("Connected and Subscribed! Ready to receive responses\n")

        send_ride_request(user_id, current_location, destination)  # 요청을 한 번만 보냅니다.

        while not SHUTDOWN:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
    except PubSubPlusClientError as exception:
        print(f'Received a PubSubPlusClientException: {exception}')
    finally:
        print('Terminating Publisher and Receiver')
        persistent_publisher.terminate()
        direct_receiver.terminate()
        print('Disconnecting Messaging Service')
        messaging_service.disconnect()


if __name__ == "__main__":
    main()
