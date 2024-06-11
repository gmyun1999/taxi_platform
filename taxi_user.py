import os
import time
import json
import sqlite3
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
from database import create_tables

load_dotenv()

TOPIC_PREFIX = "solace/taxi_samples_yun"
SHUTDOWN = False

SHUTDOWN_RIDE_RESPONSE = False
persistent_publisher = None  # 전역 변수 선언
messaging_service = None  # 전역 변수 선언


class PaymentRequestHandler(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        global SHUTDOWN
        if SHUTDOWN:
            return

        payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            payload = payload.decode()

        print("\n결제 요청 받음: {}\n".format(payload))
        try:
            request_data = json.loads(payload)
            user_id = request_data.get("userId")
            cost = request_data.get("cost")

            if not user_id or not cost:
                print("Invalid payment request data")
                return

            print(f"{cost}가 나왔습니다. 결제하시겠습니까?(y/n)")
            response = input().strip().lower()

            if response in ['y', 'n']:
                send_payment_response(user_id, response)
                SHUTDOWN = True  # 결제 완료 후 종료
            else:
                print("Invalid input, please enter 'y' or 'n'.")

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Failed to parse the message payload: {e}")

class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected: ", e)

    def on_reconnecting(self, e: ServiceEvent):
        print("\non_reconnecting: ", e)

    def on_service_interrupted(self, e: ServiceEvent):
        print("\non_service_interrupted: ", e)


def send_ride_request(user_id, current_location, destination):  # 문제없음
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


def send_payment_response(user_id, response):  # 문제없음
    global persistent_publisher, messaging_service, SHUTDOWN

    topic_to_publish = f"{TOPIC_PREFIX}/CompanyQPaymentRequestResponse/{user_id}"
    message_body = json.dumps({
        "userId": user_id,
        "response": response,
        "timestamp": time.time()
    })

    message_builder = messaging_service.message_builder() \
        .with_application_message_id("payment_response") \
        .with_property("application", "taxi_service") \
        .with_property("language", "Python")

    outbound_message = message_builder.build(message_body)
    persistent_publisher.publish(destination=Topic.of(topic_to_publish), message=outbound_message)
    print(f"Sent payment response: {response}")
    SHUTDOWN = True


def save_user_to_db(user_id, card_num):
    connection = sqlite3.connect('taxi_service.db')
    cursor = connection.cursor()

    cursor.execute('''
        INSERT OR IGNORE INTO User (userId, card_num)
        VALUES (?, ?)
    ''', (user_id, card_num))

    connection.commit()
    connection.close()


class UserResponseHandler(MessageHandler):  # 문제없음
    def on_message(self, message: 'InboundMessage'):
        global SHUTDOWN
        payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            payload = payload.decode()
        print("플랫폼으로부터 응답옴")
        print("\nResponse payload: {}\n".format(payload))


def main():
    global SHUTDOWN, persistent_publisher, messaging_service

    create_tables()

    user_id = input("Enter user ID: ")
    card_num = input("Enter card number: ")
    current_location = "123 Main St"
    destination = "456 Elm St"

    save_user_to_db(user_id, card_num)

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

    ride_response_topic = f"{TOPIC_PREFIX}/RideRequestResponse/{user_id}"
    payment_request_topic = f"{TOPIC_PREFIX}/CompanyQPaymentRequest/{user_id}"

    try:
        print(f"Subscribed to: {ride_response_topic} and {payment_request_topic}")

        # Receiver for ride response topic
        ride_response_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            [TopicSubscription.of(ride_response_topic)]
        ).build()
        ride_response_receiver.start()
        ride_response_receiver.receive_async(UserResponseHandler())

        # Receiver for payment request topic
        payment_request_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            [TopicSubscription.of(payment_request_topic)]
        ).build()
        payment_request_receiver.start()
        payment_request_receiver.receive_async(PaymentRequestHandler())

        send_ride_request(user_id, current_location, destination)  # 요청을 한 번만 보냅니다.

        while not SHUTDOWN:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
        SHUTDOWN = True
    except PubSubPlusClientError as exception:
        print(f'Received a PubSubPlusClientException: {exception}')
        SHUTDOWN = True
    finally:
        print('Terminating Publisher and Receiver')
        payment_request_receiver.terminate()
        print('Disconnecting Messaging Service')
        messaging_service.disconnect()


if __name__ == "__main__":
    main()
