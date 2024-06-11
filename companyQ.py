from datetime import datetime
import os
import time
import json
import sqlite3
from solace.messaging.messaging_service import MessagingService, RetryStrategy, ServiceInterruptionListener, \
    ReconnectionAttemptListener, ReconnectionListener, ServiceEvent
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from dotenv import load_dotenv

load_dotenv()

TOPIC_PREFIX = "solace/taxi_samples_yun"
SHUTDOWN = False
messaging_service = None  # 전역 변수 선언


class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected: ", e)

    def on_reconnecting(self, e: ServiceEvent):
        print("\non_reconnecting: ", e)

    def on_service_interrupted(self, e: ServiceEvent):
        print("\non_service_interrupted: ", e)


class UserResponseHandler(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        global messaging_service

        payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            payload = payload.decode()

        print(f"\nUser response received: {payload}\n")
        try:
            response_data = json.loads(payload)
            user_id = response_data["userId"]
            payment_response = response_data["response"]
            print(f"{user_id}의 response는 {payment_response} 입니다.")
            update_payment_state(user_id, payment_response)

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Failed to parse the message payload: {e}")


def update_payment_state(user_id, payment_response):
    connection = sqlite3.connect('taxi_service.db')
    cursor = connection.cursor()

    payment_state = "true" if payment_response == "y" else "false"

    cursor.execute('''
        UPDATE Drive
        SET paymentState = ?
        WHERE userId = ? AND paymentState IS NULL
    ''', (payment_state, user_id))

    connection.commit()
    connection.close()


def send_payment_request_to_user(user_id, cost):
    global messaging_service

    topic_to_publish = f"{TOPIC_PREFIX}/CompanyQPaymentRequest/{user_id}"
    message_body = json.dumps({
        "userId": user_id,
        "cost": cost,
        "message": f"{cost}가 나왔습니다. 결제하시겠습니까?(y/n)",
        "timestamp": datetime.now().isoformat()
    })

    message_builder = messaging_service.message_builder() \
        .with_application_message_id("payment_request") \
        .with_property("application", "taxi_service") \
        .with_property("language", "Python")

    direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
    direct_publisher.start()
    outbound_message = message_builder.build(message_body)
    direct_publisher.publish(destination=Topic.of(topic_to_publish), message=outbound_message)
    print(f"Sent payment request to user for user ID: {user_id}")
    direct_publisher.terminate()

class PaymentRequestHandler(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        global messaging_service
        payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            payload = payload.decode()

        print(f"\nPayment request received: {payload}\n")
        try:
            request_data = json.loads(payload)
            user_id = request_data["userId"]
            cost = request_data["cost"]
            card_num = request_data["card_num"]
            send_payment_request_to_user(user_id, cost)

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Failed to parse the message payload: {e}")


def main():
    global SHUTDOWN, messaging_service

    solace_host = os.getenv('SOLACE_HOST')
    solace_vpn = os.getenv('SOLACE_VPN')
    solace_username = os.getenv('COMPANYQ_USERNAME')
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

    payment_request_topic = f"{TOPIC_PREFIX}/PaymentRequestToCompanyQ"
    user_response_topic = f"{TOPIC_PREFIX}/CompanyQPaymentRequestResponse"

    try:
        print(f"Subscribed to: {payment_request_topic} and {user_response_topic}")
        payment_request_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            [TopicSubscription.of(payment_request_topic)]).build()
        payment_request_receiver.start()
        payment_request_receiver.receive_async(PaymentRequestHandler())

        user_response_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            [TopicSubscription.of(user_response_topic)]).build()
        user_response_receiver.start()
        user_response_receiver.receive_async(UserResponseHandler())

        if payment_request_receiver.is_running and user_response_receiver.is_running:
            print("Connected and Subscribed! Ready to receive payment requests and user responses\n")

        while not SHUTDOWN:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
    except PubSubPlusClientError as exception:
        print(f'Received a PubSubPlusClientException: {exception}')
    finally:
        print('Terminating Receivers')
        payment_request_receiver.terminate()
        user_response_receiver.terminate()
        print('Disconnecting Messaging Service')
        messaging_service.disconnect()


if __name__ == "__main__":
    main()
