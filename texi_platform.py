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
from dotenv import load_dotenv

load_dotenv()

TOPIC_PREFIX = "solace/taxi_samples"
SHUTDOWN = False
MAX_RETRIES = 30

persistent_publisher = None
messaging_service = None
selected_driver = None
driver_response_event = threading.Event()
lock = threading.Lock()

class RideRequestHandler(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        global SHUTDOWN, persistent_publisher, messaging_service, selected_driver, driver_response_event
        if "quit" in message.get_destination_name():
            print("QUIT message received, shutting down.")
            SHUTDOWN = True

        payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            payload = payload.decode()

        print("\nRequest payload: {}\n".format(payload))
        request_data = json.loads(payload)
        user_id = request_data["userId"]
        current_location = request_data["currentLocation"]
        destination = request_data["destination"]

        # Reset state for new request
        selected_driver = None
        driver_response_event.clear()

        threading.Thread(target=broadcast_to_drivers, args=(user_id, current_location, destination)).start()


class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected: ", e)

    def on_reconnecting(self, e: ServiceEvent):
        print("\non_reconnecting: ", e)

    def on_service_interrupted(self, e: ServiceEvent):
        print("\non_service_interrupted: ", e)


def broadcast_to_drivers(user_id, current_location, destination):
    global persistent_publisher, messaging_service, driver_response_event, selected_driver

    topic_to_publish = f"{TOPIC_PREFIX}/DriverBroadcast"
    message_body = json.dumps({
        "userId": user_id,
        "currentLocation": current_location,
        "destination": destination,
        "timestamp": time.time()
    })

    message_builder = messaging_service.message_builder() \
        .with_application_message_id("driver_broadcast") \
        .with_property("application", "taxi_service") \
        .with_property("language", "Python")

    direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
    direct_publisher.start()

    for i in range(MAX_RETRIES):
        if driver_response_event.is_set():
            break
        outbound_message = message_builder.build(message_body)
        direct_publisher.publish(destination=Topic.of(topic_to_publish), message=outbound_message)
        print(f"Broadcast ride request to drivers, attempt {i + 1}")
        time.sleep(1)  # Broadcast interval

    direct_publisher.terminate()

    if not driver_response_event.is_set():
        send_response_to_user(user_id, "택시 기사가 잡히지 않는다")


def send_response_to_user(user_id, status, driver_info=None):
    global persistent_publisher, messaging_service

    topic_to_publish = f"{TOPIC_PREFIX}/RideRequestResponse/{user_id}"
    response_body = {
        "Timestamp": time.time(),
        "Status": status
    }
    if driver_info:
        response_body.update(driver_info)

    message_body = json.dumps(response_body)
    message_builder = messaging_service.message_builder() \
        .with_application_message_id("ride_response") \
        .with_property("application", "taxi_service") \
        .with_property("language", "Python")

    outbound_message = message_builder.build(message_body)
    if persistent_publisher.is_running:
        persistent_publisher.publish(destination=Topic.of(topic_to_publish), message=outbound_message)
        print(f"Sent response to user {user_id} with status {status}")
    else:
        print(f"Unable to send response to user {user_id}, publisher is not running")


def send_confirmation_to_driver(driver_id):
    global persistent_publisher, messaging_service
    topic_to_publish = f"{TOPIC_PREFIX}/DriverConfirmation/{driver_id}"
    message_body = json.dumps({
        "Timestamp": time.time(),
        "Message": "당신이 선택되었음"
    })

    message_builder = messaging_service.message_builder() \
        .with_application_message_id("driver_confirmation") \
        .with_property("application", "taxi_service") \
        .with_property("language", "Python")

    outbound_message = message_builder.build(message_body)
    if persistent_publisher.is_running:
        persistent_publisher.publish(destination=Topic.of(topic_to_publish), message=outbound_message)
        print(f"Sent confirmation to driver {driver_id}")
    else:
        print(f"Unable to send confirmation to driver {driver_id}, publisher is not running")


def send_failure_to_driver(driver_id):
    global persistent_publisher, messaging_service

    topic_to_publish = f"{TOPIC_PREFIX}/DriverResponse/{driver_id}"
    message_body = json.dumps({
        "Timestamp": time.time(),
        "Message": "다른 기사가 먼저 accept하였습니다"
    })

    message_builder = messaging_service.message_builder() \
        .with_application_message_id("driver_failure") \
        .with_property("application", "taxi_service") \
        .with_property("language", "Python")

    outbound_message = message_builder.build(message_body)
    if persistent_publisher.is_running:
        persistent_publisher.publish(destination=Topic.of(topic_to_publish), message=outbound_message)
        print(f"Sent failure notification to driver {driver_id}")
    else:
        print(f"Unable to send failure notification to driver {driver_id}, publisher is not running")


class DriverResponseHandler(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        global selected_driver, driver_response_event, lock
        payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            payload = payload.decode()

        driver_info = json.loads(payload)
        with lock:
            if selected_driver is None:
                # 첫 번째로 응답한 드라이버를 선택
                selected_driver = driver_info
                driver_response_event.set()

                send_response_to_user(driver_info["userId"], "택시가 잡혔습니다", driver_info)
                send_confirmation_to_driver(driver_info["driverId"])
            else:
                # 다른 드라이버에게 실패 메시지 전송
                if selected_driver["driverId"] != driver_info["driverId"]:
                    send_failure_to_driver(driver_info["driverId"])


def main():
    global SHUTDOWN, persistent_publisher, messaging_service

    topic_to_subscribe = f"{TOPIC_PREFIX}/RideRequest"
    topics_sub = [TopicSubscription.of(topic_to_subscribe)]

    solace_host = os.getenv('SOLACE_HOST')
    solace_vpn = os.getenv('SOLACE_VPN')
    solace_username = os.getenv('PLATFORM_USERNAME')
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

    try:
        print(f"Subscribed to: {topics_sub}")
        direct_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            topics_sub).build()
        direct_receiver.start()
        direct_receiver.receive_async(RideRequestHandler())

        driver_response_topics = [TopicSubscription.of(f"{TOPIC_PREFIX}/DriverResponse")]
        driver_response_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            driver_response_topics).build()
        driver_response_receiver.start()
        driver_response_receiver.receive_async(DriverResponseHandler())

        if direct_receiver.is_running:
            print("Connected and Subscribed! Ready to receive ride requests\n")
        while not SHUTDOWN:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
    except PubSubPlusClientError as exception:
        print(f'Received a PubSubPlusClientException: {exception}')
    finally:
        print('Terminating Publisher and Receiver')
        if persistent_publisher.is_running:
            persistent_publisher.terminate()
        direct_receiver.terminate()
        driver_response_receiver.terminate()
        print('Disconnecting Messaging Service')
        messaging_service.disconnect()


if __name__ == "__main__":
    main()
