import os
import time
import json
import threading
import sqlite3
from datetime import datetime
from solace.messaging.config.solace_properties.message_properties import APPLICATION_MESSAGE_ID
from solace.messaging.messaging_service import MessagingService, RetryStrategy, ServiceInterruptionListener, \
    ReconnectionAttemptListener, ReconnectionListener, ServiceEvent
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.resources.queue import Queue
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from dotenv import load_dotenv
from database import create_tables

load_dotenv()

TOPIC_PREFIX = "solace/taxi_samples_yun"
SHUTDOWN = False
MAX_RETRIES = 30

persistent_publisher = None
messaging_service = None
selected_driver = None
driver_response_event = threading.Event()
lock = threading.Lock()

# 전역 수신기 정의
queue_receivers = {}

class RideRequestHandler(MessageHandler):
    def on_message(self, message: InboundMessage):
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

        # 메시지 확인
        if 'queue_rideRequest' in queue_receivers:
            queue_receivers['queue_rideRequest'].ack(message)


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


def send_response_to_user(user_id, driverInfo):
    global persistent_publisher, messaging_service

    topic_to_publish = f"{TOPIC_PREFIX}/RideRequestResponse/{user_id}"
    response_body = {
        "driverInfo": driverInfo
    }

    message_body = json.dumps(response_body)
    message_builder = messaging_service.message_builder() \
        .with_application_message_id("ride_response") \
        .with_property("application", "taxi_service") \
        .with_property("language", "Python")

    outbound_message = message_builder.build(message_body)
    if persistent_publisher.is_running:
        persistent_publisher.publish(destination=Topic.of(topic_to_publish), message=outbound_message)
        print()
        print(f"Sent response to user {user_id} with status {driverInfo}")
        print()
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
        print()
    else:
        print(f"Unable to send confirmation to driver {driver_id}, publisher is not running")


def send_failure_to_driver(driver_id):
    global persistent_publisher, messaging_service

    topic_to_publish = f"{TOPIC_PREFIX}/DriverConfirmation/{driver_id}"
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
        print()
    else:
        print(f"Unable to send failure notification to driver {driver_id}, publisher is not running")


class DriverResponseHandler(MessageHandler):
    def on_message(self, message: InboundMessage):
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

                send_response_to_user(user_id=driver_info["userId"], driverInfo=driver_info)
                send_confirmation_to_driver(driver_info["driverId"])
            else:
                # 다른 드라이버에게 실패 메시지 전송
                if selected_driver["driverId"] != driver_info["driverId"]:
                    send_failure_to_driver(driver_info["driverId"])

        # 메시지 확인
        if 'queue_DriverResponse' in queue_receivers:
            queue_receivers['queue_DriverResponse'].ack(message)


class PickupDropoffHandler(MessageHandler):
    def on_message(self, message: InboundMessage):
        global persistent_publisher
        payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            payload = payload.decode()

        try:
            data = json.loads(payload)
            print()
            print(f"Received message on {message.get_destination_name()}: {data}")

            if "Pickup" in message.get_destination_name():
                print(f"Pickup message received: {data}")
                print()
                save_pickup(data)

            elif "Dropoff" in message.get_destination_name():
                print(f"Dropoff message received: {data}")
                print()
                save_dropoff_and_calculate_cost(data)

        except json.JSONDecodeError as e:
            print(f"Failed to parse the message payload: {e}")
        except KeyError as e:
            print(f"Missing expected key in the message payload: {e}")

        # 메시지 확인
        if 'queue_Pickup' in queue_receivers:
            queue_receivers['queue_Pickup'].ack(message)
        if 'queue_Dropoff' in queue_receivers:
            queue_receivers['queue_Dropoff'].ack(message)


def save_pickup(data):
    connection = sqlite3.connect('taxi_service.db')
    cursor = connection.cursor()

    user_id = data["userId"]
    rider_id = data["driverId"]
    pickup_time = data["timestamp"]

    cursor.execute('''
        INSERT INTO Drive (riderId, userId, pickupTime)
        VALUES (?, ?, ?)
    ''', (rider_id, user_id, pickup_time))

    connection.commit()
    connection.close()


def save_dropoff_and_calculate_cost(data):
    connection = sqlite3.connect('taxi_service.db')
    cursor = connection.cursor()

    user_id = data["userId"]
    rider_id = data["driverId"]
    dropoff_time_str = data["timestamp"]

    # Dropoff time을 datetime 객체로 변환
    dropoff_time = datetime.fromisoformat(dropoff_time_str)

    cursor.execute('''
        SELECT pickupTime FROM Drive
        WHERE riderId = ? AND userId = ?
    ''', (rider_id, user_id))
    pickup_time_str = cursor.fetchone()[0]

    # Pickup time을 datetime 객체로 변환
    pickup_time = datetime.fromisoformat(pickup_time_str)

    # 시간 차이 계산
    time_diff = (dropoff_time - pickup_time).total_seconds()
    cost = time_diff * 10  # 초당 10원으로 계산

    cursor.execute('''
        UPDATE Drive
        SET dropoffTime = ?, paymentState = 'Pending'
        WHERE riderId = ? AND userId = ? AND paymentState IS NULL
    ''', (dropoff_time_str, rider_id, user_id))

    send_payment_request(user_id, rider_id, cost)

    connection.commit()
    connection.close()


def send_payment_request(user_id, rider_id, cost):
    global persistent_publisher, messaging_service

    payment_topic = f"{TOPIC_PREFIX}/PaymentRequest"
    payment_message = json.dumps({
        "timestamp": datetime.now().isoformat(),
        "userId": user_id,
        "driverId": rider_id,
        "cost": cost
    })
    message_builder = messaging_service.message_builder() \
        .with_application_message_id("payment_request") \
        .with_property("application", "taxi_service") \
        .with_property("language", "Python")

    additional_properties = {APPLICATION_MESSAGE_ID: 'payment_request_1'}
    outbound_message = message_builder.build(payment_message,
                                             additional_message_properties=additional_properties)
    persistent_publisher.publish(destination=Topic.of(payment_topic), message=outbound_message)
    print(f"Sent payment request: {payment_message}")


def main():
    global SHUTDOWN, persistent_publisher, messaging_service, queue_receivers

    create_tables()

    # Solace 메시지 구독 설정
    queue_rideRequest = "Q.GYUMIN_RideRequest"
    queue_DriverResponse = "Q.GYUMIN_DriverResponse"
    queue_Pickup = "Q.GYUMIN_Pickup"
    queue_Dropoff = "Q.GYUMIN_Dropoff"

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
        print(f"Subscribed to queue: {queue_rideRequest}")
        queue_receiver = messaging_service.create_persistent_message_receiver_builder().build(
            Queue.durable_exclusive_queue(queue_rideRequest))
        queue_receiver.start()
        queue_receiver.receive_async(RideRequestHandler())
        queue_receivers['queue_rideRequest'] = queue_receiver

        print(f"Subscribed to queue: {queue_DriverResponse}")
        driver_response_receiver = messaging_service.create_persistent_message_receiver_builder().build(
            Queue.durable_exclusive_queue(queue_DriverResponse))
        driver_response_receiver.start()
        driver_response_receiver.receive_async(DriverResponseHandler())
        queue_receivers['queue_DriverResponse'] = driver_response_receiver

        print(f"Subscribed to queue: {queue_Pickup}")
        pickup_receiver = messaging_service.create_persistent_message_receiver_builder().build(
            Queue.durable_exclusive_queue(queue_Pickup))
        pickup_receiver.start()
        pickup_receiver.receive_async(PickupDropoffHandler())
        queue_receivers['queue_Pickup'] = pickup_receiver

        print(f"Subscribed to queue: {queue_Dropoff}")
        dropoff_receiver = messaging_service.create_persistent_message_receiver_builder().build(
            Queue.durable_exclusive_queue(queue_Dropoff))
        dropoff_receiver.start()
        dropoff_receiver.receive_async(PickupDropoffHandler())
        queue_receivers['queue_Dropoff'] = dropoff_receiver

        if queue_receiver.is_running:
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
        for receiver in queue_receivers.values():
            if receiver.is_running:
                receiver.terminate()
        print('Disconnecting Messaging Service')
        messaging_service.disconnect()


if __name__ == "__main__":
    main()
