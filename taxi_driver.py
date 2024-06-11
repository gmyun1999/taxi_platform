import datetime
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
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from dotenv import load_dotenv
from database import create_tables

load_dotenv()

TOPIC_PREFIX = "solace/taxi_samples_yun"
SHUTDOWN = False
direct_publisher = None
direct_receiver = None
confirmation_receiver = None
user_id_global = None  # 글로벌 변수로 user_id를 설정


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def save_driver_to_db(driver_id, car_num):
    connection = sqlite3.connect('taxi_service.db')
    cursor = connection.cursor()

    cursor.execute('''
        INSERT OR IGNORE INTO Rider (riderId, car_num)
        VALUES (?, ?)
    ''', (driver_id, car_num))

    connection.commit()
    connection.close()


def driver_main(driver_id, car_num, wait_time):
    global SHUTDOWN, direct_publisher, direct_receiver, confirmation_receiver, user_id_global

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
    direct_publisher.start()  # 토픽 발생 객체 초기화

    topic_to_subscribe = f"{TOPIC_PREFIX}/DriverBroadcast"
    topics_sub = [TopicSubscription.of(topic_to_subscribe)]

    class RideBroadcastHandler(MessageHandler):
        def on_message(self, message: 'InboundMessage'):
            global SHUTDOWN, direct_publisher, user_id_global
            if SHUTDOWN:
                return

            payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                payload = payload.decode()

            print(f"\nDriver {driver_id} received broadcast: {payload}\n")
            time.sleep(wait_time)  # wait for the specified time before accepting
            try:
                request_data = json.loads(payload)
                user_id_global = request_data["userId"]  # user_id를 글로벌 변수로 설정
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Failed to parse the message payload: {e}")
                return

            # Send acceptance response to platform
            response_topic = f"{TOPIC_PREFIX}/DriverResponse"
            response_body = json.dumps({
                "driverId": driver_id,
                "userId": user_id_global,
                "carNumber": car_num,
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

            # Set SHUTDOWN to stop further processing
            SHUTDOWN = True

    class DriverConfirmationHandler(MessageHandler):
        def on_message(self, message: 'InboundMessage'):
            global SHUTDOWN, direct_publisher, user_id_global
            payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                payload = payload.decode()

            try:
                response_data = json.loads(payload)
            except json.JSONDecodeError as e:
                print(f"Failed to parse the message payload: {e}")
                return

            if response_data.get("Message") == "다른 기사가 먼저 accept하였습니다":
                print(f"Driver {driver_id}: 승객 탑승 실패")
                SHUTDOWN = False  # Allow to receive new broadcast
            else:
                print(f"Driver {driver_id} confirmed")
                SHUTDOWN = True

                # Send pickup message after 10 seconds
                time.sleep(10)
                pickup_topic = f"{TOPIC_PREFIX}/Pickup"
                pickup_message = json.dumps({
                    "driverId": driver_id,
                    "userId": user_id_global,
                    "message": "pickup",
                    "timestamp": datetime.datetime.now()
                }, default=json_serial)
                pickup_outbound_message = messaging_service.message_builder() \
                    .with_application_message_id("pickup_notification") \
                    .with_property("application", "taxi_service") \
                    .with_property("language", "Python") \
                    .build(pickup_message)
                direct_publisher.publish(destination=Topic.of(pickup_topic), message=pickup_outbound_message)
                print(f"Driver {driver_id} sent pickup notification")

                # Send dropoff message after another 10 seconds
                time.sleep(10)
                dropoff_topic = f"{TOPIC_PREFIX}/Dropoff"
                dropoff_message = json.dumps({
                    "driverId": driver_id,
                    "userId": user_id_global,
                    "message": "dropoff",
                    "timestamp": datetime.datetime.now()
                }, default=json_serial)
                dropoff_outbound_message = messaging_service.message_builder() \
                    .with_application_message_id("dropoff_notification") \
                    .with_property("application", "taxi_service") \
                    .with_property("language", "Python") \
                    .build(dropoff_message)
                direct_publisher.publish(destination=Topic.of(dropoff_topic), message=dropoff_outbound_message)
                print(f"Driver {driver_id} sent dropoff notification")

                # Allow new ride requests after dropoff
                SHUTDOWN = False

    def terminate_services():
        global direct_publisher, direct_receiver, confirmation_receiver, messaging_service
        if direct_publisher and direct_publisher.is_running:
            direct_publisher.terminate()
        if direct_receiver and direct_receiver.is_running:
            direct_receiver.terminate()
        if confirmation_receiver and confirmation_receiver.is_running:
            confirmation_receiver.terminate()
        print('Disconnecting Messaging Service')
        messaging_service.disconnect()

    try:
        print(f"Driver {driver_id} subscribed to: {topics_sub}")
        # 택시콜 토픽을 구독한다. 만약 이벤트가 발생했을때 콜백함수로 RideBroadcastHandler를 실행시킨다.
        direct_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            topics_sub).build()
        direct_receiver.start()
        direct_receiver.receive_async(RideBroadcastHandler())

        # 택시콜을 ACCEPT한후에 ACCEPT이 수락되었는지 확인한다.
        confirmation_topic = f"{TOPIC_PREFIX}/DriverConfirmation/{driver_id}"
        confirmation_sub = [TopicSubscription.of(confirmation_topic)]
        confirmation_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            confirmation_sub).build()
        confirmation_receiver.start()
        confirmation_receiver.receive_async(DriverConfirmationHandler())

        if direct_receiver.is_running:
            print(f"Driver {driver_id} connected and ready to receive broadcasts\n")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
        SHUTDOWN = True
        terminate_services()
    except PubSubPlusClientError as exception:
        print(f'Received a PubSubPlusClientException: {exception}')
        SHUTDOWN = True
        terminate_services()
    finally:
        if SHUTDOWN:
            terminate_services()


if __name__ == "__main__":
    create_tables()
    driver_id = input("Enter driver ID: ")
    car_num = input("Enter car number: ")
    wait_time = int(input("Enter wait time before accepting (seconds): "))
    save_driver_to_db(driver_id, car_num)
    driver_main(driver_id, car_num, wait_time)
