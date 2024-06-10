import os
import time
import json
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
direct_publisher = None
direct_receiver = None
confirmation_receiver = None


def driver_main(driver_id, wait_time):
    global SHUTDOWN, direct_publisher, direct_receiver, confirmation_receiver

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
    direct_publisher.start()    # 토픽 발생 객체 초기화

    topic_to_subscribe = f"{TOPIC_PREFIX}/DriverBroadcast"
    topics_sub = [TopicSubscription.of(topic_to_subscribe)]

    class RideBroadcastHandler(MessageHandler):
        def on_message(self, message: 'InboundMessage'):
            global SHUTDOWN, direct_publisher
            if SHUTDOWN:
                return

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

            # Set SHUTDOWN to stop further processing
            SHUTDOWN = True

    class DriverConfirmationHandler(MessageHandler):
        def on_message(self, message: 'InboundMessage'):
            global SHUTDOWN
            payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                payload = payload.decode()

            response_data = json.loads(payload)
            if response_data.get("Message") == "다른 기사가 먼저 accept하였습니다":
                print(f"Driver {driver_id}: 승객 탑승 실패")
                SHUTDOWN = False  # Allow to receive new broadcast
            else:
                print(f"Driver {driver_id} confirmed")
                SHUTDOWN = True

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
        #   택시콜 토픽을 구독한다. 만약 이벤트가 발생했을때 콜백함수로 RideBroadcastHandler를 실행시킨다.
        direct_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(
            topics_sub).build()
        direct_receiver.start()
        direct_receiver.receive_async(RideBroadcastHandler())

        # 택시콜을 ACEPT한후에 ACCEPT이 수락되었는지 확인한다.
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
    driver_id = input("Enter driver ID: ")
    wait_time = int(input("Enter wait time before accepting (seconds): "))
    driver_main(driver_id, wait_time)
