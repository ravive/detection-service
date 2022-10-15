import json
import unittest
import redis
from testcontainers.kafka import KafkaContainer
from testcontainers.redis import RedisContainer

import detection_service
from detection_service import OUTPUT_TOPIC, SCAN_TOPIC, FISHING_TOPIC
from tests.kakfa_test import get_producer, create_topic, get_consumer


def send_message(message):
    detection_service.producer.produce(
        OUTPUT_TOPIC,
        value=json.dumps(message),
    )
    detection_service.producer.flush()


def create_topics(container):
    create_topic(container.get_bootstrap_server(), OUTPUT_TOPIC)
    create_topic(container.get_bootstrap_server(), SCAN_TOPIC)
    create_topic(container.get_bootstrap_server(), FISHING_TOPIC)


def get_output_consumer(kafka):
    output_consumer = get_consumer(kafka.get_bootstrap_server())
    output_consumer.subscribe([OUTPUT_TOPIC])
    return output_consumer


def get_fishing_topic(kafka):
    fishing_consumer = get_consumer(kafka.get_bootstrap_server())
    fishing_consumer.subscribe([FISHING_TOPIC])
    return fishing_consumer


def get_scan_consumer(container):
    scan_consumer = get_consumer(container.get_bootstrap_server())
    scan_consumer.subscribe([SCAN_TOPIC])
    return scan_consumer


class TestDetectionService(unittest.TestCase):
    def test_no_engine_send_to_scan_topic(self):
        with KafkaContainer("confluentinc/cp-kafka:latest") as container:
            create_topics(container)
            detection_service.producer = get_producer(container.get_bootstrap_server())

            send_message({"transaction_id": 1, "is_phishing": False})

            detection_service.producer.flush()
            output_consumer = get_output_consumer(container)
            detection_service.consume_message(output_consumer)
            output_consumer.close()

            scan_consumer = get_scan_consumer(container)
            msg = scan_consumer.poll(5.0)
            assert msg is not None
            assert (
                msg.value().decode("utf-8")
                == '{"transaction_id": 1, "is_phishing": false}'
            )

    def test_engine_exists_phishing_false(self):
        with KafkaContainer("confluentinc/cp-kafka:latest") as kafka:
            create_topics(kafka)
            detection_service.producer = get_producer(kafka.get_bootstrap_server())

            send_message(
                {
                    "transaction_id": 1,
                    "is_phishing": False,
                    "engine_name": "engine1",
                }
            )

            output_consumer = get_output_consumer(kafka)
            detection_service.consume_message(output_consumer)
            output_consumer.close()

            fishing_consumer = get_fishing_topic(kafka)
            msg = fishing_consumer.poll(5.0)
            assert msg is None

            scan_consumer = get_scan_consumer(kafka)
            msg = scan_consumer.poll(5.0)
            assert msg is None

    def test_engine_exists_phishing_true(self):
        with KafkaContainer(
            "confluentinc/cp-kafka:latest"
        ) as kafka, RedisContainer() as redis_container:
            create_topics(kafka)
            detection_service.producer = get_producer(kafka.get_bootstrap_server())
            detection_service.redis = redis.Redis(
                host="localhost", port=int(redis_container.get_exposed_port(6379)), db=0
            )

            output_consumer = get_output_consumer(kafka)

            for x in range(2):
                send_message(
                    {
                        "transaction_id": 1,
                        "is_phishing": True,
                        "engine_name": "engine1",
                    }
                )
                detection_service.consume_message(output_consumer)

            output_consumer.close()

            fishing_consumer = get_fishing_topic(kafka)
            msg = fishing_consumer.poll(5.0)
            assert msg is not None
            assert (
                    msg.value().decode("utf-8")
                    == '{"transaction_id": 1, "is_phishing": true, "engine_name": "engine1"}'
            )

            scan_consumer = get_scan_consumer(kafka)
            msg = scan_consumer.poll(5.0)
            assert msg is None
