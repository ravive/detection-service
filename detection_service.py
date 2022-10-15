import json

import redis
from confluent_kafka import Consumer, Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})
redis = redis.Redis(host="localhost", port=6379, db=0)
EXPIRED_IN_SECONDS = 60 * 30
from prometheus_client import start_http_server, Summary, Counter

is_phishing_counter = Counter(
    "is_phishing_counter", "Description of counter", ["is_phishing"]
)
transaction_total_messages = Counter("total_transactions", "Description of counter")

REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")


class Transaction:
    transaction_id: str
    is_phishing: bool
    engine_name: str


OUTPUT_TOPIC = "output_topic"
SCAN_TOPIC = "scan_topic"
FISHING_TOPIC = "fishing_topic"
is_running = True


@REQUEST_TIME.time()
def is_phishing(msg):
    """
    :param msg:  transaction to check if fishing
    :return: if transaction is phishing false , no need to wait for more messages
             if transaction is phishing true , need to check second transaction result

    """
    if not msg.get("is_phishing"):
        is_phishing_counter.labels("false").inc()
        return False

    is_first_is_phishing_true = redis.setnx(str(msg.get("transaction_id")), 1)
    if is_first_is_phishing_true:
        redis.expire(msg.get("transaction_id"), EXPIRED_IN_SECONDS)
        return None

    is_phishing_counter.labels("true").inc()
    return True


def handle_message(msg):
    if msg is None:
        return None

    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        return None

    transaction_total_messages.inc()
    value = msg.value().decode("utf-8")
    print("Received message: {}".format(value))

    transaction = json.loads(value)
    if not transaction.get("engine_name"):
        producer.produce(SCAN_TOPIC, value)
        producer.flush()
        return None

    if is_phishing(transaction):
        producer.produce(FISHING_TOPIC, value)
        producer.flush()


def consume_message(output_consumer):
    msg = output_consumer.poll(5.0)
    handle_message(msg)


def run_service(bootstrap_servers):
    output_consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "mygroup",
            "auto.offset.reset": "latest",
        }
    )

    output_consumer.subscribe([OUTPUT_TOPIC])
    while is_running:
        consume_message(output_consumer)

    output_consumer.close()


if __name__ == "__main__":
    start_http_server(8000)
    run_service("localhost:9092")
