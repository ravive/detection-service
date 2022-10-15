from confluent_kafka import Producer, Consumer
from kafka.admin import KafkaAdminClient, NewTopic


def get_producer(bootstrap_servers):
    config = {
        "bootstrap.servers": bootstrap_servers,
        "message.timeout.ms": 5000,
        "socket.timeout.ms": 5000,
    }

    return Producer(config)


def get_consumer(bootstrap_servers):
    c = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "mygroup",
            "auto.offset.reset": "earliest",
        }
    )

    return c


def create_topic(bootstrap_servers, topic_name):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers, client_id="test"
    )

    topic_list = []
    topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
