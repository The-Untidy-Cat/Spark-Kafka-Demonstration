from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

config = {
    "bootstrap.servers": "localhost:9092",
    "id": "demo",
}

producer = Producer({
    "bootstrap.servers": config["bootstrap.servers"],
    "client.id": config["id"]
})

consumer = Consumer({
    "bootstrap.servers": config["bootstrap.servers"],
    "group.id": "demo"
})

admin = AdminClient({
    "bootstrap.servers": config["bootstrap.servers"],
    "client.id": config["id"]
})


def create_topic(topic_name):
    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    admin.create_topics([topic])


def create_topic_if_not_exist(topic_name):
    if topic_name not in list_topics():
        create_topic(topic_name)


def list_topics():
    return admin.list_topics().topics


def send_message(topic_name, message):
    producer.produce(topic_name, value=message)
    producer.flush()
