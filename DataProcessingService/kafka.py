from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import socket

conf = {"bootstrap.servers": "localhost:9092", "client.id": "demo", "group.id": "demo"}

producer = Producer(conf)

consumer = Consumer(conf)

admin = AdminClient(conf)


def create_topic(topic_name):
    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    admin.create_topics([topic])


def list_topics():
    return admin.list_topics().topics


def send_message(topic_name, message):
    producer.produce(topic_name, value=message)
    producer.flush()
