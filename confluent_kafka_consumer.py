from typing import List
from confluent_kafka import Consumer as ConfluentKafkaConsumer
from ingestion import Consumer


class KafkaConsumer(Consumer):

    def __init__(self, bootstrap_servers: List[str], topics: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics

        self.consumer = ConfluentKafkaConsumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': '1'
        })
        self.consumer.subscribe(topics)

    def poll(self, timeout_ms: int = -1, max_records: int = -1):
        return self.consumer.consume(max_records, timeout_ms)
