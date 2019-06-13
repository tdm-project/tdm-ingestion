from typing import List

from confluent_kafka import Consumer as ConfluentKafkaConsumer
from tdm_ingestion.ingestion import Consumer, Message


class KafkaConsumer(Consumer):

    def __init__(self, bootstrap_servers: List[str], topics: List[str], group_id: str='tdm_ingestion'):
        self.bootstrap_servers = ','.join(bootstrap_servers)
        self.topics = topics
        self.group_id = group_id

        self.consumer = ConfluentKafkaConsumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id
        })
        self.consumer.subscribe(self.topics)

    def poll(self, timeout_s: int = -1, max_records: int = 1) -> List[Message]:
        return [Message(m.key(), m.value()) for m in self.consumer.consume(max_records, timeout_s)]
