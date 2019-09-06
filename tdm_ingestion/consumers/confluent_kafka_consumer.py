import logging
from typing import List

from confluent_kafka import Consumer as ConfluentKafkaConsumer
from tdm_ingestion.consumers.base import BaseKafkaConsumer

logger = logging.getLogger(__name__)


class KafkaConsumer(BaseKafkaConsumer):

    def __init__(self,
                 bootstrap_servers: List[str],
                 topics: List[str],
                 group_id: str = 'tdm_ingestion', **kwargs
                 ):
        self.bootstrap_servers = ','.join(bootstrap_servers)
        self.topics = topics
        self.group_id = group_id

        params = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id
        }
        params.update(kwargs)
        logger.debug('creating consumer with params %s', params)
        self.consumer = ConfluentKafkaConsumer(params)
        self.consumer.subscribe(self.topics)
        logger.debug('subscribed to topics %s', topics)

    def poll(self, timeout_s: int = -1, max_records: int = 1) -> List[str]:
        return [m.value() for m in
                self.consumer.consume(max_records, timeout_s)]
