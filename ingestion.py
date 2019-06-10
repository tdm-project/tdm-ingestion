from abc import ABC, abstractmethod
from typing import List


class Message:
    def __init__(self, key: str, value: str):
        self.key = key
        self.key = value


class AbstractConsumer(ABC):
    def __init__(self, bootstrap_servers: List[str], topics: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics

    @abstractmethod
    def poll(self, timeout_ms: int=-1, max_records: int=-1) -> List[Message]:
        pass


class AbstractStorage(ABC):
    @abstractmethod
    def write(self, messages: List[Message]):
        pass


class DummyStorage(AbstractStorage):
    def __init__(self):
        self.messages = []

    def write(self, messages: List[Message]):
        self.messages += messages

class DummyConsumer(AbstractConsumer):
    def poll(self, timeout_ms=0, max_records=0)-> List[Message]:
        return [Message('key', 'value')]


class Ingester:
    def __init__(self, consumer: AbstractConsumer, storage: AbstractStorage):
        self.consumer = consumer
        self.storage = storage

    def process(self, timeout_ms: int=0, max_records: int=0):
        self.storage.write(self.consumer.poll(timeout_ms, max_records))


if __name__ == '__main__':
    import argparse
    import importlib

    parser = argparse.ArgumentParser()
    consumer_choices = ['confluent_kafka_consumer.Consumer']
    storage_choices = ['tdmq_storage']
    parser.add_argument('-c', help='consumer class', choices=consumer_choices, dest='consumer_class', default=consumer_choices[0]) 
    parser.add_argument('-s', help='storage module', choices=consumer_choices, dest='storage_module', default=storage_choices[0]) 
    parser.add_argument('--bootstrap_servers', help='kafka  comma separated bootstrap servers', dest='bootstrap_servers', required=True) 
    parser.add_argument('--topics', help='kafka comma separated topics', dest='topics', required=True) 
    args = parser.parse_args()

    bootstrap_servers = args.bootstrap_servers.split(',')
    topics = args.topics.split(',')
    
    storage = importlib.import_module(args.storage_module).Storage()
    consumer_module = args.consumer_class.split('.')[:-1]
    consumer_class = args.consumer_class.split('.')[-1]
    
    consumer = importlib.import_module(args.consumer_module).Consumer(bootstrap_servers, topics)
    ingester = Ingester(consumer, storage)
    
    while True:
        ingester.process()
        
    