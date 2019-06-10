from abc import ABC, abstractmethod
from typing import List


class Measure(ABC):
    @abstractmethod
    def to_dict(self):
        pass


class ValueMeasure(Measure):
    def __init__(self, value):
        self.value = value

    def to_dict(self):
        return {'value': self.value}


class RefMeasure(Measure):
    def __init__(self, ref, index):
        self.ref = ref
        self.index = index

    def to_dict(self):
        return {'reference': self.ref, 'index': self.index}


class TimeSeries:
    def __init__(self, time: str, sensorcode: str, measure: Measure):
        self.time = time
        self.sensorcode = sensorcode
        self.measure = measure

    def to_dict(self):
        return {'time': self.time, 'sensorcode': self.sensorcode, 'measure': self.measure.to_dict()}


class Message:
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value


class Consumer(ABC):
    def __init__(self, bootstrap_servers: List[str], topics: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics

    @abstractmethod
    def poll(self, timeout_ms: int = -1, max_records: int = -1) -> List[Message]:
        pass


class Storage(ABC):
    @abstractmethod
    def write(self, messages: List[TimeSeries]):
        pass


class MessageConverter(ABC):
    @abstractmethod
    def convert(self, messages: List[Message]) -> List[TimeSeries]:
        pass


class Ingester:
    def __init__(self, consumer: Consumer, storage: Storage, converter: MessageConverter):
        self.consumer = consumer
        self.storage = storage
        self.converter = converter

    def process(self, timeout_ms: int = 0, max_records: int = 0):
        self.storage.write(self.converter.convert(self.consumer.poll(timeout_ms, max_records)))


if __name__ == '__main__':
    import argparse
    import importlib


    def import_class(class_path: str):
        class_path_splitted = class_path.split('.')
        module = '.'.join(class_path_splitted[:-1])
        cls = class_path_splitted[-1]
        return getattr(importlib.import_module(module), cls)


    parser = argparse.ArgumentParser()
    consumer_choices = ['ingestion.DummyConsumer', 'confluent_kafka_consumer.Consumer']
    storage_choices = ['tdmq_storage.Storage']
    parser.add_argument('-c', help='consumer class', choices=consumer_choices, dest='consumer_class',
                        default=consumer_choices[0])
    parser.add_argument('-s', help='storage class', choices=consumer_choices, dest='storage_class',
                        default=storage_choices[0])
    parser.add_argument('--bootstrap_servers', help='kafka  comma separated bootstrap servers',
                        dest='bootstrap_servers', required=True)
    parser.add_argument('--topics', help='kafka comma separated topics', dest='topics', required=True)
    args = parser.parse_args()

    bootstrap_servers = args.bootstrap_servers.split(',')
    topics = args.topics.split(',')

    storage = import_class(args.storage_class)()
    consumer = import_class(args.consumer_class)(bootstrap_servers, topics)
    ingester = Ingester(consumer, storage)

    while True:
        ingester.process()
