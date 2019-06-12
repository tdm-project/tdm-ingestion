import json
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List


class Measure(ABC):
    @abstractmethod
    def to_dict(self) -> dict:
        pass


class ValueMeasure(Measure):
    def __init__(self, value):
        self.value = value

    def to_dict(self) -> dict:
        return {'value': self.value}


class RefMeasure(Measure):
    def __init__(self, ref, index):
        self.ref = ref
        self.index = index

    def to_dict(self) -> dict:
        return {'reference': self.ref, 'index': self.index}


class TimeSeries:
    def __init__(self, time: datetime, sensorcode: uuid.UUID, measure: Measure):
        self.time = time.astimezone(timezone.utc)
        self.sensorcode = sensorcode
        self.measure = measure

    def to_dict(self) -> dict:
        return {'time': self.time.strftime('%Y-%m-%dT%H:%M:%SZ'), 'sensorcode': str(self.sensorcode),
                'measure': self.measure.to_dict()}


class Message:
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value


class Consumer(ABC):

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

    def get_sensorcode(self, _id: str) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_DNS, _id)


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
    from converters.ngsi_converter import NgsiConverter


    def import_class(class_path: str):
        class_path_splitted = class_path.split('.')
        module = '.'.join(class_path_splitted[:-1])
        cls = class_path_splitted[-1]
        return getattr(importlib.import_module(module), cls)


    parser = argparse.ArgumentParser()
    consumer_choices = ['tests.dummies.DummyConsumer', 'confluent_kafka_consumer.Consumer']
    storage_choices = ['tdmq_storage.TDMQStorage']
    parser.add_argument('-c', help='consumer class', choices=consumer_choices, dest='consumer_class',
                        default=consumer_choices[0])
    parser.add_argument('-s', help='storage class', choices=consumer_choices, dest='storage_class',
                        default=storage_choices[0])
    parser.add_argument('--consumer_args', dest='consumer_args', help='json dict with kwargs for building the consumer',
                        default='{}')
    parser.add_argument('--storage_args', dest='storage_args', help='json dict with kwargs for building the storage',
                        default='{}')
    args = parser.parse_args()

    storage = import_class(args.storage_class)(**json.loads(args.storage_args))
    consumer = import_class(args.consumer_class)(**json.loads(args.consumer_args))
    ingester = Ingester(consumer, storage, NgsiConverter())

    while True:
        ingester.process()
