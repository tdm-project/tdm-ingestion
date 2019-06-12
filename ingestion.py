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
    import logging
    from converters.ngsi_converter import NgsiConverter

    logging.basicConfig(level=logging.DEBUG)


    def import_class(class_path: str):
        class_path_splitted = class_path.split('.')
        module = '.'.join(class_path_splitted[:-1])
        cls = class_path_splitted[-1]
        return getattr(importlib.import_module(module), cls)


    def parse_kwargs(comma_separated_kwargs: str) -> dict:
        res = {}
        for key_value in comma_separated_kwargs.split(','):
            splitted_key_value = key_value.split('=')
            if len(splitted_key_value) == 2:
                res[splitted_key_value[0]] = splitted_key_value[1]
        return res


    parser = argparse.ArgumentParser()
    consumer_choices = ['tests.dummies.DummyConsumer', 'confluent_kafka_consumer.Consumer']
    storage_choices = ['tdmq_storage.TDMQStorage']
    parser.add_argument('-c', help='consumer class', choices=consumer_choices, dest='consumer_class',
                        default=consumer_choices[0])
    parser.add_argument('-s', help='storage class', choices=consumer_choices, dest='storage_class',
                        default=storage_choices[0])
    parser.add_argument('--consumer-args', dest='consumer_args', help='comma separated key=value',
                        default='')
    parser.add_argument('--storage-args', dest='storage_args', help='comma separated key=value',
                        default='')
    args = parser.parse_args()

    storage = import_class(args.storage_class)(**parse_kwargs(args.storage_args))
    consumer = import_class(args.consumer_class)(**parse_kwargs(args.consumer_args))
    ingester = Ingester(consumer, storage, NgsiConverter())

    while True:
        ingester.process()
