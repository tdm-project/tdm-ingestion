#!/usr/bin/python3

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
    def poll(self, timeout_s: int = -1, max_records: int = -1) -> List[Message]:
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

    def process(self, timeout_s: int = -1, max_records: int = 1):
        self.storage.write(self.converter.convert(self.consumer.poll(timeout_s, max_records)))


if __name__ == '__main__':
    import argparse
    import importlib
    import logging
    import yaml
    from tdm_ingestion.converters.ngsi_converter import NgsiConverter

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
    parser.add_argument('conf_file', help='conf file', default='conf.yaml')
    args = parser.parse_args()
    with open(args.conf_file, 'r') as conf_file:
        conf = yaml.safe_load(conf_file)
        logging.debug('conf %s', conf)
        storage = import_class(conf['storage']['class'])(**conf['storage']['args'])
        consumer = import_class(conf['consumer']['class'])(**conf['consumer']['args'])
        ingester_process_args = conf['ingester']['process']

    ingester = Ingester(consumer, storage, NgsiConverter())

    while True:
        try:
            ingester.process(**ingester_process_args)
        except Exception as ex:
            logging.exception(ex)
