#!/usr/bin/python3
from abc import ABC, abstractmethod
from typing import List, Dict

from tdm_ingestion.models import TimeSeries
from tdm_ingestion.utils import import_class


class Message:
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value


class Consumer(ABC):

    @abstractmethod
    def poll(self, timeout_s: int = -1,
             max_records: int = -1) -> List[Message]:
        pass


class Storage(ABC):
    @staticmethod
    @abstractmethod
    def create_from_json(json: Dict) -> "Storage":
        pass

    @abstractmethod
    def write(self, messages: List[TimeSeries]):
        pass


class MessageConverter(ABC):
    @abstractmethod
    def convert(self, messages: List[Message]) -> List[TimeSeries]:
        pass


class Ingester:
    def __init__(self, consumer: Consumer, storage: Storage,
                 converter: MessageConverter):
        self.consumer = consumer
        self.storage = storage
        self.converter = converter

    def process(self, timeout_s: int = -1, max_records: int = 1):
        self.storage.write(
            self.converter.convert(self.consumer.poll(timeout_s, max_records)))


if __name__ == '__main__':
    import argparse
    import logging
    import yaml
    from tdm_ingestion.converters.ngsi_converter import NgsiConverter

    logging.basicConfig(level=logging.DEBUG)

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
        storage = import_class(conf['storage']['class']).create_from_json(
            conf['storage']['args'])
        consumer = import_class(conf['consumer']['class'])(
            **conf['consumer']['args'])
        ingester_process_args = conf['ingester']['process']

    ingester = Ingester(consumer, storage, NgsiConverter())

    while True:
        try:
            ingester.process(**ingester_process_args)
        except Exception as ex:
            logging.exception(ex)
