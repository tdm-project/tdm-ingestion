#!/usr/bin/python3
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any

from tdm_ingestion.models import TimeSeries
from tdm_ingestion.utils import import_class


class JsonBuildable:
    @classmethod
    def create_from_json(cls, json: Dict):
        json = json or {}
        return cls(**json)


class Consumer(ABC, JsonBuildable):

    @abstractmethod
    def poll(self, timeout_s: int = -1,
             max_records: int = -1) -> List[Any]:
        pass


class Storage(ABC, JsonBuildable):
    @abstractmethod
    def write(self, messages: List[TimeSeries]):
        pass


class MessageConverter(ABC, JsonBuildable):
    @abstractmethod
    def convert(self, messages: List[Any]) -> List[TimeSeries]:
        pass


class Ingester(ABC, JsonBuildable):
    @abstractmethod
    def process(self, *args, **kwargs):
        pass

    @abstractmethod
    def process_forever(self, *args, **kwargs):
        pass


class BasicIngester(Ingester):
    @classmethod
    def create_from_json(cls, json: Dict) -> "Ingester":
        consumer = import_class(json['consumer']['class']).create_from_json(
            json['consumer']['args'])

        storage = import_class(json['storage']['class']).create_from_json(
            json['storage']['args'])
        converter = import_class(json['converter']['class']).create_from_json(
            json['converter']['args'])
        return cls(consumer, storage, converter)

    def __init__(self, consumer: Consumer, storage: Storage,
                 converter: MessageConverter):
        self.consumer = consumer
        self.storage = storage
        self.converter = converter

    def process(self, *args, **kwargs):
        self.storage.write(
            self.converter.convert(self.consumer.poll(*args, **kwargs)))

    def process_forever(self, *args, **kwargs):
        while True:
            try:
                self.process(*args, **kwargs)
            except Exception as ex:
                logging.exception(ex)


if __name__ == '__main__':
    import argparse
    import yaml


    def parse_kwargs(comma_separated_kwargs: str) -> dict:
        res = {}
        for key_value in comma_separated_kwargs.split(','):
            splitted_key_value = key_value.split('=')
            if len(splitted_key_value) == 2:
                res[splitted_key_value[0]] = splitted_key_value[1]
        return res


    parser = argparse.ArgumentParser()
    parser.add_argument('conf_file', help='conf file',
                        default='sync-conf.yaml')
    parser.add_argument('-d', help='debug', dest='debug', action='store_true',
                        default=False)

    args = parser.parse_args()
    logging_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=logging_level,
                        format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S')

    with open(args.conf_file, 'r') as conf_file:
        conf = yaml.safe_load(conf_file)
        logging.debug('conf %s', conf)

        ingester = import_class(conf['ingester']['class']).create_from_json(
            conf['ingester']['args'])
        ingester_process_args = conf['ingester']['process']

        ingester.process_forever(**ingester_process_args)
