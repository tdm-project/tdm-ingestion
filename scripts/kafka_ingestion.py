#!/usr/bin/python3
import logging

from tdm_ingestion.consumers.confluent_kafka_consumer import KafkaConsumer
from tdm_ingestion.converters.ngsi_converter import NgsiConverter
from tdm_ingestion.http_client.requests import Requests
from tdm_ingestion.storage.tdmq import CachedStorage
from tdm_ingestion.tdmq.remote import Client

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', help='debug', dest='debug', action='store_true',
                        default=False)
    parser.add_argument('--bootstrap_server', dest='bootstrap_server',
                        help="comma separated values",
                        required=True)
    parser.add_argument('--topics', dest='topics',
                        help="comma separated values", required=True)
    parser.add_argument('--from_beginning', dest='from_beginning',
                        action='store_true',
                        default=False)
    parser.add_argument('--tdmq_url', dest='tdmq_url', required=True)

    args = parser.parse_args()
    logging_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=logging_level)

    logging.info('running with args %s', args.__dict__)

    kwargs = {}
    if args.from_beginning:
        kwargs['auto.offset.reset'] = 'beginning'
    consumer = KafkaConsumer(args.bootstrap_server.split(','),
                             args.topics.split(','),
                             **kwargs)
    tdmq = CachedStorage(Client(Requests(), args.tdmq_url))
    converter = NgsiConverter()

    while True:

        try:
            tdmq.write(converter.convert(consumer.poll(timeout_s=5)))
        except Exception as ex:
            logging.exception(ex)
