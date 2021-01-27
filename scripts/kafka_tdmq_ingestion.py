#!/usr/bin/python3
import logging

import tdm_ingestion.converters.ngsi_converter
import tdm_ingestion.storage.tdmq
import tdm_ingestion.tdmq.remote

from tdm_ingestion.consumers.confluent_kafka_consumer import KafkaConsumer
from tdm_ingestion.converters.ngsi_converter import NgsiConverter
from tdm_ingestion.storage.tdmq import CachedStorage
from tdm_ingestion.tdmq.remote import Client

logger = logging.getLogger('tdm_ingestion.kafka_tdms_ingestion')

def main():
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
    parser.add_argument('--kafka_group', dest='kafka_group', default='tdm_ingestion',
                        required=False)
    parser.add_argument('--tdmq_auth_token', dest='tdmq_auth_token', required=True)

    args = parser.parse_args()

    logger.info('running with args %s', args.__dict__)
    if (args.debug):
        tdm_ingestion.converters.ngsi_converter.set_log_level(logging.DEBUG)
        tdm_ingestion.storage.tdmq.set_log_level(logging.DEBUG)
        tdm_ingestion.tdmq.remote.set_log_level(logging.DEBUG)

    kwargs = {}
    if args.from_beginning:
        kwargs['auto.offset.reset'] = 'beginning'
    consumer = KafkaConsumer(args.bootstrap_server.split(','),
                             args.topics.split(','),
                             args.kafka_group,
                             **kwargs)
    tdmq = CachedStorage(Client(args.tdmq_url, auth_token=args.tdmq_auth_token))
    converter = NgsiConverter()

    while True:
        try:
            tdmq.write(converter.convert(consumer.poll(timeout_s=5)))
        except Exception as ex:
            logger.exception(ex)

if __name__ == '__main__':
    main()
