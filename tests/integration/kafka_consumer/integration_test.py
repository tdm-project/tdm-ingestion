import argparse
import json
import logging.config
import os
import requests
import time
from kafka import KafkaProducer

from tdm_ingestion.converters.ngsi_converter import NgsiConverter
from tests.integration.utils import \
    check_docker_logs, docker_compose_up, try_func, \
    docker_compose_down, docker_compose_exec, get_service_port

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'default': {
            'format': '{asctime} - {name} - {levelname} - {message}',
            'style': '{'
        }
    },
    'handlers': {
        'console': {
            'level': logging.DEBUG,
            'class': 'logging.StreamHandler',
            'formatter': 'default',
        },
    },
    'loggers': {
        'tdm_ingestion': {
            'handlers': ['console'],
            'level': logging.DEBUG,
            'propagate': True
        }
    }
}

logging.config.dictConfig(LOGGING)

logger = logging.getLogger('tdm_ingestion.kafka_consumer.integration_tests')

DIR = os.path.dirname(os.path.realpath(__file__))
docker_yaml = os.path.join(DIR, 'docker-compose.yaml')


def check_timeseries(base_url, sensor_id, params):
    docker_compose_exec(
        docker_yaml, 'timescaledb',
        ['psql', '-U', 'postgres', '-d', 'tdmqtest', '-c',
         f"UPDATE source SET public = true WHERE external_id = '{sensor_id}';"])
    logger.debug("sensors %s", requests.get(f'{base_url}/sources').json())
    check_docker_logs(docker_yaml, 'ingester')
    check_docker_logs(docker_yaml, 'web')
    r = requests.get(f"{base_url}/sources", params={'id': sensor_id})
    r.raise_for_status()
    if len(r.json()) == 0:
        return False
    tdmq_id = r.json()[0]['tdmq_id']
    r = requests.get(
        f"{base_url}/sources/{tdmq_id}/timeseries",
        params=params)
    r.raise_for_status()
    return len(r.json()['data']) > 0


def send_message(producer, topic, data):
    if producer is None:
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    future = producer.send(topic, data)
    future.get(60)
    logger.debug("message sent")
    return True


def increment_and_wait(counter, wait=5):
    time.sleep(wait)
    counter += 1


def main(run_docker=True):
    try:
        if run_docker:
            docker_compose_up(docker_yaml)
        with open(os.path.join(DIR, '../../messages/ngsi-weather.json'), 'rb') as f:
            messages = json.load(f)
        logger.debug("Data to send %s", messages)

        port = get_service_port(docker_yaml, 'web', '8000')
        base_url = f'http://localhost:{port}/api/v0.0'
        for message in messages:
            sensor_name = NgsiConverter._get_source_id(message)

            try_func(send_message, 1, 10, None, 'test', message)
            try_func(check_timeseries, 1, 10, base_url, sensor_name,
                     {
                         'after': '2000-01-01T00:00:00Z',
                         'before': '2100-01-01T00:00:00Z'
                     })
    except RuntimeError as e:
        logger.error("Test failed: %s", str(e))
    else:
        logger.info("Test passed")
    finally:
        if run_docker:
            docker_compose_down(docker_yaml)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", dest="run_docker", action="store_true")
    args = parser.parse_args()
    main(args.run_docker)
