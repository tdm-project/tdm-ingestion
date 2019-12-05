import json
import logging
import os
import time

import requests
from kafka import KafkaProducer
from tdm_ingestion.converters.ngsi_converter import NgsiConverter
from tests.integration.utils import \
    check_docker_logs, docker_compose_up, try_func, \
    docker_compose_down, get_service_port


logger = logging.getLogger('tdm_ingestion.integration_tests')


DIR = os.path.dirname(os.path.realpath(__file__))
docker_yaml = os.path.join(DIR, 'docker-compose.yaml')


def check_timeseries(base_url, sensor_id, params):
    logger.debug("sensors %s", requests.get(f'{base_url}/sources').json())
    check_docker_logs(docker_yaml, 'ingester')
    check_docker_logs(docker_yaml, 'web')
    r = requests.get(f"{base_url}/sources", params={'id': sensor_id})
    r.raise_for_status()
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


try:
    docker_compose_up(docker_yaml)
    with open(os.path.join(DIR, '../../messages/ngsi-weather.json'), 'rb') as f:
        data = json.load(f)
    logger.debug("Data to send %s", data)

    port = get_service_port(docker_yaml, 'web', '8000')
    base_url = f'http://localhost:{port}/api/v0.0'

    _, _, _, sensor_name = NgsiConverter._get_names(data)

    sensor_name = f'{sensor_name}'

    try_func(send_message, 1, 10, None, 'test', data)
    try_func(check_timeseries, 1, 10, base_url, sensor_name,
             {
                 'after': '2000-01-01T00:00:00Z',
                 'before': '2100-01-01T00:00:00Z'
             })
finally:
    docker_compose_down(docker_yaml)
    pass
