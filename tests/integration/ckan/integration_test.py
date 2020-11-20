import argparse
import json
import logging.config
import os
import requests
import subprocess
from csv import DictReader
from io import StringIO

from tests.integration.utils import (docker_compose_down,
                                     docker_compose_restart, docker_compose_up,
                                     get_service_port, try_func)

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

logger = logging.getLogger('tdm_ingestion.ckan.integration_tests')

DIR = os.path.dirname(os.path.realpath(__file__))
docker_yaml = os.path.join(DIR, 'docker-compose.yaml')


def check_ckan():
    logger.debug('checking ckan')
    try:
        dataset = requests.get('http://localhost:5000/api/3/action/package_show?id=lisa').json()['result']

        logger.debug(f'num resources found: {dataset["num_resources"]} expected: 1')
        assert dataset['num_resources'] == 1, "Number of resources different than expected"
        expected = """_id,station,type,date,location,temperature,humidity
1,tdm/sensor_0,Station,2019-05-02T11:00:00Z,"38.9900400015583,8.93607900725268",23,0.272000001122554
2,tdm/sensor_1,Station,2019-05-02T10:50:00Z,"40.5841280014956,8.24696900768626",20,0.419999986886978
3,tdm/sensor_1,Station,2019-05-02T11:00:00Z,"40.5841280014956,8.24696900768626",25,0.400000005960464
4,tdm/sensor_1,Station,2019-05-02T11:10:00Z,"40.5841280014956,8.24696900768626",22,0.379999995231628
5,tdm/sensor_1,Station,2019-05-02T11:20:00Z,"40.5841280014956,8.24696900768626",25,0.349999994039536"""
        actual = requests.get(dataset['resources'][0]['url']).text
        actual_dict_lines = [dict(l) for l in DictReader(StringIO(actual))]
        expected_dict_lines = [dict(l) for l in DictReader(StringIO(expected))]
        logger.debug("Retrieved lines: %s", actual_dict_lines)
        logger.debug("Expected lines: %s", expected_dict_lines)
        assert actual_dict_lines == expected_dict_lines, "Lines found different than the expected"
    except AssertionError as e:
        logger.error("Failed ckan test: %s", str(e))
        return False
    except Exception as e:
        logger.error(str(e))
        return False
    return True


def main(run_docker=True):
    try:
        if run_docker:
            docker_compose_up(docker_yaml)
        subprocess.check_call(['./init.sh'])

        port = get_service_port(docker_yaml, 'web', '8000')
        base_url = f'http://localhost:{port}/api/v0.0'
        with open('data/sources.json') as f:
            res = requests.post(f'{base_url}/sources', json=json.load(f))

        with open('data/records.json') as f:
            requests.post(f'{base_url}/records', json=json.load(f))
        docker_compose_restart(docker_yaml, 'ingester')
        try_func(check_ckan, 2, 10)
    except RuntimeError as e:
        logger.error("Test failed: %s", str(e))
    else:
        logger.info("Test Passed")
    finally:
        if run_docker:
            docker_compose_down(docker_yaml)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", dest="run_docker", action="store_true")
    args = parser.parse_args()
    main(args.run_docker)
