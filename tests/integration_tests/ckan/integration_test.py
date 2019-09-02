import json
import logging
import os
import subprocess

import requests
from tests.integration_tests.utils import \
    docker_compose_up, get_service_port, docker_compose_down, try_func

logging.basicConfig(level=logging.DEBUG)
DIR = os.path.dirname(os.path.realpath(__file__))
docker_yaml = os.path.join(DIR, 'docker-compose.yaml')


def check_ckan():
    print('check ckan')
    try:
        dataset = requests.get(
            'http://localhost:5000/api/3/action/package_show?id=lisa').json()[
            'result']

        print(f'num resources {dataset["num_resources"]}')
        assert dataset['num_resources'] == 1
        expected = """_id,station,type,date,location,humidity,temperature
1,tdm/sensor_0,Station,2019-05-02T11:00:00+00:00,"38.9900400015583,8.93607900725268",0.272000001122554,23
2,tdm/sensor_1,Station,2019-05-02T10:50:00+00:00,"40.5841280014956,8.24696900768626",0.419999986886978,20
3,tdm/sensor_1,Station,2019-05-02T11:00:00+00:00,"40.5841280014956,8.24696900768626",0.400000005960464,25
4,tdm/sensor_1,Station,2019-05-02T11:10:00+00:00,"40.5841280014956,8.24696900768626",0.379999995231628,22
5,tdm/sensor_1,Station,2019-05-02T11:20:00+00:00,"40.5841280014956,8.24696900768626",0.349999994039536,25"""
        actual = requests.get(dataset['resources'][0]['url']).text
        print(f'actual resource {actual}')
        assert expected.splitlines() == actual.splitlines()
    except Exception as ex:
        print(ex)
        return False
    return True


try:
    docker_compose_up(docker_yaml)
    subprocess.check_call(['./init.sh'])

    port = get_service_port(docker_yaml, 'web', '8000')
    base_url = f'http://localhost:{port}/api/v0.0'
    with open('data/sources.json') as f:
        requests.post(f'{base_url}/sources', json=json.load(f))

    with open('data/records.json') as f:
        requests.post(f'{base_url}/records', json=json.load(f))

    try_func(check_ckan, 2, 10)

finally:
    docker_compose_down(docker_yaml)
