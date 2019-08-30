import json
import logging
import os
import subprocess

import requests
from tests.integration_tests.utils import \
    docker_compose_up, get_service_port, docker_compose_down

logging.basicConfig(level=logging.DEBUG)
DIR = os.path.dirname(os.path.realpath(__file__))
docker_yaml = os.path.join(DIR, 'docker-compose.yaml')

try:
    docker_compose_up(docker_yaml)
    subprocess.check_call(['./init.sh'])

    port = get_service_port(docker_yaml, 'web', '8000')
    base_url = f'http://localhost:{port}/api/v0.0'
    with open('data/sources.json') as f:
        requests.post(f'{base_url}/sources', json=json.load(f))

    with open('data/records.json') as f:
        requests.post(f'{base_url}/records', json=json.load(f))
finally:
    docker_compose_down(docker_yaml)
