import logging
import subprocess

from tests.integration_tests.utils import docker_compose_up

logging.basicConfig(level=logging.DEBUG)

try:
    docker_compose_up()
    subprocess.check_call(['./init.sh'])

    # port = get_tdmq_port()
    # base_url = f'http://localhost:{port}/api/v0.0'
    # with open('data/sources.json') as f:
    #    requests.post(f'{base_url}/sources', json=json.load(f))

    # with open('data/records.json') as f:
    #    requests.post(f'{base_url}/records', json=json.load(f))
finally:
    pass
    # docker_compose_down()
