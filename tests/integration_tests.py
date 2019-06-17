import json
import subprocess
import sys
import time

import requests
from kafka import KafkaProducer
from tdm_ingestion.converters.ngsi_converter import NgsiConverter


def create_sensor(url, sensors):
    r = requests.post(
        f'{base_url}/sensors',
        json=sensors
    )
    r.raise_for_status()
    return r

test_passed = False

with open('messages/ngsi-weather.json', 'rb') as f:
    data = json.load(f)

windSpeed = NgsiConverter().get_sensorcode(f"{data['body']['id']}-windSpeed")
port = subprocess.check_output(['docker', 'port', 'travis_web_1', '8000']).strip().split(b':')[-1].decode()
base_url = f'http://localhost:{port}/api/v0.0'
print(f'base_url {base_url}')

create_sensor(base_url, [{"code": str(windSpeed),
                          "stypecode": "0fd67c67-c9be-45c6-9719-4c4eada4be65",
                          "nodecode": "0fd67ccc-c9be-45c6-9719-4c4eada4beaa",
                          "geometry": {"type": "Point", "coordinates": [9.221, 30.0]}
                          }])


def check_timeseries(url, params):
    r = requests.get(
        url,
        params=params)
    r.raise_for_status()
    return r


def send_message(producer):
    if producer is None:
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    future = producer.send(topic, data)
    future.get(60)


def increment_and_wait(counter, wait=5):
    time.sleep(wait)
    counter += 1


counter = 0
producer = None
topic = 'test'

while not test_passed or counter > 20:
    try:
        send_message(producer)

        r = check_timeseries(f"{base_url}/sensors/{windSpeed}/timeseries", {
            'after': '2000-01-01T00:00:00Z',
            'before': '2100-01-01T00:00:00Z'
        })

        if len(r.json()['data']) > 0:
            test_passed = True
            print('OK')
        else:
            print('timeseries empty, retrying...')
            increment_and_wait(counter)
    except Exception as ex:
        print(ex)
        increment_and_wait(counter)

if not test_passed:
    print('FAILED')
    sys.exit(1)
