import datetime
import json
import random
import uuid
from typing import List

from ingestion import Consumer, Storage, TimeSeries, Message, MessageConverter


class DummyStorage(Storage):
    def __init__(self):
        self.messages = []

    def write(self, messages: List[TimeSeries]):
        self.messages += messages


class DummyConsumer(Consumer):
    message = {
        "headers": [{"fiware-service": "tdm"}, {"fiware-servicePath": "/cagliari/edge/meteo"},
                    {"timestamp": 1531774294021}],
        "body": {
            "attributes": [
                {"name": "barometricPressure", "type": "float", "value": " "},
                {"name": "dateObserved", "type": "String", "value": "2018-07-16T20:51:33+00:00"},
                {"name": "location", "type": "geo:point", "value": "39.2479168, 9.1329701"},
                {"name": "timestamp", "type": "Integer", "value": "1531774293"},
                {"name": "windDirection", "type": "Float", "value": "174.545"},
                {"name": "windSpeed", "type": "Float", "value": "0.000"},
                {"name": "latitude", "type": "string", "value": "39.2479168"},
                {"name": "longitude", "type": "string", "value": "9.1329701"}
            ],
            "type": "WeatherObserved",
            "isPattern": "false", "id": "WeatherObserved:Edge-CFA703F4.esp8266-7806085.Davis"
        }
    }


    def poll(self, timeout_ms=0, max_records=0) -> List[Message]:
        return [Message('key', json.dumps(DummyConsumer.message))]


class DummyConverter(MessageConverter):
    def convert(self, messages: List[Message]) -> List[TimeSeries]:
        series = []
        for m in messages:
            m = json.loads(m.value)
            series.append(TimeSeries(datetime.datetime.now(), uuid.uuid4(), random.random()))

        return series
