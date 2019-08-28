import datetime
import datetime
import json
import random
import uuid
from typing import List, AnyStr, Dict, Any

from tdm_ingestion.ingestion import Consumer, Storage, TimeSeries, Message, \
    MessageConverter
from tdm_ingestion.models import Source, EntityType
from tdm_ingestion.storage.ckan import CkanClient
from tdm_ingestion.tdmq import Client


class DummyStorage(Storage):
    @staticmethod
    def create_from_json(json: Dict) -> "Storage":
        raise NotImplementedError

    def __init__(self):
        self.messages = []

    def write(self, messages: List[TimeSeries]):
        self.messages += messages


class DummyClient(Client):

    def __init__(self):
        self.sensors = {}
        self.sensor_types = {}
        self.time_series = []

    def sources_count(self, query: Dict) -> int:
        try:
            return len([self.sensors[query['name']]])
        except KeyError:
            return 0

    def entity_types_count(self, query: Dict) -> int:
        try:
            return len([self.sensor_types[query['name']]])
        except KeyError:
            return 0

    def create_entity_types(self, sensor_types: List[EntityType]) -> List[
        AnyStr]:
        self.sensor_types.update({s.name: s for s in sensor_types})
        return [s.name for s in sensor_types]

    def create_sources(self, sensors: List[Source]) -> List[AnyStr]:
        self.sensors.update({s._id: s for s in sensors})
        return [s._id for s in sensors]

    def create_time_series(self, time_series: List[TimeSeries]):
        self.time_series += time_series

    def get_entity_types(self, _id: AnyStr = None,
                         query: Dict = None) -> EntityType:
        """
            only query by name is supported
        """
        k = _id if _id else query['name']
        try:
            return self.sensor_types[k]
        except KeyError:
            raise Client.NotFound

    def get_sources(self, _id: AnyStr = None, query: Dict = None) -> Source:
        """
            only query by name is supported
        """
        k = _id if _id else query['name']
        try:
            return self.sensors[k]
        except KeyError:
            raise Client.NotFound


class DummyConsumer(Consumer):
    message = {
        "headers": [{"fiware-service": "tdm"},
                    {"fiware-servicePath": "/cagliari/edge/meteo"},
                    {"timestamp": 1531774294021}],
        "body": {
            "attributes": [
                {"name": "barometricPressure", "type": "float", "value": " "},
                {"name": "dateObserved", "type": "String",
                 "value": "2018-07-16T20:51:33+00:00"},
                {"name": "location", "type": "geo:point",
                 "value": "39.2479168, 9.1329701"},
                {"name": "timestamp", "type": "Integer",
                 "value": "1531774293"},
                {"name": "windDirection", "type": "Float", "value": "174.545"},
                {"name": "windSpeed", "type": "Float", "value": "0.000"},
                {"name": "latitude", "type": "string", "value": "39.2479168"},
                {"name": "longitude", "type": "string", "value": "9.1329701"}
            ],
            "type": "WeatherObserved",
            "isPattern": "false",
            "id": "WeatherObserved:Edge-CFA703F4.esp8266-7806085.Davis"
        }
    }

    def poll(self, timeout_ms=0, max_records=0) -> List[Message]:
        return [Message('key', json.dumps(DummyConsumer.message))]


class DummyConverter(MessageConverter):
    def convert(self, messages: List[Message]) -> List[TimeSeries]:
        series = []
        for m in messages:
            m = json.loads(m.value)
            series.append(TimeSeries(datetime.datetime.now(), uuid.uuid4(),
                                     random.random()))

        return series


class AsyncDummyConsumer(Consumer):
    def __init__(self):
        self.consumer = DummyConsumer()

    async def poll(self, timeout_s: int = -1, max_records: int = -1
                   ) -> List[Message]:
        return self.consumer.poll(timeout_s, max_records)


class AsyncDummyStorage(Storage):
    @staticmethod
    def create_from_json(json: Dict) -> "Storage":
        raise NotImplementedError

    def __init__(self):
        self.messages = []

    async def write(self, messages: List[TimeSeries]):
        self.messages += messages


class DummyCkan(CkanClient):
    def __init__(self):
        self.resources = {}

    def create_resource(self, resource: str, dataset: str,
                        records: List[Dict[str, Any]],
                        upsert: bool = False) -> None:
        self.resources[resource] = dict(
            dataset=dataset,
            records=records
        )
