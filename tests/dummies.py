import datetime
import datetime
import json
import random
import uuid
from collections import defaultdict
from typing import List, AnyStr, Dict, Any, Union

from tdm_ingestion.ingestion import Consumer, Storage, Record, \
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

    def write(self, messages: List[Record]):
        self.messages += messages


class DummyTDMQClient(Client):

    def __init__(self):
        self.sources = {}
        self.sources_by_types = defaultdict(list)
        self.entity_types = {}
        self.time_series = defaultdict(list)

    def sources_count(self, query: Dict) -> int:
        try:
            return len([self.sources[query['name']]])
        except KeyError:
            return 0

    def entity_types_count(self, query: Dict) -> int:
        try:
            return len([self.entity_types[query['name']]])
        except KeyError:
            return 0

    def create_entity_types(self, sensor_types: List[EntityType]) -> List[
        AnyStr]:
        self.entity_types.update({s.name: s for s in sensor_types})
        return [s.name for s in sensor_types]

    def create_sources(self, sources: List[Source]) -> List[AnyStr]:
        for source in sources:
            self.sources[source._id] = source
            self.sources_by_types[source.type.name].append(source)
        return [s._id for s in sources]

    def create_time_series(self, time_series: List[Record]):
        for ts in time_series:
            self.time_series[ts.source._id].append(ts)
            self.create_sources([ts.source])

    def get_time_series(self, source: Source, query: Dict[str, Any]) -> List[
        Record]:
        return self.time_series[source._id]

    def get_entity_types(self, _id: AnyStr = None,
                         query: Dict = None) -> EntityType:
        """
            only query by name is supported
        """
        k = _id if _id else query['name']
        try:
            return self.entity_types[k]
        except KeyError:
            raise Client.NotFound

    def get_sources(self, _id: AnyStr = None, query: Dict = None) -> Union[
        Source, List[Source]]:
        if _id is None and query is None:
            return list(self.sources.values())
        elif _id:
            try:
                return self.sources[_id]
            except KeyError:
                raise Client.NotFound
        elif query:
            if 'entity_type' in query:
                return self.sources_by_types[query['entity_type']]


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

    def poll(self, timeout_ms=0, max_records=0) -> List[str]:
        return [json.dumps(DummyConsumer.message)]


class DummyConverter(MessageConverter):
    def convert(self, messages: List[str]) -> List[Record]:
        series = []
        for m in messages:
            m = json.loads(m)
            series.append(Record(datetime.datetime.now(), uuid.uuid4(),
                                 random.random()))

        return series


class AsyncDummyConsumer(Consumer):
    def __init__(self):
        self.consumer = DummyConsumer()

    async def poll(self, timeout_s: int = -1, max_records: int = -1) -> List[
        str]:
        return self.consumer.poll(timeout_s, max_records)


class AsyncDummyStorage(Storage):
    @staticmethod
    def create_from_json(json: Dict) -> "Storage":
        raise NotImplementedError

    def __init__(self):
        self.messages = []

    async def write(self, messages: List[Record]):
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
