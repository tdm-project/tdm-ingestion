import datetime
import json
import random
import uuid
from collections import defaultdict
from typing import Any, AnyStr, Dict, List, Union

from tdm_ingestion.consumers.base import BaseKafkaConsumer
from tdm_ingestion.http_client.base import Http
from tdm_ingestion.storage.ckan import CkanClient
from tdm_ingestion.tdmq import Client
from tdm_ingestion.tdmq.models import EntityType, Record, Source


class DummyTDMQStorage:

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

    def create_entity_types(self, sensor_types: List[EntityType]) -> List[AnyStr]:
        self.entity_types.update({s.name: s for s in sensor_types})
        return [s.name for s in sensor_types]

    def create_sources(self, sensors: List[Source]) -> List[AnyStr]:
        for sensor in sensors:
            self.sources[sensor.id_] = sensor
            self.sources_by_types[sensor.type.name].append(sensor)
        return [s.id_ for s in sensors]

    def create_time_series(self, time_series: List[Record]):
        for ts in time_series:
            self.time_series[ts.source.id_].append(ts)
            # self.create_sources([ts.source])

    def get_time_series(self, source: Source, query: Dict[str, Any]) -> List[
            Record]:
        return self.time_series[source.id_]

    def get_entity_types(self, id_: AnyStr = None,
                         query: Dict = None) -> EntityType:
        """
            only query by name is supported
        """
        k = id_ if id_ else query['name']
        try:
            return self.entity_types[k]
        except KeyError:
            raise Client.NotFound

    def get_sources(self, id_: AnyStr = None, query: Dict = None) -> Union[
            Source, List[Source]]:
        if id_ is None and query is None:
            return list(self.sources.values())
        elif id_:
            try:
                return self.sources[id_]
            except KeyError:
                raise Client.NotFound
        elif query:
            if 'entity_type' in query:
                return self.sources_by_types[query['entity_type']]


class DummyKafkaMessage:
    def __init__(self, value, error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error


class DummyKafkaError:
    pass


class DummyConfluentConsumer:

    def __init__(self, value, error, *args, **kwargs):
        self.messages = [DummyKafkaMessage(value, error)]

    def consume(self, timeout_s=0, max_records=0) -> List[str]:
        return self.messages

    def subscribe(self, topics):
        pass


class DummyConfluentConsumerCorrectMessages(DummyConfluentConsumer):
    def __init__(self, *args, **kwargs):
        correct_message = {
            "headers": [{"fiware-service": "tdm"},
                        {"fiware-servicePath": "/cagliari/edge/meteo"},
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
                "isPattern": "false",
                "id": "WeatherObserved:Edge-z.Station2.Davis"
            }
        }
        super(DummyConfluentConsumerCorrectMessages, self).__init__(correct_message, None, *args, **kwargs)


class DummyConfluentConsumerErrorMessages(DummyConfluentConsumer):
    def __init__(self, *args, **kwargs):
        super(DummyConfluentConsumerErrorMessages, self).__init__(None, DummyKafkaError(), *args, **kwargs)


class DummyConverter:
    def convert(self, messages: List[str]) -> List[Record]:
        series = []
        for m in messages:
            m = json.loads(m)
            series.append(Record(datetime.datetime.now(), uuid.uuid4(),
                                 random.random()))

        return series


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


class DummyHttp(Http):
    def post(self, url: str, json: Union[List, Dict, str] = None,
             headers: Dict[str, str] = None) -> Union[List, Dict]:
        pass

    def get(self, url, params: Union[List, Dict] = None,
            headers: Dict[str, str] = None) -> Union[List, Dict]:
        pass
