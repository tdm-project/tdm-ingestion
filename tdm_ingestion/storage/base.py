from abc import ABC, abstractmethod
from typing import List, Dict, AnyStr
from typing import Union, Type, Set

from tdm_ingestion.ingestion import Storage as BaseStorage
from tdm_ingestion.ingestion import TimeSeries
from tdm_ingestion.models import SensorType, Sensor
from tdm_ingestion.utils import import_class


class Client(ABC):
    class NotFound(Exception):
        pass

    @abstractmethod
    def create_entity_types(self,
                            sensor_types: List[SensorType]) -> List[AnyStr]:
        pass

    @abstractmethod
    def create_sources(self, sensors: List[Sensor]) -> List[AnyStr]:
        pass

    @abstractmethod
    def create_time_series(self, time_series: List[TimeSeries]):
        pass

    @abstractmethod
    def get_entity_types(self, _id: AnyStr = None,
                         query: Dict = None) -> SensorType:
        pass

    @abstractmethod
    def get_sources(self, _id: AnyStr = None, query: Dict = None) -> Sensor:
        pass

    @abstractmethod
    def sources_count(self, query: Dict) -> int:
        pass

    @abstractmethod
    def sensor_types_count(self, query: Dict) -> int:
        pass


class CachedStorage(BaseStorage):
    def __init__(self, client: Client):
        self.client = client
        self._cache: Set = set()

    @classmethod
    def create_from_json(cls, json: Dict):
        client = json['client']
        return CachedStorage(
            import_class(client['class']).create_from_json(client['args']))

    def _idempotent_create(self, obj: Union[SensorType, Sensor]):
        if obj.name not in self._cache:
            if self.client.sources_count(query={'name': obj.name}) <= 0:
                self.client.create_sources([obj])
            self._cache.add(obj.name)

    def write(self, time_series: List[TimeSeries]):
        if time_series:
            for ts in time_series:
                self._idempotent_create(ts.source)

            self.client.create_time_series(time_series)
