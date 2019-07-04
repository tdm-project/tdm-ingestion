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
    def create_sensor_types(self,
                            sensor_types: List[SensorType]) -> List[AnyStr]:
        pass

    @abstractmethod
    def create_sensors(self, sensors: List[Sensor]) -> List[AnyStr]:
        pass

    @abstractmethod
    def create_time_series(self, time_series: List[TimeSeries]):
        pass

    @abstractmethod
    def get_sensor_type(self, _id: AnyStr = None,
                        query: Dict = None) -> SensorType:
        pass

    @abstractmethod
    def get_sensors(self, _id: AnyStr = None, query: Dict = None) -> Sensor:
        pass

    @abstractmethod
    def sensors_count(self, query: Dict) -> int:
        pass

    @abstractmethod
    def sensor_types_count(self, query: Dict) -> int:
        pass


class CachedStorage(BaseStorage):
    def __init__(self, client: Client):
        self.client = client

        self._cache: Dict[Type, Set[AnyStr]] = {SensorType: set(),
                                                Sensor: set()}

    @staticmethod
    def create_from_json(json: Dict):
        client = json['client']
        return CachedStorage(
            import_class(client['class']).create_from_json(client['args']))

    def _idempotent_create(self, obj: Union[SensorType, Sensor]):
        if obj.name not in self._cache[obj.__class__]:
            query = {'name': obj.name}
            if isinstance(obj, Sensor):
                count_method = self.client.sensors_count
                create_method = self.client.create_sensors
            else:
                count_method = self.client.sensor_types_count
                create_method = self.client.create_sensor_types

            if count_method(query=query) <= 0:
                create_method([obj])
            self._cache[obj.__class__].add(obj.name)

    def write(self, time_series: List[TimeSeries]):
        if time_series:
            for ts in time_series:
                for obj in [ts.sensor.type, ts.sensor]:
                    self._idempotent_create(obj)

            self.client.create_time_series(time_series)
