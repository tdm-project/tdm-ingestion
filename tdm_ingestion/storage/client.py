from abc import ABC, abstractmethod
from typing import List, Dict, AnyStr

from tdm_ingestion.ingestion import TimeSeries
from tdm_ingestion.models import SensorType, Sensor


class Client(ABC):
    class NotFound(Exception):
        pass

    @abstractmethod
    def create_sensor_type(self, sensor_types: List[SensorType]) -> List[
        AnyStr]:
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
