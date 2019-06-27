from typing import List, Dict, AnyStr, Union, Type, Set

from tdm_ingestion.ingestion import Storage as BaseStorage, TimeSeries
from tdm_ingestion.models import SensorType, Sensor
from tdm_ingestion.storage.client import Client


class Storage(BaseStorage):
    def __init__(self, client: Client):
        self.client = client

        self._cache: Dict[Type, Set[AnyStr]] = {SensorType: set(),
                                                Sensor: set()}

    def _idempotent_create(self, obj: Union[SensorType, Sensor]):
        if not obj.name in self._cache[obj.__class__]:
            query = {'name': obj.name}
            if isinstance(obj, Sensor):
                count_method = self.client.sensors_count
                create_method = self.client.create_sensors
            else:
                count_method = self.client.sensor_types_count
                create_method = self.client.create_sensor_type

            if count_method(query=query) <= 0:
                create_method([obj])
            self._cache[obj.__class__].add(obj.name)

    def write(self, time_series: List[TimeSeries]):
        if time_series:
            for ts in time_series:
                for obj in [ts.sensor.type, ts.sensor]:
                    self._idempotent_create(obj)

            self.client.create_time_series(time_series)
