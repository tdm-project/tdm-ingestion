import logging
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
                get_method = self.client.get_sensor
                create_method = self.client.create_sensors
            else:
                get_method = self.client.get_sensor_type
                create_method = self.client.create_sensor_type

            try:
                get_method(query=query)
            except Client.NotFound as ex:
                logging.error('query %s on %s returns not found', query,
                              obj.__class__.__name__)
                create_method([obj])
            self._cache[obj.__class__].add(obj.name)

    def write(self, time_series: List[TimeSeries]):
        if time_series:
            for ts in time_series:
                for obj in [ts.sensor.type, ts.sensor]:
                    self._idempotent_create(obj)

            self.client.create_time_series(time_series)
