import logging
import os
from typing import Dict, Union, List, Type, Set

import aiohttp
from tdm_ingestion.ingestion import Storage
from tdm_ingestion.models import SensorType, Model, Sensor, TimeSeries
from tdm_ingestion.storage.remote_client import Http, Client
from tdm_ingestion.utils import import_class

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class AioHttp(Http):
    def __init__(self):
        self.session = aiohttp.ClientSession()

    async def post(self, url: str, json: Union[List, Dict, str] = None
                   ) -> Union[List, Dict]:
        json = json or {}
        logger.debug("doing POST with url %s and json %s", url, json)
        r = await self.session.post(url, data=json,
                                    headers={
                                        'content-type': 'application/json'}
                                    )
        r.raise_for_status()
        return await r.json()

    async def get(self, url, params: Union[List, Dict] = None
                  ) -> Union[List, Dict]:
        params = params or {}
        resp = await self.session.get(url, params=params)
        resp.raise_for_status()
        return await resp.json()


class AsyncClient(Client):

    @staticmethod
    def create_from_json(json: Dict):
        logging.debug("building Client with %s", json)
        return AsyncClient(AioHttp(), json['url'])

    async def create_sensor_types(self, sensor_types: List[SensorType]
                                  ) -> List[str]:
        logger.debug("create_sensor_types")
        return await self.http.post(self.sensor_types_url,
                                    Model.list_to_json(sensor_types))

    async def create_sensors(self, sensors: List[Sensor]) -> List[str]:
        logger.debug("create_sensors")
        return await self.http.post(self.sensors_url,
                                    Model.list_to_json(sensors))

    async def create_time_series(self, time_series: List[TimeSeries]):
        logger.debug("create_time_series")
        return await self.http.post(self.measures_url,
                                    Model.list_to_json(time_series))
    async def sensors_count(self, query):
        return len(await self.http.get(self.sensors_url, params=query)) > 0

    async def sensor_types_count(self, query: Dict):
        return len(await self.http.get(self.sensor_types_url, params=query)) > 0


class AsyncCachedStorage(Storage):
    #FIXME duplicated code
    def __init__(self, client: AsyncClient):
        self.client = client
        self._cache: Dict[Type, Set[str]] = {SensorType: set(),
                                             Sensor: set()}

    @classmethod
    def create_from_json(cls, json: Dict):
        client = json['client']
        return AsyncCachedStorage(
            import_class(client['class']).create_from_json(client['args']))

    async def _idempotent_create(self, obj: Union[SensorType, Sensor]):
        if obj.name not in self._cache[obj.__class__]:
            query = {'name': obj.name}
            if isinstance(obj, Sensor):
                count_method = self.client.sensors_count
                create_method = self.client.create_sensors
            else:
                count_method = self.client.sensor_types_count
                create_method = self.client.create_sensor_types

            if await count_method(query=query) <= 0:
                await create_method([obj])
            self._cache[obj.__class__].add(obj.name)

    async def write(self, time_series: List[TimeSeries]):
        if time_series:
            for ts in time_series:
                logger.debug(f"try create sensor sensor type for ts {ts}")
                for obj in [ts.sensor.type, ts.sensor]:
                    await self._idempotent_create(obj)

            await self.client.create_time_series(time_series)
