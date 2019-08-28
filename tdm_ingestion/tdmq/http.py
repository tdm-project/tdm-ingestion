import logging
import os
from typing import Dict, List, Union, Any

from tdm_ingestion.http.base import Http
from tdm_ingestion.http.requests import Requests
from tdm_ingestion.models import EntityType, Model, TimeSeries, Source
from tdm_ingestion.tdmq.base import Client as BaseClient


class Client(BaseClient):

    def __init__(self, http_client: Http, url, api_version='v0.0'):
        self.http = http_client
        self.url = url
        logging.debug("url %s", self.url)
        self.api_version = api_version
        self.entity_types_url = os.path.join(self.url,
                                             f'api/{api_version}/entity_types')
        self.sources_url = os.path.join(self.url,
                                        f'api/{api_version}/sources')
        self.records_url = os.path.join(self.url,
                                        f'api/{api_version}/records')

    @staticmethod
    def create_from_json(json: Dict):
        logging.debug("building Client with %s", json)
        # TODO works only with Requests client
        return Client(Requests(), json['url'])

    def create_entity_types(self, sensor_types: List[EntityType]
                            ) -> List[str]:
        return self.http.post(self.entity_types_url,
                              Model.list_to_json(sensor_types))

    def create_sources(self, sensors: List[Source]) -> List[str]:
        logging.debug('create_sources %s', Model.list_to_json(sensors))
        return self.http.post(self.sources_url,
                              Model.list_to_json(sensors))

    def create_time_series(self, time_series: List[TimeSeries]):
        return self.http.post(self.records_url,
                              Model.list_to_json(time_series))

    def get_time_series(self, source: Source, query: Dict[str, Any]) -> List[
        TimeSeries]:
        return self.http.get(f'{self.sources_url}/{source.tdmq_id}/timeseries',
                             params=query)

    def get_entity_types(self, _id: str = None,
                         query: Dict = None
                         ) -> Union[EntityType, List[EntityType]]:
        raise NotImplementedError

    def get_sources(self, _id: str = None, query: Dict = None
                    ) -> Union[Source, List[Source]]:
        raise NotImplementedError

    def sources_count(self, query):
        return len(self.http.get(self.sources_url, params=query)) > 0

    def entity_types_count(self, query: Dict):
        return len(self.http.get(self.entity_types_url, params=query)) > 0


class AsyncClient(Client):

    @staticmethod
    def create_from_json(json: Dict):
        # FIXME it
        from tdm_ingestion.http.asyncio import AioHttp
        logging.debug("building Client with %s", json)
        return AsyncClient(AioHttp(), json['url'])

    async def create_entity_types(self, sensor_types: List[EntityType]
                                  ) -> List[str]:
        logging.debug("create_sensor_types")
        return await self.http.post(self.entity_types_url,
                                    Model.list_to_json(sensor_types))

    async def create_sources(self, sensors: List[Source]) -> List[str]:
        logging.debug("create_sensors")
        return await self.http.post(self.sources_url,
                                    Model.list_to_json(sensors))

    async def create_time_series(self, time_series: List[TimeSeries]):
        logging.debug("create_time_series")
        return await self.http.post(self.records_url,
                                    Model.list_to_json(time_series))

    async def sources_count(self, query):
        return len(await self.http.get(self.sources_url, params=query)) > 0

    async def entity_types_count(self, query: Dict):
        return len(
            await self.http.get(self.entity_types_url, params=query)) > 0
