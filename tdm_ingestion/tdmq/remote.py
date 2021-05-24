import datetime
import logging
import os
from typing import Any, Dict, List, Union
from urllib.parse import urljoin

from requests.exceptions import HTTPError

from tdm_ingestion.http_client.base import Http
from tdm_ingestion.http_client.requests import Requests
from tdm_ingestion.tdmq.base import Client as BaseClient
from tdm_ingestion.tdmq.models import EntityType, Model, Point, Record, Source

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


def log_level():
    return logger.getEffectiveLevel()


def set_log_level(level):
    logger.setLevel(level)


class GenericHttpError(Exception):
    """
    Generic exception raised when an error occurs performing an HttpRequest
    """


class DuplicatedEntryError(GenericHttpError):
    """
    Exception to raise when a POST request fails because of duplicated entry
    """


def raise_exception(status_code):
    """
    Raise an exception based on status code in input
    """
    if status_code == 409:
        raise DuplicatedEntryError
    raise GenericHttpError


class Client(BaseClient):

    def __init__(self, url: str, http_client: Http = None, api_version='v0.0', auth_token=None):
        self.http = http_client or Requests(auth_token)
        self.url = url
        logger.debug("tdmq url %s", self.url)
        self.api_version = api_version
        self.entity_types_url = urljoin(self.url, f'api/{api_version}/entity_types')
        self.sources_url = urljoin(self.url, f'api/{api_version}/sources')
        self.records_url = urljoin(self.url, f'api/{api_version}/records')

    def create_entity_types(self, sensor_types: List[EntityType]) -> List[str]:
        logger.debug('create_entity_types %s', Model.list_to_json(sensor_types))
        try:
            return self.http.post(self.entity_types_url, Model.list_to_json(sensor_types))
        except HTTPError as e:
            logger.error('error response from server with status code %s', e.response.status_code)
            raise_exception(e.response.status_code)

    def create_sources(self, sensors: List[Source]) -> List[str]:
        logger.debug('create_sources %s', Model.list_to_json(sensors))
        try:
            return self.http.post(self.sources_url, Model.list_to_json(sensors))
        except HTTPError as e:
            logger.error('error response from server with status code %s', e.response.status_code)
            raise_exception(e.response.status_code)

    def create_time_series(self, time_series: List[Record]):
        logger.debug('creating timeseries %s', Model.list_to_json(time_series))
        try:
            return self.http.post(self.records_url, Model.list_to_json(time_series))
        except HTTPError as e:
            logger.error('error response from server with status code %s', e.response.status_code)
            raise_exception(e.response.status_code)

    def get_time_series(self, source: Source, query: Dict[str, Any] = None) -> List[Record]:
        try:
            time_series = self.http.get(f'{self.sources_url}/{source.tdmq_id}/timeseries', params=query)
        except HTTPError as e:
            logger.error('error response from server with status code %s', e.response.status_code)
            raise_exception(e.response.status_code)

        records: List[Record] = []
        logger.debug('time_series %s', time_series)
        for idx, time in enumerate(time_series['coords']['time']):
            date_time = datetime.datetime.fromtimestamp(time, datetime.timezone.utc)

            try:
                # No support for MultiPoint, just bring the last coordinate pair
                footprint = time_series['coords']['footprint'][idx]["coordinates"][-1]
            except TypeError:
                footprint = time_series['default_footprint']['coordinates']
            # Point(latitude, longitude) but point are returned as [longitude, latitude]
            records.append(Record(date_time, source, Point(footprint[1], footprint[0]), {data: value_list[idx]
                                                      for data, value_list in time_series['data'].items() if value_list}))

        return records

    def get_entity_types(self, id_: str = None, query: Dict = None
                         ) -> Union[EntityType, List[EntityType]]:
        raise NotImplementedError

    def get_sources(self, id_: str = None, query: Dict = None
                    ) -> Union[Source, List[Source]]:
        logger.debug("getting sources from server")
        if id_:
            try:
                source_data = self.http.get(f'{self.sources_url}/{id_}')
                return Source(id_=source_data['external_id'],
                              tdmq_id=source_data['tdmq_id'],
                              type_=EntityType(source_data['entity_type'], source_data['entity_category']),
                              station_model=source_data.get('station_model', ''),
                              geometry=Point(source_data['default_footprint']['coordinates'][1],
                                             source_data['default_footprint']['coordinates'][0]))
            except HTTPError as e:
                logger.error('error response from server with status code %s', e.response.status_code)
                raise_exception(e.response.status_code)
        try:
            return [Source(id_=s['external_id'],
                           tdmq_id=s['tdmq_id'],
                           type_=EntityType(s['entity_type'], s['entity_category']),
                           station_model=s.get('station_model', ''),
                           geometry=Point(s['default_footprint']['coordinates'][1],
                                          s['default_footprint']['coordinates'][0])
                           ) for s in self.http.get(f'{self.sources_url}', params=query)]
        except HTTPError as e:
            logger.error('error response from server with status code %s', e.response.status_code)
            raise_exception(e.response.status_code)

    def sources_count(self, query: Dict = None):
        try:
            return len(self.http.get(self.sources_url, params=query))
        except HTTPError as e:
            raise_exception(e.response.status_code)

    def entity_types_count(self, query: Dict = None):
        try:
            return len(self.http.get(self.entity_types_url, params=query))
        except HTTPError as e:
            raise_exception(e.response.status_code)

class AsyncClient(Client):

    @staticmethod
    def create_from_json(json: Dict):
        # FIXME it
        from tdm_ingestion.http_client.asyncio import AioHttp
        logger.debug("building Client with %s", json)
        return AsyncClient(AioHttp(), json['url'])

    async def create_entity_types(self, sensor_types: List[EntityType]
                                  ) -> List[str]:
        logger.debug("create_sensor_types")
        return await self.http.post(self.entity_types_url,
                                    Model.list_to_json(sensor_types))

    async def create_sources(self, sensors: List[Source]) -> List[str]:
        logger.debug("create_sensors")
        return await self.http.post(self.sources_url,
                                    Model.list_to_json(sensors))

    async def create_time_series(self, time_series: List[Record]):
        logger.debug("create_time_series")
        return await self.http.post(self.records_url,
                                    Model.list_to_json(time_series))

    async def sources_count(self, query):
        return len(await self.http.get(self.sources_url, params=query)) > 0

    async def entity_types_count(self, query: Dict):
        return len(
            await self.http.get(self.entity_types_url, params=query)) > 0
