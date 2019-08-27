import logging
import os
from abc import ABC, abstractmethod
from typing import AnyStr, Dict, List, Union

import requests
from tdm_ingestion.models import Sensor, SensorType, TimeSeries, Model
from tdm_ingestion.storage.base import Client as BaseClient

logger = logging.getLogger(__name__)


class Http(ABC):
    @abstractmethod
    def post(self, url: AnyStr, json: Union[List, Dict, str] = None,
             headers: Dict[str, str] = None
             ) -> Union[List, Dict]:
        pass

    @abstractmethod
    def get(self, url, params: Union[List, Dict] = None,
            headers: Dict[str, str] = None
            ) -> Union[List, Dict]:
        pass


class Requests(Http):
    def post(self, url: AnyStr, json: Union[List, Dict, str] = None,
             headers: Dict[str, str] = None
             ) -> Union[List, Dict]:
        json = json or {}
        logging.debug("doing POST with url %s and json %s", url, json)
        headers = headers or {}
        headers['content-type'] = 'application/json'
        r = requests.post(url, data=json, headers=headers)
        r.raise_for_status()
        return r.json()

    def get(self, url, params: Union[List, Dict] = None,
            headers: Dict[str, str] = None
            ) -> Union[List, Dict]:
        params = params or {}
        headers = headers or {}
        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()
        return r.json()


class Client(BaseClient):

    def __init__(self, http_client, url, api_version='v0.0'):
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

    def create_entity_types(self, sensor_types: List[SensorType]
                            ) -> List[AnyStr]:
        return self.http.post(self.entity_types_url,
                              Model.list_to_json(sensor_types))

    def create_sources(self, sensors: List[Sensor]) -> List[AnyStr]:
        logger.debug('create_sources %s', Model.list_to_json(sensors))
        return self.http.post(self.sources_url,
                              Model.list_to_json(sensors))

    def create_time_series(self, time_series: List[TimeSeries]):
        return self.http.post(self.records_url,
                              Model.list_to_json(time_series))

    def get_entity_types(self, _id: AnyStr = None,
                         query: Dict = None
                         ) -> Union[SensorType, List[SensorType]]:
        raise NotImplementedError

    def get_sources(self, _id: AnyStr = None, query: Dict = None
                    ) -> Union[Sensor, List[Sensor]]:
        raise NotImplementedError

    def sources_count(self, query):
        return len(self.http.get(self.sources_url, params=query)) > 0

    def sensor_types_count(self, query: Dict):
        return len(self.http.get(self.entity_types_url, params=query)) > 0
