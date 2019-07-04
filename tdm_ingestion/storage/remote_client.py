import logging
import os
from abc import ABC, abstractmethod
from typing import AnyStr, Dict, List, Union

import requests
from tdm_ingestion.models import Sensor, SensorType, TimeSeries, Model
from tdm_ingestion.storage.base import Client as BaseClient


class Http(ABC):
    @abstractmethod
    def post(self, url: AnyStr, json: Union[List, Dict, str] = None
             ) -> Union[List, Dict]:
        pass

    @abstractmethod
    def get(self, url, params: Union[List, Dict] = None
            ) -> Union[List, Dict]:
        pass


class Requests(Http):
    def post(self, url: AnyStr, json: Union[List, Dict, str] = None
             ) -> Union[List, Dict]:
        json = json or {}
        logging.debug("doing POST with url %s and json %s", url, json)
        r = requests.post(url, data=json,
                          headers={'content-type': 'application/json'}
                          )
        r.raise_for_status()
        return r.json()

    def get(self, url, params: Union[List, Dict] = None
            ) -> Union[List, Dict]:
        params = params or {}
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()


# class RemoteModel:
#    def __init__(self, url, client):
#        self.url = url
#        self.client = client
#
#    def __getattr__(self, item):
#        if item in self.__dict__:
#            if self.__dict__[item]:
#                return self.__dict__[item]
#            else:
#                self.__dict__.update(
#                    self.client(self.url, {'name': self.name})[0])
#        else:
#            raise AttributeError
#
#
# class RemoteSensorType(RemoteModel, SensorType):
#    def __init__(self, name: AnyStr, url: AnyStr, client):
#        super(SensorType).__init__(name, None, None)
#        super(RemoteModel).__init__(url, client)


class Client(BaseClient):

    def __init__(self, http_client, url, api_version='v0.0'):
        self.http = http_client
        self.url = url
        logging.debug("url %s", self.url)
        self.api_version = api_version
        self.sensor_types_url = os.path.join(self.url,
                                             f'api/{api_version}/sensor_types')
        self.sensors_url = os.path.join(self.url,
                                        f'api/{api_version}/sensors')
        self.measures_url = os.path.join(self.url,
                                         f'api/{api_version}/measures')

    @staticmethod
    def create_from_json(json: Dict):
        logging.debug("building Client with %s", json)
        # TODO works only with Requests client
        return Client(Requests(), json['url'])

    def create_sensor_types(self, sensor_types: List[SensorType]
                            ) -> List[AnyStr]:
        return self.http.post(self.sensor_types_url,
                              Model.list_to_json(sensor_types))

    def create_sensors(self, sensors: List[Sensor]) -> List[AnyStr]:
        return self.http.post(self.sensors_url,
                              Model.list_to_json(sensors))

    def create_time_series(self, time_series: List[TimeSeries]):
        return self.http.post(self.measures_url,
                              Model.list_to_json(time_series))

    def get_sensor_type(self, _id: AnyStr = None,
                        query: Dict = None
                        ) -> Union[SensorType, List[SensorType]]:
        raise NotImplementedError

    def get_sensors(self, _id: AnyStr = None, query: Dict = None
                    ) -> Union[Sensor, List[Sensor]]:
        raise NotImplementedError

    def sensors_count(self, query):
        return len(self.http.get(self.sensors_url, params=query)) > 0

    def sensor_types_count(self, query: Dict):
        return len(self.http.get(self.sensor_types_url, params=query)) > 0
