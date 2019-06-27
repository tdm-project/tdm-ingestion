import os
from abc import ABC, abstractmethod
from typing import AnyStr, Dict, List, Union

import requests
from tdm_ingestion.models import Sensor, SensorType, TimeSeries
from tdm_ingestion.storage.client import Client as BaseClient


class Http(ABC):
    @abstractmethod
    def post(self, url: AnyStr, json: Union[List, Dict] = None) -> Union[
        List, Dict]:
        pass

    @abstractmethod
    def get(self, url, params: Union[List, Dict] = None) -> Union[
        List, Dict]:
        pass


class Requests(Http):
    def post(self, url: AnyStr, json: Union[List, Dict] = None) -> Union[
        List, Dict]:
        json = json or {}
        r = requests.post(url, json=json)
        r.raise_for_status()
        return r.json()

    def get(self, url, params: Union[List, Dict] = None) -> Union[
        List, Dict]:
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
    def __init__(self, client, url, api_version='v0.0'):
        self.client = client
        self.url = url
        self.api_version = api_version
        self.sensor_types_url = os.path.join(self.url,
                                             f'api/{api_version}/sensor_types')
        self.sensors_url = os.path.join(self.url,
                                        f'api/{api_version}/sensors')
        self.measures_url = os.path.join(self.url,
                                         f'api/{api_version}/measures')

    def create_sensor_type(self, sensor_types: List[SensorType]) -> List[
        AnyStr]:
        return self.client.post(os.path.join(self.url, self.sensor_types_url),
                                [st.to_json() for st in sensor_types])

    def create_sensors(self, sensors: List[Sensor]) -> List[AnyStr]:
        return self.client.post(self.sensors_url,
                                [s.to_json() for s in sensors])

    def create_time_series(self, time_series: List[TimeSeries]):
        return self.client.post(self.measures_url,
                                [ts.to_json() for ts in time_series])

    def get_sensor_type(self, _id: AnyStr = None,
                        query: Dict = None) -> Union[
        SensorType, List[SensorType]]:
        raise NotImplementedError

    #        if _id:
    #            data = self.client.get(os.path.join(self.sensors_type_url, _id))
    #            return SensorType(**data)
    #        else:
    #            res = self.client.get(os.path.join(self.sensors_type_url),
    #                                  params=query)
    #            return [SensorType(**data) for data in res]

    def get_sensor(self, _id: AnyStr = None, query: Dict = None) -> Union[
        Sensor, List[Sensor]]:
        raise NotImplementedError

    def sensor_exists(self, query):
        return len(self.client.get(self.sensors_url, params=query)) > 0

    def sensor_type_exists(self, query: Dict):
        return len(self.client.get(self.sensor_types_url, params=query)) > 0
