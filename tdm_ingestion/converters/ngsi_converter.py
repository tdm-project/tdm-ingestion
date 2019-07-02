import datetime
import json
import logging
import re
from collections import OrderedDict, defaultdict
from typing import List, Tuple, Dict

from tdm_ingestion.ingestion import MessageConverter, Message
from tdm_ingestion.models import TimeSeries, ValueMeasure, Geometry, Point, \
    SensorType, Sensor


class NgsiConverter(MessageConverter):
    non_properties = {'latitude', 'longitude', 'timestamp', 'dateObserved',
                      'location'}
    to_skip = {'dateObserved', 'location', 'latitude', 'longitude'}

    @staticmethod
    def _get_geometry(msg: dict) -> Geometry:
        geom = {}
        for attr in msg['body']['attributes']:
            if attr['name'] in {'latitude', 'longitude'}:
                geom[attr['name']] = float(attr['value'])
        return Point(geom['latitude'], geom['longitude'])

    @staticmethod
    def _get_names(msg: Dict) -> Tuple[str, str, str, str]:
        p = re.compile(
            r'(?P<Type>\w+):(?P<Edge>[a-zA-Z0-9_-]+)\.(?P<Node>[a-zA-Z0-9_-]+)'
            r'\.(?P<Sensor>[a-zA-Z0-9_-]+)')

        if p:
            st_type, node_name, station_name, st_name = p.search(
                msg['body']['id']).groups()

            sensor_name = '{}.{}'.format(station_name, st_name)
            return node_name, st_name, st_type, sensor_name
        else:
            raise RuntimeError(f'invalid id {msg["body"]["id"]}')

    def _create_sensor_type(self, st_name: str, st_type: str,
                            properties: List[str]) -> SensorType:
        return SensorType(st_name, st_type, properties)

    def _create_sensor(self, sensor_name: str, sensor_type: SensorType,
                       node_name: str, geometry: Geometry) -> Sensor:
        return Sensor(sensor_name, sensor_type, node_name,
                      geometry)

    def _create_models(self, msg: Dict) -> List[TimeSeries]:
        node_name, st_name, st_type, sensor_name = NgsiConverter._get_names(
            msg)

        properties = self._get_properties(msg)

        sensor_type = self._create_sensor_type(st_name, st_type,
                                               properties)

        geometry = NgsiConverter._get_geometry(msg)

        measures: Dict = OrderedDict()
        time = None
        for attr in msg['body']['attributes']:
            name = attr['name']
            value = attr['value']
            if value is not None and str(
                    value).strip() and name not in self.to_skip:
                if name == 'timestamp':
                    time = datetime.datetime.fromtimestamp(
                        float(value), datetime.timezone.utc
                    )
                else:
                    measures[name] = float(value)

        time_series_list = []
        for measure, value in measures.items():
            sensor = self._create_sensor(f"{sensor_name}.{measure}",
                                         sensor_type, node_name,
                                         geometry)

            time_series_list.append(
                TimeSeries(time, sensor, ValueMeasure(value)))

        return time_series_list

    def convert(self, messages: List[Message]) -> List[TimeSeries]:

        logging.debug("messages %s", len(messages))
        timeseries_list: List = []
        for m in messages:
            try:
                m_dict = json.loads(m.value)
            except json.decoder.JSONDecodeError:
                logging.error('skipping message %s, error while jsondecoding',
                              m.value)
                continue

            timeseries_list += self._create_models(m_dict)

        return timeseries_list

    @staticmethod
    def _get_properties(msg: Dict) -> List[str]:
        return [attr['name'] for attr in msg['body']['attributes']
                if attr['name'] not in NgsiConverter.non_properties]


class CachedNgsiConverter(NgsiConverter):
    def __init__(self):
        self.sensor_types: Dict[str, SensorType] = defaultdict()
        self.sensors: Dict[str, Sensor] = defaultdict()

    def _create_sensor(self, sensor_name: str, sensor_type: SensorType,
                       node_name: str, geometry: Geometry):
        return self.sensors.setdefault(sensor_name,
                                       Sensor(sensor_name, sensor_type,
                                              node_name, geometry))

    def _create_sensor_type(self, st_name: str, st_type: str,
                            properties: List[str]):
        return self.sensor_types.setdefault(st_name,
                                            SensorType(st_name, st_type,
                                                       properties))
