import datetime
import json
import logging
import re
from collections import defaultdict
from typing import List, Tuple, Dict

from tdm_ingestion.ingestion import MessageConverter
from tdm_ingestion.models import Record, Geometry, Point, \
    EntityType, Source


class NgsiConverter(MessageConverter):
    non_properties = {'latitude', 'longitude', 'timestamp', 'dateObserved',
                      'location'}
    to_skip = {'dateObserved', 'location', 'latitude', 'longitude'}
    fiware_service_path_to_sensor_type = {
        '/cagliari/edge/meteo': EntityType('WeatherObserver', 'Station'),
        '/cagliari/edge/energy': EntityType('EnergyConsumptionMonitor',
                                            'Station'),

    }

    @staticmethod
    def get_fiware_service_path(msg: Dict):
        for header in msg['headers']:
            if header.keys() == {"fiware-servicePath"}:
                return header["fiware-servicePath"]
        raise RuntimeError(f"fiware-servicePath not found in msg {msg}")

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

    def _create_sensor(self, sensor_name: str, sensor_type: EntityType,
                       geometry: Geometry,
                       properties: List[str]) -> Source:
        return Source(sensor_name, sensor_type, geometry, properties)

    def _create_models(self, msg: Dict) -> Record:
        node_name, st_name, st_type, sensor_name = NgsiConverter._get_names(
            msg)

        properties = self._get_properties(msg)
        geometry = NgsiConverter._get_geometry(msg)

        records: Dict = {}
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
                    try:
                        records[name] = float(value)
                    except ValueError:
                        logging.error(
                            f"cannot convert to float {name} = {value}")

        sensor_type = self.fiware_service_path_to_sensor_type[
            self.get_fiware_service_path(msg)]
        sensor = self._create_sensor(f"{sensor_name}",
                                     sensor_type,
                                     geometry, properties)

        return Record(time, sensor, records)

    def convert(self, messages: List[str]) -> List[Record]:

        logging.debug("messages %s", len(messages))
        timeseries_list: List = []
        for m in messages:
            try:
                m_dict = json.loads(m)
            except json.decoder.JSONDecodeError:
                logging.error('skipping message %s, error while jsondecoding',
                              m)
                continue

            timeseries_list.append(self._create_models(m_dict))

        return timeseries_list

    @staticmethod
    def _get_properties(msg: Dict) -> List[str]:
        return [attr['name'] for attr in msg['body']['attributes']
                if attr['name'] not in NgsiConverter.non_properties]


class CachedNgsiConverter(NgsiConverter):
    def __init__(self):
        self.sensors: Dict[str, Source] = defaultdict()

    def _create_sensor(self,
                       sensor_name: str,
                       sensor_type: EntityType,
                       geometry: Geometry,
                       properties: List[str]):
        return self.sensors.setdefault(sensor_name,
                                       Source(sensor_name, sensor_type,
                                              geometry, properties))
