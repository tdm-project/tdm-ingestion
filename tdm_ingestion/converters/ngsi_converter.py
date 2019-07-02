import datetime
import json
import logging
import re
from collections import OrderedDict
from typing import List, Tuple, Dict

from tdm_ingestion.ingestion import MessageConverter, Message
from tdm_ingestion.models import TimeSeries, ValueMeasure, Geometry, Point, \
    SensorType, Sensor


class NgsiConverter(MessageConverter):
    non_properties = {'latitude', 'longitude', 'timestamp', 'dateObserved',
                      'location'}

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

    @staticmethod
    def _create_models(msg: Dict) -> List[TimeSeries]:
        node_name, st_name, st_type, sensor_name = NgsiConverter._get_names(
            msg)

        properties = [attr['name'] for attr in msg['body']['attributes']
                      if attr['name'] not in NgsiConverter.non_properties]
        sensor_type = SensorType(st_name, st_type, properties)

        geometry = NgsiConverter._get_geometry(msg)

        measures: Dict = OrderedDict()
        time = None
        to_skip = {'dateObserved', 'location', 'latitude', 'longitude'}
        for attr in msg['body']['attributes']:
            name = attr['name']
            value = attr['value']
            if value is not None and str(
                    value).strip() and name not in to_skip:
                if name == 'timestamp':
                    time = datetime.datetime.fromtimestamp(
                        float(value), datetime.timezone.utc
                    )
                else:
                    measures[name] = float(value)

        time_series_list = []
        for measure, value in measures.items():
            sensor = Sensor(f"{sensor_name}.{measure}", sensor_type, node_name,
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
