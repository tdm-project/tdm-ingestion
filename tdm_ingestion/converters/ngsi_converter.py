import datetime
import json
import logging
import re
from builtins import TypeError
from collections import defaultdict
from json import JSONDecodeError
from typing import Dict, List, Tuple

from dateutil.parser import isoparse

from tdm_ingestion.tdmq.models import (EntityType, Geometry, Point, Record,
                                       Source)

logger = logging.getLogger(__name__)


class NgsiConverter:
    """
    Class to convert json-formatted NGSI-Fiware messages to :class:`Record` instances
    """

    non_properties = {"dateObserved", "location", "TimeInstant"}
    # Legacy attributes replaced by "location"
    _to_ignore = {"latitude", "longitude"}

    fiware_service_path_to_sensor_type = {
        "/cagliari/edge/meteo": EntityType("WeatherObserver", "Station"),
        "/cagliari/edge/energy": EntityType("EnergyConsumptionMonitor", "Station"),
        "/cagliari/edge/device": EntityType("DeviceStatusMonitor", "Station"),
    }

    #: Maps the ngsi attribute types to python types
    _type_mapper = {
        "String": str,
        "Float": float,
        "Integer": int,
        "geo:point": lambda v: Point(*v.replace(" ", "").split(",")),
        "ISO8601": isoparse
    }

    #: Maps the attributes with a specific handler. It has priority higher than type
    _attrs_mapper = {
        "timestamp": lambda v: datetime.datetime.fromtimestamp(int(v), datetime.timezone.utc),
        "dateObserved": isoparse,
        "rssi": int,
        "dewpoint": float,
        "memoryFree": float
    }

    _message_id_regex = re.compile(
        r"(?P<Type>\w+):(?P<Edge>[a-zA-Z0-9_-]+)\.(?P<Station>[a-zA-Z0-9_-]+)\.(?P<Sensor>[a-zA-Z0-9_-]+)"
    )

    @staticmethod
    def _get_fiware_service_path(msg: Dict):
        for header in msg["headers"]:
            try:
                return header["fiware-servicePath"]
            except KeyError:
                pass
        raise RuntimeError(f"fiware-servicePath not found in msg {msg}")

    @staticmethod
    def _get_source_id(msg: Dict) -> str:
        try:
            match = NgsiConverter._message_id_regex.search(msg["body"]["id"])
        except KeyError:
            raise RuntimeError(f"invalid id {msg['body']['id']}")
        else:
            if match:
                _, edge_name, station_name, sensor_name = match.groups()
                source_id = "{}.{}.{}".format(edge_name, station_name, sensor_name)
                return source_id

        raise RuntimeError(f"invalid id {msg['body']['id']}")

    def _get_sensor_type(self, msg: Dict) -> str:
        try:
            service_path = self._get_fiware_service_path(msg)
            return self.fiware_service_path_to_sensor_type[service_path]
        except KeyError:
            raise RuntimeError(f"invalid message type {service_path}")


    @staticmethod
    def _create_sensor(sensor_name: str, sensor_type: EntityType, geometry: Geometry,
                       properties: List[str]) -> Source:
        return Source(sensor_name, sensor_type, geometry, properties)

    def _create_record(self, msg: Dict) -> Record:
        source_id = self._get_source_id(msg)
        sensor_type = self._get_sensor_type(msg)

        logging.debug("Converting message of type %s", sensor_type.category)

        records: Dict = {}
        time = None
        geometry = None
        for attr in msg["body"]["attributes"]:
            name, value, type_ = attr["name"], attr["value"], attr["type"]
            if value is not None and str(value).strip() and not name in self._to_ignore:
                # First check for a converter for the attribute
                try:
                    converter = self._attrs_mapper[name]
                except KeyError:
                    converter = self._type_mapper.get(type_, None)

                try:
                    converted_value = converter(value)
                except (ValueError, TypeError):
                    # FIXME: should we skip the message or just the property?
                    logger.warning("cannot read attribute %s of type %s with value %s", name, type_, value)
                else:
                    if name == "timestamp":
                        time = converted_value
                    elif name == "location":
                        geometry = converted_value
                    elif name not in self.non_properties:
                        records[name] = converted_value

        if not records:
            raise RuntimeError("conversion produced no useful data")

        if geometry is None:
            raise RuntimeError("missing latitude and/or longitude")

        sensor = self._create_sensor(source_id, sensor_type, geometry, records.keys())

        return Record(time, sensor, records)

    def convert(self, messages: List[str]) -> List[Record]:
        """
        Method that reads a list of ngsi messages and convert them in a list of :class:`Record`
        """
        logger.debug("messages %s", len(messages))
        timeseries_list: List = []
        for m in messages:
            try:
                m_dict = json.loads(m)
                logger.debug("Message is %s", m_dict)
                timeseries_list.append(self._create_record(m_dict))
            except JSONDecodeError:
                logger.error("exception decoding message %s", m)
                continue
            except RuntimeError as e:
                logger.error("exception occurred with message %s", m)
                logger.error(e)
                continue
        return timeseries_list


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
