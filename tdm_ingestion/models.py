import datetime
from abc import ABC
from enum import Enum
from typing import AnyStr, List

import jsons
import stringcase


def jsonify(obj: dict):
    dct = {}
    for k, v in obj.items():
        dct[stringcase.camelcase(k)] = v
    return jsons.dumps(dct)


class Model(ABC):
    def to_json(self):
        return jsonify(self.__dict__)


class Measure(Model):
    pass


class ValueMeasure(Measure):
    def __init__(self, value):
        self.value = value


class RefMeasure(Measure):
    def __init__(self, ref, index):
        self.ref = ref
        self.index = index


class SensorType(Model):
    def __init__(self, name: AnyStr, type: AnyStr,
                 controlled_property: List[AnyStr], category: AnyStr = None,
                 function=None, **kwargs):
        self.name = name
        self.type = type
        self.controlled_property = controlled_property
        self.category = category or ["sensor"]
        self.function = category or ["sensing"]
        self.__dict__.update(kwargs)

    def __repr__(self):
        return f'SensorType {self.name}'


class GeometryType(Enum):
    Point = "Point"


class Geometry(Model):
    def __init__(self, type: GeometryType):
        self.type = type


class Point(Geometry):
    def __init__(self, latitude: float, longitude: float):
        super().__init__(GeometryType.Point)
        self.latitude = latitude
        self.longitude = longitude

    def to_json(self):
        return jsonify(
            {"type": "Point", "coordinates": [self.longitude, self.latitude]})


class Sensor(Model):
    def __init__(self, name: AnyStr, type: SensorType, node: AnyStr,
                 geometry: Geometry):
        self.name = name
        self.type = type
        self.node = node
        self.geometry = geometry

    def __repr__(self):
        return f'Sensor {self.name} of {repr(self.type)}'

    def to_json(self):
        dct = dict(self.__dict__)
        dct["type"] = self.type.name
        return jsonify(dct)


class TimeSeries(Model):
    def __init__(self, utc_time: datetime, sensor: Sensor,
                 measure: Measure):
        self.time = utc_time
        self.sensor = sensor
        self.measure = measure

    def to_json(self):
        dct = dict(self.__dict__)
        dct["sensor"] = self.sensor.name
        return jsonify(dct)
