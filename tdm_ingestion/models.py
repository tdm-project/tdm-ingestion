import datetime
from abc import ABC
from enum import Enum
from typing import List, Dict, Union

import jsons
import stringcase


def to_camel_case_dict(obj: Dict) -> Dict:
    dct = {}
    for k, v in obj.items():
        dct[stringcase.camelcase(k)] = v
    return dct


def jsonify(obj: Union[Dict, List]) -> str:
    return jsons.dumps(obj)


class Model(ABC):
    def to_json(self, serialize: bool = True) -> Union[Dict, str]:
        camel_cased = to_camel_case_dict(self.__dict__)
        return jsonify(camel_cased) if serialize else camel_cased

    @staticmethod
    def list_to_json(models: List["Model"]) -> str:
        return jsonify([m.to_json(False) for m in models])


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
    def __init__(self, name: str, type: str,
                 controlled_property: List[str], category: str = None,
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

    def to_json(self, serialize: bool = True) -> Union[Dict, str]:
        camel_cased = to_camel_case_dict(
            {"type": "Point", "coordinates": [self.longitude, self.latitude]})
        return jsonify(camel_cased) if serialize else camel_cased


class Sensor(Model):
    def __init__(self, name: str, type: SensorType, node: str,
                 geometry: Geometry):
        self.name = name
        self.type = type
        self.node = node
        self.geometry = geometry

    def __repr__(self):
        return f'Sensor {self.name} of {repr(self.type)}'

    def to_json(self, serialize: bool = True) -> Union[Dict, str]:
        dct = dict(self.__dict__)
        dct["type"] = self.type.name
        dct["geometry"] = self.geometry.to_json(False)
        camel_cased = to_camel_case_dict(dct)
        return jsonify(camel_cased) if serialize else camel_cased


class TimeSeries(Model):
    def __init__(self, utc_time: datetime.datetime, sensor: Sensor,
                 measure: Measure):
        self.time = utc_time
        self.sensor = sensor
        self.measure = measure

    def to_json(self, serialize: bool = True) -> Union[Dict, str]:
        dct = dict(self.__dict__)
        dct["sensor"] = self.sensor.name
        camel_cased = to_camel_case_dict(dct)
        return jsonify(camel_cased) if serialize else camel_cased
