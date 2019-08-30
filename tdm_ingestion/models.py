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


class EntityType(Model):
    def __init__(self, _id: str, category: str):
        self.name = _id
        self.category = category or ["sensor"]

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


class Source(Model):
    def __init__(self,
                 _id: str = None,
                 type: EntityType = None,
                 geometry: Geometry = None,
                 controlled_properties: List[str] = None,
                 tdmq_id: str = None):
        self._id = _id
        self.type = type
        self.geometry = geometry
        self.controlled_properties = controlled_properties
        self.tdmq_id = tdmq_id

    def __repr__(self):
        return f'Sensor {self._id} of {repr(self.type)}'

    def to_json(self, serialize: bool = True) -> Union[Dict, str]:
        dct = dict(
            id=self._id,
            alias=self._id,
            entity_type=self.type.name,
            entity_category="Station",
            default_footprint=self.geometry.to_json(False),
            stationary=True,
            controlledProperties=self.controlled_properties,
            shape=[],
            description={
            }
        )
        return jsonify(dct) if serialize else dct


class Record(Model):
    def __init__(self, utc_time: datetime.datetime, source: Source,
                 measure: Dict[str, float]):
        self.time = utc_time
        self.source = source
        self.data = measure

    def to_json(self, serialize: bool = True) -> Union[Dict, str]:
        dct = dict(self.__dict__)
        dct["source"] = self.source._id
        camel_cased = to_camel_case_dict(dct)
        return jsonify(camel_cased) if serialize else camel_cased
