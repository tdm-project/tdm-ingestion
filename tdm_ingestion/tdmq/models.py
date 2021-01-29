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
    def __init__(self, name: str, category: str = "sensor"):
        self.name = name
        self.category = category

    def __repr__(self):
        return f'SensorType {self.name} of category {self.category}'

    def __eq__(self, obj):
        return self.name == obj.name and self.category == obj.category


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
                 id_: str = None,
                 type_: EntityType = None,
                 station_model: str = None,
                 geometry: Geometry = None,
                 controlled_properties: List[str] = None,
                 tdmq_id: str = None,
                 edge_id: str = None,
                 station_id: str = None,
                 sensor_id: str = None):
        self.id_ = id_
        self.type = type_
        self.station_model = station_model
        self.geometry = geometry
        self.controlled_properties = controlled_properties
        self.tdmq_id = tdmq_id
        self.edge_id = edge_id
        self.station_id = station_id
        self.sensor_id = sensor_id

    def to_json(self, serialize: bool = True) -> Union[Dict, str]:
        dct = dict(
            id=self.id_,
            alias=self.id_,
            entity_type=self.type.name,
            entity_category="Station",
            station_model=self.station_model,
            default_footprint=self.geometry.to_json(False),
            stationary=True,
            controlledProperties=self.controlled_properties,
            shape=[],
            description={
            },
            edge_id = self.edge_id,
            station_id = self.station_id,
            sensor_id = self.sensor_id
        )
        return jsonify(dct) if serialize else dct

    def __repr__(self):
        return f'Sensor {self.id_} of {repr(self.type)}'

class Record(Model):
    def __init__(self,
                 utc_time: datetime.datetime,
                 source: Source,
                 footprint: Geometry,
                 measure: Dict[str, float]):
        self.time = utc_time
        self.source = source
        self.footprint = footprint
        self.data = measure

    def to_dict(self):
        dct = dict(self.__dict__)
        dct["source"] = self.source.id_

    def to_json(self, serialize: bool = True) -> Union[Dict, str]:
        dct = dict(
            source = self.source.id_,
            time = self.time,
            footprint = self.footprint.to_json(False),
            data = self.data
        )
        camel_cased = to_camel_case_dict(dct)
        return jsonify(camel_cased) if serialize else camel_cased
