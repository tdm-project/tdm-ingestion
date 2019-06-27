import datetime
import uuid
from abc import ABC
from typing import AnyStr, List

import jsons
import stringcase


class Model(ABC):
    def to_json(self):
        dct = {}
        for k,v in self.__dict__.items():
            dct[stringcase.camelcase(k)] = v
        return jsons.dumps(dct)


class Measure(Model):
    pass


class ValueMeasure(Measure):
    def __init__(self, value):
        self.value = value


class RefMeasure(Measure):
    def __init__(self, ref, index):
        self.ref = ref
        self.index = index


class TimeSeries(Model):
    def __init__(self, utc_time: datetime, sensorcode: uuid.UUID,
                 measure: Measure):
        self.time = utc_time
        self.sensorcode = sensorcode
        self.measure = measure
