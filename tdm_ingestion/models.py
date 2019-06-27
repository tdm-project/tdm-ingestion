import datetime
import uuid
from abc import ABC

import jsons


class Model(ABC):
    def to_json(self):
        return jsons.dumps(self)


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
