import datetime
import uuid
from abc import ABC, abstractmethod


class Measure(ABC):
    @abstractmethod
    def to_dict(self) -> dict:
        pass


class ValueMeasure(Measure):
    def __init__(self, value):
        self.value = value

    def to_dict(self) -> dict:
        return {'value': self.value}


class RefMeasure(Measure):
    def __init__(self, ref, index):
        self.ref = ref
        self.index = index

    def to_dict(self) -> dict:
        return {'reference': self.ref, 'index': self.index}


class TimeSeries:
    def __init__(self, utc_time: datetime, sensorcode: uuid.UUID, measure: Measure):

        self.time = utc_time
        self.sensorcode = sensorcode
        self.measure = measure

    def to_dict(self, time_format: str = '%Y-%m-%dT%H:%M:%SZ') -> dict:
        return {'time': self.time.strftime(time_format), 'sensorcode': str(self.sensorcode),
                'measure': self.measure.to_dict()}
