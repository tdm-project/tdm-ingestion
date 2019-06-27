import datetime
import unittest
import uuid

from jsonpickle import json
from tdm_ingestion.ingestion import Ingester
from tdm_ingestion.models import ValueMeasure, TimeSeries, SensorType, Sensor, \
    Point
from tests.dummies import DummyConsumer, DummyStorage, DummyConverter


class TestIngester(unittest.TestCase):

    def test_process(self):
        storage = DummyStorage()
        ingester = Ingester(DummyConsumer(), storage, DummyConverter())
        ingester.process()
        self.assertEqual(len(storage.messages), 1)


class TestTimeSeries(unittest.TestCase):
    def test_to_dict(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        value = 100
        sensor_type = SensorType('test', 'test', ['test'])
        sensor = Sensor('sensor', sensor_type, 'test', Point(0,0))
        ts = TimeSeries(now, sensor, ValueMeasure(100))
        time_format = '%Y-%m-%dT%H:%M:%SZ'
        to_dict = json.loads(ts.to_json())
        self.assertEqual(to_dict['time'], now.strftime(time_format))
        self.assertEqual(to_dict['sensor'], str(sensor.name))
        self.assertEqual(to_dict['measure'], {'value': value})


if __name__ == '__main__':
    unittest.main()
