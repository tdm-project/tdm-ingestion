import datetime
import json
import unittest

from tdm_ingestion.ingesters.async_ingester import AsyncIngester
from tdm_ingestion.ingestion import BasicIngester
from tdm_ingestion.models import SensorType, Sensor, \
    Point
from tdm_ingestion.models import TimeSeries, ValueMeasure
from tests.dummies import DummyConsumer, DummyStorage, DummyConverter


class TestIngester(unittest.TestCase):

    def _test_ingester(self, ingester_cls):
        storage = DummyStorage()
        consumer = DummyConsumer()
        converter = DummyConverter()
        ingester = ingester_cls(consumer, storage, converter)
        ingester.process()
        self.assertEqual(len(storage.messages), 1)

    def test_basic_ingester(self):
        self._test_ingester(BasicIngester)

    def test_async_ingester(self):
        self._test_ingester(AsyncIngester)



class TestTimeSeries(unittest.TestCase):
    def test_to_dict(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        value = 100
        sensor_type = SensorType('test', 'test', ['test'])
        sensor = Sensor('sensor', sensor_type, 'test', Point(0, 0))
        ts = TimeSeries(now, sensor, ValueMeasure(100))
        time_format = '%Y-%m-%dT%H:%M:%SZ'
        to_dict = json.loads(ts.to_json())
        self.assertEqual(to_dict['time'], now.strftime(time_format))
        self.assertEqual(to_dict['sensor'], str(sensor.name))
        self.assertEqual(to_dict['measure'], {'value': value})


if __name__ == '__main__':
    unittest.main()
