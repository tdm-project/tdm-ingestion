import datetime
import json
import unittest

from tdm_ingestion.ingesters.async_ingester import AsyncIngester
from tdm_ingestion.ingestion import BasicIngester
from tdm_ingestion.models import SensorType, Sensor, \
    Point
from tdm_ingestion.models import TimeSeries, ValueMeasure
from tests.dummies import DummyConsumer, DummyStorage, DummyConverter, \
    AsyncDummyConsumer, AsyncDummyStorage


class TestIngester(unittest.TestCase):

    def _test_ingester(self, ingester_cls, storage_cls, consumer_cls,
                       converter_cls):
        storage = storage_cls()
        consumer = consumer_cls()
        converter = converter_cls()
        ingester = ingester_cls(consumer, storage, converter)
        ingester.process()
        self.assertEqual(len(storage.messages), 1)

    def test_basic_ingester(self):
        self._test_ingester(BasicIngester, DummyStorage, DummyConsumer,
                            DummyConverter)

    def test_async_ingester(self):
        self._test_ingester(AsyncIngester, AsyncDummyStorage,
                            AsyncDummyConsumer, DummyConverter)


class TestTimeSeries(unittest.TestCase):
    def test_to_dict(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        value = 100
        sensor_type = SensorType('test', 'test')
        sensor = Sensor('sensor', sensor_type, 'test', Point(0, 0), ['test'])
        ts = TimeSeries(now, sensor, {'value': 100.0})
        time_format = '%Y-%m-%dT%H:%M:%SZ'
        to_dict = json.loads(ts.to_json())
        self.assertEqual(to_dict['time'], now.strftime(time_format))
        self.assertEqual(to_dict['source'], str(sensor.name))
        self.assertEqual(to_dict['data'], {'value': value})


if __name__ == '__main__':
    unittest.main()
