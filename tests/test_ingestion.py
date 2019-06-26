import datetime
import json
import unittest

from tdm_ingestion.ingesters.async_ingester import async_ingester_factory
from tdm_ingestion.ingestion import BasicIngester
from tdm_ingestion.models import SensorType, Sensor, \
    Point
from tdm_ingestion.models import TimeSeries, ValueMeasure
from tests.dummies import DummyConsumer, DummyStorage, DummyConverter


class TestIngester(unittest.TestCase):

    def test_basic_ingester(self):
        storage = DummyStorage()
        ingester = BasicIngester(DummyConsumer(), storage, DummyConverter())
        ingester.process()
        self.assertEqual(len(storage.messages), 1)

    def test_async_ingester(self):
        storage = DummyStorage()
        ingester = async_ingester_factory([
            (DummyConsumer().poll, {}),
            (DummyConverter().convert, {}),
            (storage.write, {})]
        )
        ingester.process()
        self.assertEqual(len(storage.messages), 1)


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
