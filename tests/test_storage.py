import datetime
import unittest

import jsons
from tdm_ingestion.models import SensorType, Sensor, Point, \
    TimeSeries
from tdm_ingestion.storage.base import CachedStorage
from tdm_ingestion.storage.ckan import CkanStorage, RemoteCkan
from tdm_ingestion.storage.remote_client import Requests
from tests.dummies import DummyClient

now = datetime.datetime.now(datetime.timezone.utc)
sensors_type = [
    SensorType('st1', 'type1'),
    SensorType('st2', 'type2')
]
sensors = [
    Sensor('s1', sensors_type[0], 'node', Point(0, 0), ['temp']),
    Sensor('s2', sensors_type[1], 'node', Point(1, 1), ['temp'])
]
time_series = [
    TimeSeries(now, sensors[0], {'value': 0.0}),
    TimeSeries(now, sensors[1], {'value': 1.0})
]


class TestCachedStorage(unittest.TestCase):

    def test_write_no_data(self):
        client = DummyClient()
        storage = CachedStorage(client)
        storage.write(time_series)

        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sensors.values()))

        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(client.time_series))

    def test_write_sensors_type_pre_loaded(self):
        client = DummyClient()
        client.create_entity_types(sensors_type)

        storage = CachedStorage(client)
        storage.write(time_series)

        self.assertEqual(jsons.dumps(sensors_type),
                         jsons.dumps(client.sensor_types.values()))

        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sensors.values()))

        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(client.time_series))

    def test_write_all_data_preloaded(self):
        client = DummyClient()

        client.create_entity_types(sensors_type)
        client.create_sources(sensors)

        storage = CachedStorage(client)
        storage.write(time_series)

        self.assertEqual(jsons.dumps(sensors_type),
                         jsons.dumps(client.sensor_types.values()))

        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sensors.values()))

        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(client.time_series))

    def test_write_ckan(self):
        storage = CkanStorage(
            RemoteCkan('http://127.0.0.1:5000', Requests(), 'tester'), 'lisa',
            'test')
        storage.write(time_series)


if __name__ == '__main__':
    unittest.main()
