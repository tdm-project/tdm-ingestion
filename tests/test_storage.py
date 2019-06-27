import datetime
import unittest

import jsons
from tdm_ingestion.models import SensorType, ValueMeasure, Sensor, Point, \
    TimeSeries
from tdm_ingestion.storage.cached_storage import Storage
from tests.dummies import DummyClient

now = datetime.datetime.now(datetime.timezone.utc)
sensors_type = [
    SensorType('st1', 'type1', ['temp']),
    SensorType('st2', 'type2', ['temp'])
]
sensors = [
    Sensor('s1', sensors_type[0], 'node', Point(0, 0)),
    Sensor('s2', sensors_type[1], 'node', Point(1, 1))
]
time_series = [
    TimeSeries(now, sensors[0], ValueMeasure(0)),
    TimeSeries(now, sensors[1], ValueMeasure(1)),
]


class TestCachedStorage(unittest.TestCase):

    def test_write_no_data(self):
        client = DummyClient()
        storage = Storage(client)
        storage.write(time_series)

        self.assertEqual(jsons.dumps(sensors_type),
                         jsons.dumps(client.sensor_types.values()))

        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sensors.values()))

        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(client.time_series))

    def test_write_sensors_type_pre_loaded(self):
        client = DummyClient()
        client.create_sensor_type(sensors_type)

        storage = Storage(client)
        storage.write(time_series)

        self.assertEqual(jsons.dumps(sensors_type),
                         jsons.dumps(client.sensor_types.values()))

        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sensors.values()))

        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(client.time_series))

    def test_write_all_data_preloaded(self):
        client = DummyClient()

        client.create_sensor_type(sensors_type)
        client.create_sensors(sensors)

        storage = Storage(client)
        storage.write(time_series)

        self.assertEqual(jsons.dumps(sensors_type),
                         jsons.dumps(client.sensor_types.values()))

        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sensors.values()))

        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(client.time_series))


if __name__ == '__main__':
    unittest.main()
