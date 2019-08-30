import datetime
import unittest

import jsons
from tdm_ingestion.models import EntityType, Source, Point, \
    Record
from tdm_ingestion.storage.ckan import CkanStorage
from tdm_ingestion.storage.tdmq import CachedStorage
from tests.dummies import DummyTDMQClient, DummyCkan

now = datetime.datetime.now(datetime.timezone.utc)
sensors_type = [
    EntityType('st1', 'type1'),
    EntityType('st2', 'type2')
]
sensors = [
    Source('s1', sensors_type[0], Point(0, 0), ['temp']),
    Source('s2', sensors_type[1], Point(1, 1), ['temp'])
]
time_series = [
    Record(now, sensors[0], {'value': 0.0}),
    Record(now, sensors[1], {'value': 1.0})
]


class TestCachedStorage(unittest.TestCase):

    def test_write_no_data(self):
        client = DummyTDMQClient()
        storage = CachedStorage(client)
        storage.write(time_series)

        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sources.values()))

        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)
        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(actual_time_series))

    def test_write_sensors_type_pre_loaded(self):
        client = DummyTDMQClient()
        client.create_entity_types(sensors_type)

        storage = CachedStorage(client)
        storage.write(time_series)

        self.assertEqual(jsons.dumps(sensors_type),
                         jsons.dumps(client.entity_types.values()))

        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sources.values()))

        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)
        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(actual_time_series))

    def test_write_all_data_preloaded(self):
        client = DummyTDMQClient()

        client.create_entity_types(sensors_type)
        client.create_sources(sensors)

        storage = CachedStorage(client)
        storage.write(time_series)

        self.assertEqual(jsons.dumps(sensors_type),
                         jsons.dumps(client.entity_types.values()))

        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sources.values()))

        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)

        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(actual_time_series)
                         )

    def test_write_ckan(self):
        storage = CkanStorage(
            DummyCkan(),
            'lisa',
            'test')
        storage.write(time_series)
        self.assertEqual(
            storage.client.resources,
            {'test': {'dataset': 'lisa', 'records': [
                {'timestamp': now.timestamp(), 'value': 0.0},
                {'timestamp': now.timestamp(), 'value': 1.0}]}})


if __name__ == '__main__':
    unittest.main()
