import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import jsons
from tdm_ingestion.models import EntityType, Source, Point, \
    Record
from tdm_ingestion.storage.ckan import CkanStorage, RemoteCkan
from tdm_ingestion.storage.tdmq import CachedStorage
from tests.dummies import DummyTDMQClient, DummyCkan, DummyHttp

now = datetime.now(timezone.utc)
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
            {'test': {'dataset': 'lisa',
                      'records': [{'date': now,
                                   'location': '0,0',
                                   'station': 's1',
                                   'type': 'type1',
                                   'value': 0.0},
                                  {'date': now,
                                   'location': '1,1',
                                   'station': 's2',
                                   'type': 'type2',
                                   'value': 1.0}]}}
        )

    def test_write_ckan_empty(self):
        storage = CkanStorage(
            DummyCkan(),
            'lisa',
            'test')
        storage.write([])

    def test_ckan_client_write_no_records(self):
        client = RemoteCkan('base_url', DummyHttp(), 'api_key')
        client.create_resource('resource', 'dataset', [])

    def test_ckan_resource_name(self):
        now = datetime.now()
        with patch('test_storage.datetime') as mock_datetime:
            mock_datetime.now.return_value = now
            resource = 'resource-%Y-%m-%d'
            storage = CkanStorage(
                DummyCkan(),
                'dataset',
                resource)
        self.assertEqual(storage.resource, now.strftime(resource))


if __name__ == '__main__':
    unittest.main()
