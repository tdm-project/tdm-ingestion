import logging
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import jsons

from tdm_ingestion.storage.ckan import CkanStorage, RemoteCkan
from tdm_ingestion.storage.tdmq import CachedStorage
from tdm_ingestion.tdmq.models import EntityType, Point, Record, Source
from tdm_ingestion.utils import DateTimeFormatter
from tests.unit.dummies import DummyCkan, DummyHttp, DummyTDMQClient

logger = logging.getLogger('tdm_ingestion')

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

    def test_write_no_preloaded_data(self):
        """
        Tests writing Records when no data are preloaded in the client
        """
        client = DummyTDMQClient()
        # check that clients has no sources yet
        self.assertEqual(len(client.sources), 0)
        storage = CachedStorage(client)
        storage.write(time_series)

        # check that the client sources are the same as the test sensors
        self.assertEqual(len(client.sources), 2)
        self.assertEqual(jsons.dumps(sensors),
                         jsons.dumps(client.sources.values()))

        # check that the time series in the client are the same as the test values
        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)
        self.assertEqual(jsons.dumps(time_series),
                         jsons.dumps(actual_time_series))

    def test_write_sensors_type_pre_loaded(self):
        """
        Tests writing time_series when the client has already loaded the entity types. In this case the entity types won't be 
        """
        # Why are we testing entity types? storage.write doesn't try to create them if they don't exist
        client = DummyTDMQClient()
        self.assertEqual(len(client.entity_types), 0)
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
        """
        Tests that, when entity types and sources are already created, the storage writes only the time_series
        """
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
                         jsons.dumps(actual_time_series))

class TestCkanStorage(unittest.TestCase):

    def test_write_ckan(self):
        storage = CkanStorage(DummyCkan())

        storage.write(time_series, 'lisa', 'test')
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
            DummyCkan())
        storage.write([], 'lisa', 'test')

    def test_ckan_client_write_no_records(self):
        client = RemoteCkan('base_url', DummyHttp(), 'api_key')
        client.create_resource('resource', 'dataset', [])

    def test_resource_name(self):
        now = datetime.now()
        with patch('tests.unit.test_storage.datetime') as mock_datetime:
            mock_datetime.now.return_value = now
            resource = 'resource-%Y-%m-%d'
            resource_formatted = DateTimeFormatter().format(resource)
        self.assertEqual(resource_formatted, now.strftime(resource))


if __name__ == '__main__':
    unittest.main()
