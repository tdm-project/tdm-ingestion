import logging
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import jsons

from tdm_ingestion.storage.ckan import CkanStorage, RemoteCkan
from tdm_ingestion.storage.tdmq import CachedStorage
from tdm_ingestion.utils import DateTimeFormatter
from tests.unit.dummies import DummyCkan, DummyHttp, DummyTDMQClient

from .data import (REST_SOURCE, REST_TIME_SERIES, SENSORS, SENSORS_TYPE,
                   TIME_SERIES)

logger = logging.getLogger('tdm_ingestion')


class TestCachedStorage(unittest.TestCase):

    def test_write_no_preloaded_data(self):
        """
        Tests writing Records when no data are preloaded in the client
        """
        client = DummyTDMQClient()
        # check that clients has no sources yet
        self.assertEqual(len(client.sources), 0)
        storage = CachedStorage(client)
        storage.write(TIME_SERIES)

        # check that the client sources are the same as the test sensors
        self.assertEqual(len(client.sources), len(SENSORS))
        self.assertEqual(jsons.dumps(SENSORS),
                         jsons.dumps(client.sources.values()))

        # check that the time series in the client are the same as the test values
        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)
        self.assertEqual(jsons.dumps(TIME_SERIES),
                         jsons.dumps(actual_time_series))

    def test_write_sensors_type_pre_loaded(self):
        """
        Tests writing time_series when the client has already loaded the entity types. In this case the entity types won't be created
        """
        # Why are we testing entity types? storage.write doesn't try to create them if they don't exist
        client = DummyTDMQClient()
        self.assertEqual(len(client.entity_types), 0)
        client.create_entity_types(SENSORS_TYPE)

        storage = CachedStorage(client)
        storage.write(TIME_SERIES)

        self.assertEqual(jsons.dumps(SENSORS_TYPE),
                         jsons.dumps(client.entity_types.values()))

        self.assertEqual(jsons.dumps(SENSORS),
                         jsons.dumps(client.sources.values()))

        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)
        self.assertEqual(jsons.dumps(TIME_SERIES),
                         jsons.dumps(actual_time_series))

    def test_write_all_data_preloaded(self):
        """
        Tests that, when entity types and sources are already created, the storage writes only the time_series
        """
        client = DummyTDMQClient()

        client.create_entity_types(SENSORS_TYPE)
        client.create_sources(SENSORS)

        storage = CachedStorage(client)
        storage.write(TIME_SERIES)

        self.assertEqual(jsons.dumps(SENSORS_TYPE),
                         jsons.dumps(client.entity_types.values()))

        self.assertEqual(jsons.dumps(SENSORS),
                         jsons.dumps(client.sources.values()))

        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)

        self.assertEqual(jsons.dumps(TIME_SERIES),
                         jsons.dumps(actual_time_series))

class TestCkanStorage(unittest.TestCase):

    def test_write_ckan(self):
        storage = CkanStorage(DummyCkan())

        storage.write(TIME_SERIES, 'lisa', 'test')
        expected_result = {'test': {'dataset': 'lisa',
                      'records': [{'date': TIME_SERIES[0].time,
                                   'location': '0,1',
                                   'station': 's1',
                                   'type': 'cat1',
                                   'temperature': TIME_SERIES[0].data['temperature']},
                                  {'date': TIME_SERIES[1].time,
                                   'location': '2,3',
                                   'station': 's2',
                                   'type': 'cat2',
                                   'humidity': TIME_SERIES[1].data['humidity']}]}}
        self.assertDictEqual(storage.client.resources, expected_result)

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
