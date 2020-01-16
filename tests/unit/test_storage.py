import jsons
import logging
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from urllib.parse import urljoin

import httpretty
import jsons

from tdm_ingestion.http_client.requests import Requests
from tdm_ingestion.storage.ckan import CkanStorage, RemoteCkan
from tdm_ingestion.storage.tdmq import CachedStorage
from tdm_ingestion.tdmq.remote import Client
from tdm_ingestion.utils import DateTimeFormatter
from tests.unit.dummies import DummyCkan, DummyHttp, DummyTDMQClient

from .data import SENSORS, SENSORS_TYPE, TIME_SERIES

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
        self.assertEqual(jsons.dumps(SENSORS), jsons.dumps(client.sources.values()))

        # check that the time series in the client are the same as the test values
        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)
        self.assertEqual(jsons.dumps(TIME_SERIES), jsons.dumps(actual_time_series))

    @httpretty.activate
    def test_write_no_time_series_when_source_creation_fails(self):
        """
        Tests writing Records when no data are preloaded in the client
        """

        client = Client("http://foo.url/")
        client.create_time_series = Mock()

        httpretty.register_uri(httpretty.POST, client.sources_url, status=400)

        storage = CachedStorage(client)
        storage.write(TIME_SERIES)

        client.create_time_series.assert_not_called()

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

        self.assertEqual(jsons.dumps(SENSORS_TYPE), jsons.dumps(client.entity_types.values()))
        self.assertEqual(jsons.dumps(SENSORS), jsons.dumps(client.sources.values()))

        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)
        self.assertEqual(jsons.dumps(TIME_SERIES), jsons.dumps(actual_time_series))

    def test_write_all_data_preloaded(self):
        """
        Tests that, when entity types and sources are already created, the storage writes only the time_series
        """
        client = DummyTDMQClient()

        client.create_entity_types(SENSORS_TYPE)
        client.create_sources(SENSORS)

        storage = CachedStorage(client)
        storage.write(TIME_SERIES)

        self.assertEqual(jsons.dumps(SENSORS_TYPE), jsons.dumps(client.entity_types.values()))
        self.assertEqual(jsons.dumps(SENSORS), jsons.dumps(client.sources.values()))

        actual_time_series = []
        for ts_list in client.time_series.values():
            actual_time_series.extend(ts_list)

        self.assertEqual(jsons.dumps(TIME_SERIES), jsons.dumps(actual_time_series))


class TestCkanStorage(unittest.TestCase):

    def setUp(self):
        base_url = "http://foo.url"
        self.ckan_client = RemoteCkan(base_url, Requests(), "api_key")
        self.records_data = {
            'resource': {'package_id': 'lisa', 'name': 'test'},
            'fields': [{'id': 'station'}, {'id': 'type'}, {'id': 'date'}, {'id': 'location'}, {'id': 'temperature'}],
            'records': [
                {'station': 's1', 'type': 'cat1', 'date': TIME_SERIES[0].time, 'location': '0,1', 'temperature': 14.0},
                {'station': 's2', 'type': 'cat2', 'date': TIME_SERIES[1].time, 'location': '2,3', 'humidity': 95.0}]
        }

        # minimal package info returned from ckan
        self.package_info = {
            "result": {
                "id": "7446532e-715f-4e18-a667-067e059a81bb",
                "state": "active",
                "type": "dataset",
                "resources": [{"name": "test", "id": "random_id", "package_id": "lisa", "state": "active", "position": 0}]
            }
        }

    @httpretty.activate
    def test_write_records_no_upsert(self):
        """
        Test creation of a resource without deleting the previous one
        """
        def request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode('utf-8'), jsons.dumps(self.records_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps('')]

        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, body=request_callback)

        storage = CkanStorage(self.ckan_client)
        res = storage.write(TIME_SERIES, 'lisa', 'test')
        self.assertEqual(res, True)

    @httpretty.activate
    def test_write_records_upsert(self):
        def create_request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(self.records_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps(self.records_data)]

        def delete_request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps({"id": "random_id"}))
            return [200, response_headers, "{}"]

        httpretty.register_uri(httpretty.GET, self.ckan_client.dataset_info_url, status=200, body=jsons.dumps(self.package_info))
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_delete_url, body=delete_request_callback)
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, body=create_request_callback)

        storage = CkanStorage(self.ckan_client)
        res = storage.write(TIME_SERIES, 'lisa', 'test', upsert=True)
        self.assertEqual(res, True)

    @httpretty.activate
    def test_write_upsert_error_retrieving_package_info(self):
        """
        Tests that, if an error occurs when retrieving package info, no records are stored in ckan
        """
        def create_request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(self.records_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps(self.records_data)]

        httpretty.register_uri(httpretty.GET, self.ckan_client.dataset_info_url, status=400)
        self.ckan_client.delete_resource = Mock()

        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, body=create_request_callback)

        storage = CkanStorage(self.ckan_client)
        res = storage.write(TIME_SERIES, 'lisa', 'test', upsert=True)
        self.assertEqual(res, True)

        self.ckan_client.delete_resource.assert_not_called()

    @httpretty.activate
    def test_write_upsert_error_deleting_previous_records(self):
        """
        Tests that, if an error occurs when retrieving package info, no records are stored in ckan
        """
        def create_request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(self.records_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps(self.records_data)]

        httpretty.register_uri(httpretty.GET, self.ckan_client.dataset_info_url, status=200, body=jsons.dumps(self.package_info))
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_delete_url, status=500)
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, body=create_request_callback)


        storage = CkanStorage(self.ckan_client)
        res = storage.write(TIME_SERIES, 'lisa', 'test', upsert=True)
        self.assertEqual(res, True)

    @httpretty.activate
    def test_write_upsert_error_creating_records(self):
        """
        Tests that, if an error occurs when creating the resource, it returns False
        """
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, status=500)

        storage = CkanStorage(self.ckan_client)
        res = storage.write(TIME_SERIES, 'lisa', 'test')
        self.assertEqual(res, False)

    @httpretty.activate
    def test_write_empty_records(self):
        storage = CkanStorage(self.ckan_client)
        res = storage.write([], 'lisa', 'test')
        self.assertEqual(res, False)

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
