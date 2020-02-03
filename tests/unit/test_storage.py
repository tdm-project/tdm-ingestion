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

logger = logging.getLogger("tdm_ingestion")


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
        Tests writing time_series when the client has already loaded the entity types. In this case the entity types won"t be created
        """
        # Why are we testing entity types? storage.write doesn"t try to create them if they don"t exist
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
        self.dataset_id = "lisa"
        self.ckan_client = RemoteCkan(base_url, Requests(), "api_key")
        self.records_data = {
            "resource": {"package_id": self.dataset_id, "name": "test"},
            "fields": [
                {"id": "station", "type": "text"},
                {"id": "type", "type": "text"},
                {"id": "date", "type": "text"},
                {"id": "location", "type": "text"},
                {"id": "temperature", "type": "float"},
                {"id": "humidity", "type": "float"}
            ],
            "records": [
                {"station": "s1", "type": "cat1", "date": TIME_SERIES[0].time, "location": "0,1", "temperature": 14.0},
                {"station": "s2", "type": "cat2", "date": TIME_SERIES[1].time, "location": "2,3", "humidity": 95.0}]
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
        new_res_id = "new_resource_id"
        def create_request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(self.records_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps({"id": new_res_id})]

        def reorder_callback(request, _, response_headers):
            input_data = {
                "id": self.dataset_id,
                "order": [new_res_id]
            }
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(input_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps({""})]


        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, body=create_request_callback)
        httpretty.register_uri(httpretty.POST, self.ckan_client.dataset_reorder_url, body=reorder_callback)

        storage = CkanStorage(self.ckan_client)
        res = storage.write({ts.source.id_: [ts] for ts in TIME_SERIES}, "lisa", "test")
        self.assertEqual(res, True)

    @httpretty.activate
    def test_write_records_upsert(self):
        new_res_id = "new_resource_id"
        def create_request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(self.records_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps({"id": new_res_id})]

        def delete_request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps({"id": "random_id"}))
            return [200, response_headers, "{}"]

        def reorder_callback(request, _, response_headers):
            input_data = {
                "id": self.dataset_id,
                "order": [new_res_id]
            }
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(input_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps({""})]


        httpretty.register_uri(httpretty.GET, self.ckan_client.dataset_info_url, status=200, body=jsons.dumps(self.package_info))
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_delete_url, body=delete_request_callback)
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, body=create_request_callback)
        httpretty.register_uri(httpretty.POST, self.ckan_client.dataset_reorder_url, body=reorder_callback)

        storage = CkanStorage(self.ckan_client)
        res = storage.write({ts.source.id_: [ts] for ts in TIME_SERIES}, "lisa", "test", upsert=True)
        self.assertEqual(res, True)

    @httpretty.activate
    def test_write_upsert_error_retrieving_package_info(self):
        """
        Tests that, if an error occurs when retrieving package info, no records are stored in ckan
        """
        new_res_id = "new_resource_id"
        def create_request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(self.records_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps({"id": new_res_id})]

        def reorder_callback(request, _, response_headers):
            input_data = {"id": self.dataset_id, "order": [new_res_id]}
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(input_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps({""})]

        httpretty.register_uri(httpretty.GET, self.ckan_client.dataset_info_url, status=400)
        self.ckan_client.delete_resource = Mock()

        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, body=create_request_callback)
        httpretty.register_uri(httpretty.POST, self.ckan_client.dataset_reorder_url, body=reorder_callback)

        storage = CkanStorage(self.ckan_client)
        res = storage.write({ts.source.id_: [ts] for ts in TIME_SERIES}, "lisa", "test", upsert=True)
        self.assertEqual(res, True)

        self.ckan_client.delete_resource.assert_not_called()

    @httpretty.activate
    def test_write_upsert_error_deleting_previous_records(self):
        """
        Tests that, if an error occurs when retrieving package info, no records are stored in ckan
        """
        new_res_id = "new_resource_id"
        def create_request_callback(request, _, response_headers):
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(self.records_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps({"id": new_res_id})]

        def reorder_callback(request, _, response_headers):
            input_data = {"id": self.dataset_id, "order": [new_res_id]}
            self.assertEqual(request.body.decode("utf-8"), jsons.dumps(input_data))
            self.assertEqual(request.headers.get("Authorization"), "api_key")
            self.assertEqual(request.headers.get("content-type"), "application/json")
            return [200, response_headers, jsons.dumps({""})]

        httpretty.register_uri(httpretty.GET, self.ckan_client.dataset_info_url, status=200, body=jsons.dumps(self.package_info))
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_delete_url, status=500)
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, body=create_request_callback)
        httpretty.register_uri(httpretty.POST, self.ckan_client.dataset_reorder_url, body=reorder_callback)


        storage = CkanStorage(self.ckan_client)
        res = storage.write({ts.source.id_: [ts] for ts in TIME_SERIES}, "lisa", "test", upsert=True)
        self.assertEqual(res, True)

    @httpretty.activate
    def test_write_upsert_error_creating_records(self):
        """
        Tests that, if an error occurs when creating the resource, it returns False
        """
        httpretty.register_uri(httpretty.POST, self.ckan_client.resource_create_url, status=500)

        storage = CkanStorage(self.ckan_client)
        res = storage.write({ts.source.id_: [ts] for ts in TIME_SERIES}, "lisa", "test")
        self.assertEqual(res, False)

    @httpretty.activate
    def test_write_empty_records(self):
        storage = CkanStorage(self.ckan_client)
        res = storage.write({}, "lisa", "test")
        self.assertEqual(res, False)

    def test_ckan_client_write_no_records(self):
        client = RemoteCkan("base_url", DummyHttp(), "api_key")
        client.create_resource([], "resource", "dataset")

    def test_resource_name(self):
        now = datetime.now()
        with patch("tests.unit.test_storage.datetime") as mock_datetime:
            mock_datetime.now.return_value = now
            resource = "resource-%Y-%m-%d"
            resource_formatted = DateTimeFormatter().format(resource)
        self.assertEqual(resource_formatted, now.strftime(resource))


if __name__ == "__main__":
    unittest.main()
    a = [
        {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 0, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102748.668185764, "relativeHumidity": 43.1761110305786, "temperature": 22.4709999932183}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 1, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102758.90891236, "relativeHumidity": 43.2111730522284, "temperature": 22.3089385538794}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 2, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102743.08934183, "relativeHumidity": 43.2069832572724, "temperature": 22.1691620576315}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 3, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102717.540240922, "relativeHumidity": 43.1959776851718, "temperature": 22.0525698315498}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 4, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102712.079434358, "relativeHumidity": 43.2006702955875, "temperature": 21.9214524530166}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 5, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102734.308222765, "relativeHumidity": 42.8286592707288, "temperature": 21.8442459319557}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 6, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102769.944003142, "relativeHumidity": 42.6646367717722, "temperature": 21.9612291218848}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 7, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102801.261543188, "relativeHumidity": 41.9135954138938, "temperature": 22.6069663508555}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 8, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102834.378531073, "relativeHumidity": 40.5585311199986, "temperature": 23.5503954267771}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 9, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102897.799435764, "relativeHumidity": 39.2037222544352, "temperature": 24.6042222340902}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 10, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102882.405464385, "relativeHumidity": 37.9679328875835, "temperature": 25.9879888289468}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 11, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102824.585893855, "relativeHumidity": 38.3131285939137, "temperature": 26.9814524730491}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 12, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102767.236130618, "relativeHumidity": 39.4758989141229, "temperature": 26.6261797701375}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 13, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102751.64320007, "relativeHumidity": 40.4437987897649, "temperature": 26.1567039489746}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 14, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102766.095146648, "relativeHumidity": 40.6173742070544, "temperature": 25.927318498409}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 15, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102789.159130587, "relativeHumidity": 40.5535196485466, "temperature": 25.788603372414}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(
            2020, 1, 15, 16, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102821.806040503, "relativeHumidity": 40.897597925623, "temperature": 25.3929051447181}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 17, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102873.752706006, "relativeHumidity": 41.3265364662895, "temperature": 25.0085473939693}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 18, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102925.765188547, "relativeHumidity": 41.4515083781834, "temperature": 24.7710055878708}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 19, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102939.460195531, "relativeHumidity": 41.5210614763825, "temperature": 24.4415083197908}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 20, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102953.05128317, "relativeHumidity": 41.7005028325086, "temperature": 24.0504467820322}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 21, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102960.837159567, "relativeHumidity": 41.6339664992007, "temperature": 23.8205586119071}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 22, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102969.710151885, "relativeHumidity": 41.6807820623813, "temperature": 23.5734636104307}, {"station": "Edge-28DC5A97.esp8266-5222667.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 23, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102957.395878233, "relativeHumidity": 41.6162069693379, "temperature": 23.3959769654548}, {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 0, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102791.668526786, "relativeHumidity": 48.5899160048541, "temperature": 22.2205881230971}, {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 1, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102800.571100315, "relativeHumidity": 48.4511762987666, "temperature": 22.1571427513571}, {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 2, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102787.02501313, "relativeHumidity": 48.4295797909007, "temperature": 22.069579965928}, {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 3, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102762.233061975, "relativeHumidity": 48.357395043894, "temperature": 22.0022689434661}, {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 4, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102755.015429687, "relativeHumidity": 48.2842497507731, "temperature": 21.9300832430522}, {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 5, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102777.233258929, "relativeHumidity": 48.1915127089044, "temperature": 21.8684874943324}, {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 6, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102814.781575521, "relativeHumidity": 48.0107499440511, "temperature": 21.8680000940959}, {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 7, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102846.971441702, "relativeHumidity": 47.8530254524295, "temperature": 21.8879832420029}, {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 8, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102880.624348958, "relativeHumidity": 45.9030834515889, "temperature": 22.8024165948232},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(
            2020, 1, 15, 9, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102942.054556197, "relativeHumidity": 44.2154622759138, "temperature": 23.7749580255076},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(
            2020, 1, 15, 10, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102929.158482143, "relativeHumidity": 44.0319327987543, "temperature": 24.2205041356447},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 11, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 102875.312434896, "relativeHumidity": 43.9830832799276, "temperature": 24.7660832722982},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 12, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 102816.543461134, "relativeHumidity": 44.3790755392123, "temperature": 24.8354622175714},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 13, 0, tzinfo=datetime.timezone.utc),
         "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102798.667804622, "relativeHumidity": 45.1584032202969,
         "temperature": 24.4213445647424},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 14, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 102813.972623424, "relativeHumidity": 46.1242858181481, "temperature": 24.0610924568497},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 15, 0, tzinfo=datetime.timezone.utc),
         "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102837.003776042, "relativeHumidity": 46.807833480835,
         "temperature": 23.8727499643962},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 16, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 102865.731945903, "relativeHumidity": 47.5202521636706, "temperature": 23.6202519681273},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 17, 0, tzinfo=datetime.timezone.utc),
         "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102915.369140625, "relativeHumidity": 47.9851669947306,
         "temperature": 23.3040000120799},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 18, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 102972.513064601, "relativeHumidity": 48.3336133876768, "temperature": 23.0691595959062},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 19, 0, tzinfo=datetime.timezone.utc),
         "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102988.460871849, "relativeHumidity": 48.4822689986029,
         "temperature": 22.807226838184},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 20, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 103001.143098958, "relativeHumidity": 48.4738334019979, "temperature": 22.6386667569478},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 21, 0, tzinfo=datetime.timezone.utc),
         "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103007.716320903,
         "relativeHumidity": 48.5289078079352, "temperature": 22.4773109500148},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 22, 0, tzinfo=datetime.timezone.utc),
         "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103013.196494223, "relativeHumidity": 48.5243698248342,
         "temperature": 22.3382353742583},
        {"station": "Edge-28DC5A97.esp8266-5220255.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 23, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 103003.195246849, "relativeHumidity": 48.4475628828802, "temperature": 22.2230252979182},
        {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 0, 0, tzinfo=datetime.timezone.utc),
         "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102796.441474781, "relativeHumidity": 49.7784209892764,
         "temperature": 19.9298245502494},
        {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 1, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 102806.173464753, "relativeHumidity": 49.7042032064394, "temperature": 19.8532558707304},
        {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 2, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 102790.844480994, "relativeHumidity": 49.6403509664257, "temperature": 19.7758480986657},
        {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 3, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 102764.643594664, "relativeHumidity": 49.5826961227328, "temperature": 19.6909940730759},
        {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 4, 0, tzinfo=datetime.timezone.utc),
         "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102759.171144006, "relativeHumidity": 49.5096022845709,
         "temperature": 19.6174269336009},
        {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station",
         "date": datetime.datetime(2020, 1, 15, 5, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09",
         "barometricPressure": 102785.101059942, "relativeHumidity": 48.9949182432297, "temperature": 19.6343857056913},
        {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 6, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102819.079541301, "relativeHumidity": 47.2358304297018, "temperature": 19.9504678960432}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 7, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102851.361202485, "relativeHumidity": 47.779204586096, "temperature": 19.8225149299666}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 8, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102886.893960161, "relativeHumidity": 47.9875555205763, "temperature": 19.8419883237248}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 9, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102948.213404605, "relativeHumidity": 47.4971463164391, "temperature": 20.2067836069921}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(
            2020, 1, 15, 10, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102934.994060673, "relativeHumidity": 47.7867543850726, "temperature": 20.4411110459713}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 11, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102880.684210526, "relativeHumidity": 48.0849124618441, "temperature": 20.6187132562113}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 12, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102821.566429094, "relativeHumidity": 48.2196081507276, "temperature": 20.5980701781156}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 13, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102803.533762792, "relativeHumidity": 48.258292571843, "temperature": 20.7064327329223}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 14, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102818.889482822, "relativeHumidity": 48.3881870515165, "temperature": 20.796666831301},
        {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 15, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102841.204678363, "relativeHumidity": 48.3805673833479, "temperature": 20.9147368983219}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 16, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102871.623446637, "relativeHumidity": 48.6223391036541, "temperature": 20.8645029123764}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 17, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102920.242461623, "relativeHumidity": 48.7985441196732, "temperature": 20.9949707566646}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 18, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102977.281295687, "relativeHumidity": 49.383953429105, "temperature": 20.7730994196663}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(
            2020, 1, 15, 19, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102992.04879386, "relativeHumidity": 49.416415008188, "temperature": 20.6232748422009}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 20, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103003.68781981, "relativeHumidity": 49.4312864827831, "temperature": 20.5096490648058}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 21, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103011.473684211, "relativeHumidity": 49.390947219224, "temperature": 20.4106433043006}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 22, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103017.97432383, "relativeHumidity": 49.3124736317417, "temperature": 20.3185380076804}, {"station": "Edge-28DC5A97.esp8266-8956285.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 23, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103005.373629386, "relativeHumidity": 49.2178303233364, "temperature": 20.2383624405889},
        {"station": "Edge-65B526BA.emontx3-08.DS18B20-1",  "type": "Station", "date": datetime.datetime(2020, 1, 15, 0, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.5992217898833, "temperature": 22.5311285234147}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(
            2020, 1, 15, 1, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.5770750988142, "temperature": 22.4936758652035}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 2, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.6739130434783, "temperature": 22.4021735398666},
        {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 3, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.5535055350554, "temperature": 22.2999992370605}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 4, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.565543071161, "temperature": 22.2655428339926}, {
            "station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 5, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.5770750988142, "temperature": 22.2193680488074}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 6, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.5675675675676, "temperature": 22.188031583219},
        {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 7, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.5905511811024, "temperature": 22.2885820884404}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 8, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.5529411764706, "temperature": 22.4650979060753}, {
            "station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 9, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -29.278431372549, "temperature": 23.1360786811978}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 10, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -29.6134453781513, "temperature": 23.7651259879104},
        {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 11, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -29.4037735849057, "temperature": 24.5630189355814}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 12, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -29.2583025830258, "temperature": 24.6985243695249}, {
            "station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 13, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -29.102766798419, "temperature": 24.1901189479903}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 14, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -28.6899224806202, "temperature": 24.0321706653565},
        {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 15, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -29.59375, "temperature": 24.0003906264901}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 16, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.0436507936508, "temperature": 23.7579363489908}, {
            "station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 17, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.4208633093525, "temperature": 23.5550361811686}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 18, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.4767025089606, "temperature": 23.3365588581263},
        {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 19, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.5109170305677, "temperature": 23.0157205839865}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 20, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.3971119133574, "temperature": 22.8530680522161}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 21, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.3494423791822, "temperature": 22.7118963291211}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 22, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.3346007604563, "temperature": 22.6311792076314}, {"station": "Edge-65B526BA.emontx3-08.DS18B20-1", "type": "Station", "date": datetime.datetime(2020, 1, 15, 23, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "rssi": -30.2548262548263, "temperature": 22.527027130127}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 0, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102842.93046875, "relativeHumidity": 46.8835002263387, "temperature": 22.8772499243418}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 1, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102851.922662815, "relativeHumidity": 46.4663864905093, "temperature": 22.9572268894741}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(
            2020, 1, 15, 2, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102835.838235294, "relativeHumidity": 46.5118486099884, "temperature": 22.8366386509743}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 3, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102810.496419271, "relativeHumidity": 46.4713332811991, "temperature": 22.7356667200724}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 4, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102805.067489496, "relativeHumidity": 46.2877312668231, "temperature": 22.7226050080371}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 5, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102829.336265756, "relativeHumidity": 45.9978991917201, "temperature": 22.742100803792}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 6, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102864.635546875, "relativeHumidity": 45.6485832532247, "temperature": 22.7681667963664}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 7, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102896.288143382, "relativeHumidity": 45.8542857450597, "temperature": 22.744873960479}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 8, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102931.104910714, "relativeHumidity": 45.5051258952678, "temperature": 22.9017646853663},
        {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 9, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102994.843027836, "relativeHumidity": 45.8672270253927, "temperature": 23.1168068156523}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(
            2020, 1, 15, 10, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102980.308757878, "relativeHumidity": 46.025882464497, "temperature": 23.4652102013596}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 11, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102924.625911458, "relativeHumidity": 45.7473330815633, "temperature": 24.0473333835602},
        {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 12, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102865.053702731, "relativeHumidity": 46.3719327590045, "temperature": 24.1037815158107}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 13, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102849.027213542, "relativeHumidity": 47.5496666908264, "temperature": 23.6662499904633},
        {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 14, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102864.540966387, "relativeHumidity": 47.6374789967256, "temperature": 23.6166387766349}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 15, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102887.408810399, "relativeHumidity": 47.7186554339753, "temperature": 23.6142857655758}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 16, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102918.274947479, "relativeHumidity": 47.8549578049604, "temperature": 23.5457982936827}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 17, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102968.462381828, "relativeHumidity": 47.7190757559127, "temperature": 23.4848740040755}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 18, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103022.316307773, "relativeHumidity": 47.6363865187188, "temperature": 23.3749580703864}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 19, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103037.087923729, "relativeHumidity": 47.2685594720356, "temperature": 23.2859322337781}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 20, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103049.327278646, "relativeHumidity": 47.1130832354228, "temperature": 23.2253332138062}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 21, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103057.472820378, "relativeHumidity": 47.1457983065052, "temperature": 23.0637814497747}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 22, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103065.463932292, "relativeHumidity": 46.9890000979106, "temperature": 22.9779166698456}, {"station": "Edge-28DC5A97.esp8266-5224596.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 23, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103055.694196429, "relativeHumidity": 46.8794117775284, "temperature": 22.8671429257433}, {"station": "Edge-65B526BA.esp8266-2411495.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 8, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102964.3828125, "relativeHumidity": 41.7599983215332, "temperature": 23.4599990844727}, {"station": "Edge-65B526BA.esp8266-2411495.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 9, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 103007.427929688, "relativeHumidity": 38.8776666959127, "temperature": 24.7618333816528}, {"station": "Edge-65B526BA.esp8266-2411495.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 10, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102993.443474265, "relativeHumidity": 36.8293277195522, "temperature": 26.1036975604145}, {"station": "Edge-65B526BA.esp8266-2411495.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 11, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102938.250976562, "relativeHumidity": 36.1896666208903, "temperature": 26.9062501430511}, {"station": "Edge-65B526BA.esp8266-2411495.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 12, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102881.13071166, "relativeHumidity": 36.2689074989127, "temperature": 27.1906721611985}, {"station": "Edge-65B526BA.esp8266-2411495.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 13, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102863.053776042, "relativeHumidity": 36.6145833651225, "temperature": 27.0175831953684}, {"station": "Edge-65B526BA.esp8266-2411495.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 14, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102876.272200521, "relativeHumidity": 36.9630000432332, "temperature": 26.6994167804718}, {"station": "Edge-65B526BA.esp8266-2411495.BME280", "type": "Station", "date": datetime.datetime(2020, 1, 15, 15, 0, tzinfo=datetime.timezone.utc), "location": "2.0554e-11,8.821065e-09", "barometricPressure": 102888.435649671, "relativeHumidity": 37.2702632703279, "temperature": 26.6050002449437}
        ]
