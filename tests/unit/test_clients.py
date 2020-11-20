import logging
import unittest
from datetime import datetime, timedelta, timezone

import httpretty
import jsons
from requests.exceptions import HTTPError

from tdm_ingestion.http_client.requests import Requests
from tdm_ingestion.tdmq.models import EntityType, Point, Source
from tdm_ingestion.tdmq.remote import (Client, DuplicatedEntryError,
                                       GenericHttpError)

from .data import (REST_SOURCE, REST_TIME_SERIES, SENSORS, SENSORS_TYPE,
                   TIME_SERIES)

logger = logging.getLogger("tdm_ingestion")


class TestRemoteClient(unittest.TestCase):

    def setUp(self):
        self.url = "http://foo.bar"
        self.auth_token = "test_token"

    def _get_httpretty_callback(self, expected_response):
        def create_request_callback(request, _, response_headers):
            self.assertEqual(request.headers.get("Authorization"), "Bearer {}".format(self.auth_token))
            return [200, response_headers, jsons.dumps(expected_response)]
        return create_request_callback
        
    @httpretty.activate
    def test_create_entity_types(self):
        """
        Tests correct client answer
        """
        expected_response = [s.name for s in SENSORS_TYPE]

        client = Client(self.url, auth_token=self.auth_token)
        httpretty.register_uri(httpretty.POST, client.entity_types_url, body=self._get_httpretty_callback(expected_response))

        res = client.create_entity_types(SENSORS_TYPE)
        self.assertEqual(res, expected_response)

    @httpretty.activate
    def test_create_entity_types_error(self):
        """
        Tests that when an error occurs it raises GenericError
        """
        client = Client(self.url, auth_token=self.auth_token)
        httpretty.register_uri(httpretty.POST, client.entity_types_url, status=400)

        self.assertRaises(GenericHttpError, client.create_entity_types, SENSORS_TYPE)

    @httpretty.activate
    def test_create_entity_types_error(self):
        """
        Tests that when an error occurs it raises GenericError
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.entity_types_url, status=401)

        self.assertRaises(GenericHttpError, client.create_entity_types, SENSORS_TYPE)

    @httpretty.activate
    def test_create_sources(self):
        """
        Tests correct client answer
        """
        
        expected_response = [s.id_ for s in SENSORS]

        client = Client(self.url, auth_token=self.auth_token)
        httpretty.register_uri(httpretty.POST, client.sources_url, body=self._get_httpretty_callback(expected_response))

        res = client.create_sources(SENSORS)
        self.assertEqual(res, expected_response)

    @httpretty.activate
    def test_create_source_generic_error(self):
        """
        Tests that when an error occurs it raises GenericError
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.sources_url, status=400)
        self.assertRaises(GenericHttpError, client.create_sources, SENSORS)

    
    @httpretty.activate
    def test_create_source_unauthorized(self):
        """
        Tests that when an error occurs it raises GenericError
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.sources_url, status=401)

        self.assertRaises(GenericHttpError, client.create_sources, SENSORS_TYPE)

    @httpretty.activate
    def test_create_source_duplicate_entry_error(self):
        """
        Tests that when a duplicate error occurs it raises DuplicatedEntryError
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.sources_url, status=409)
        self.assertRaises(DuplicatedEntryError, client.create_sources, SENSORS)

    @httpretty.activate
    def test_create_time_series(self):
        expected_response = jsons.dumps(TIME_SERIES)

        client = Client(self.url, auth_token=self.auth_token)        
        httpretty.register_uri(httpretty.POST, client.records_url, body=jsons.dumps(expected_response))

        res = client.create_time_series(TIME_SERIES)
        self.assertEqual(res, expected_response)

    @httpretty.activate
    def test_create_time_series_error(self):
        """
        Tests that when an error occurs it raises GenericError
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.records_url, status=400)

        self.assertRaises(GenericHttpError, client.create_time_series, TIME_SERIES)

        
    @httpretty.activate
    def test_create_time_series_unauthorized(self):
        """
        Tests that when an error occurs it raises GenericError
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.records_url, status=401)

        self.assertRaises(GenericHttpError, client.create_time_series, SENSORS_TYPE)

    @httpretty.activate
    def test_get_source_by_id(self):
        """
        Tests getting a source using the tdmq id
        """
        expected_source = Source(
            id_=REST_SOURCE["external_id"],
            type_=EntityType(REST_SOURCE["entity_type"], REST_SOURCE["entity_category"]),
            geometry=Point(*REST_SOURCE["default_footprint"]["coordinates"][::-1]),  # for some strange reason the points are inverted
            controlled_properties=None,
            tdmq_id=REST_SOURCE["tdmq_id"]
        )

        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, f'{client.sources_url}/{SENSORS[0].tdmq_id}',
                               body=jsons.dumps(REST_SOURCE), match_querystring=False)

        res = client.get_sources(REST_SOURCE["tdmq_id"])
        self.assertEqual(res.to_json(), expected_source.to_json())

    @httpretty.activate
    def test_get_all_sources(self):
        """
        Tests getting all sources
        """
        expected_sources = [
            Source(
                id_=REST_SOURCE["external_id"],
                type_=EntityType(REST_SOURCE["entity_type"], REST_SOURCE["entity_category"]),
                geometry=Point(*REST_SOURCE["default_footprint"]["coordinates"][::-1]),
                controlled_properties=None,
                tdmq_id=REST_SOURCE["tdmq_id"]
            )
        ]

        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, client.sources_url,
                               body=jsons.dumps([REST_SOURCE]), match_querystring=False)

        res = client.get_sources()
        self.assertEqual([s.to_json() for s in res],
                         [s.to_json() for s in expected_sources])

    @httpretty.activate
    def test_get_sources_error(self):
        """
        Tests that when an error occurs it raises GenericError
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, client.sources_url, status=400, match_querystring=False)
        httpretty.register_uri(httpretty.GET, f"{client.sources_url}/{SENSORS[0].id_}", status=400, match_querystring=False)

        self.assertRaises(GenericHttpError, client.get_sources, SENSORS[0].id_)
        self.assertRaises(GenericHttpError, client.get_sources)

    @httpretty.activate
    def test_get_sources_count(self):
        """
        Tests source count
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, client.sources_url,
                               body=jsons.dumps([REST_SOURCE]), match_querystring=False)

        res = client.sources_count()
        self.assertEqual(res, 1)

    @httpretty.activate
    def test_get_sources_count_error(self):
        """
        Tests that when an error occurs it raises GenericError
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, client.sources_url, status=400)

        self.assertRaises(GenericHttpError, client.sources_count)

    @httpretty.activate
    def test_get_time_series(self):
        """
        Tests getting all time series of a source
        """
        expected_records = [TIME_SERIES[0]]

        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, f"{client.sources_url}/{SENSORS[0].tdmq_id}/timeseries",
                               body=jsons.dumps(REST_TIME_SERIES), match_querystring=False)

        res = client.get_time_series(SENSORS[0])
        self.assertEqual([s.to_json() for s in res],
                         [s.to_json() for s in expected_records])

    @httpretty.activate
    def test_get_time_series_with_query(self):
        """
        Tests getting all time series of a source
        """
        expected_records = [TIME_SERIES[0]]

        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, f"{client.sources_url}/{SENSORS[0].tdmq_id}/timeseries",
                               body=jsons.dumps(REST_TIME_SERIES), match_querystring=False)

        # params values are not important
        params = {
            "after": datetime.now(),
            "before": datetime.now() + timedelta(hours=1),
            "op": "sum",
            "fields": "temperature"
        }
        res = client.get_time_series(SENSORS[0], query=params)
        self.assertEqual([s.to_json() for s in res],
                         [s.to_json() for s in expected_records])

    @httpretty.activate
    def test_get_time_series_error(self):
        """
        Tests getting all time series of a source
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, f"{client.sources_url}/{SENSORS[0].tdmq_id}/timeseries",
                               status=400)

        self.assertRaises(GenericHttpError, client.get_time_series, SENSORS[0])


class TestRequestsHttpClient(unittest.TestCase):

    def setUp(self):
        self.url = "http://foo.bar"

    @httpretty.activate
    def test_post_success(self):
        """
        Tests successfull POST http call
        """
        r = Requests()
        body = {"foo": "bar"}
        headers = {"Authorization": "Bearer 123"}
        response_body = {"bar": "foo"}

        def request_callback(request, _, response_headers):
            self.assertEqual(request.body, b"foo=bar")
            self.assertEqual(request.headers.get("content-type"), headers["content-type"])
            self.assertEqual(request.headers.get("Authorization"), headers["Authorization"])
            return [200, response_headers, jsons.dumps(response_body)]

        httpretty.register_uri(httpretty.POST, self.url, body=request_callback)

        res = r.post(url=self.url, data=body, headers=headers)
        self.assertEqual(res, response_body)

    @httpretty.activate
    def test_post_error(self):
        """
        Tests that when an http error occurs on POST method, it raises HTTPError
        """
        for status in list(range(400, 419)) + list(range(500, 512)):
            httpretty.register_uri(httpretty.POST, self.url, status=status)

            r = Requests()
            body = {"attr": "value"}
            headers = {"Authorization": "Bearer 123"}
            self.assertRaises(HTTPError, r.post, url=self.url, data=body, headers=headers)

    @httpretty.activate
    def test_get_success(self):
        """
        Tests successfull POST http call
        """
        r = Requests()
        params = {"foo": "bar"}
        headers = {"Authorization": "Bearer 123"}
        response_body = {"bar": "foo"}

        def request_callback(request, _, response_headers):
            logger.debug(request.querystring)
            self.assertEqual(request.querystring, {"foo": ["bar"]})
            self.assertEqual(request.headers.get("Authorization"), headers['Authorization'])
            return [200, response_headers, jsons.dumps(response_body)]

        httpretty.register_uri(httpretty.GET, self.url, body=request_callback)

        res = r.get(url=self.url, params=params, headers=headers)
        self.assertEqual(res, response_body)

    @httpretty.activate
    def test_get_error(self):
        """
        Tests that when an http error occurs on GET method, it raises HTTPError
        """
        for status in list(range(400, 419)) + list(range(500, 512)):
            httpretty.register_uri(httpretty.GET, self.url, status=status)

            r = Requests()
            params = {"attr": "value"}
            self.assertRaises(HTTPError, r.get, url=self.url, params=params)
