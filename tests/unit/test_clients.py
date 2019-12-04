import logging
import unittest
from datetime import datetime, timezone

import httpretty
import jsons
from requests.exceptions import HTTPError

from tdm_ingestion.http_client.requests import Requests
from tdm_ingestion.tdmq.models import EntityType, Point, Record, Source
from tdm_ingestion.tdmq.remote import Client

now = datetime.now(timezone.utc)

SENSORS_TYPE = [
    EntityType("st1", "cat1"),
    EntityType("st2", "cat2")
]
SENSORS = [
    Source("s1", SENSORS_TYPE[0], Point(0, 1), ["temp"]),
    Source("s2", SENSORS_TYPE[1], Point(2, 3), ["temp"])
]
TIME_SERIES = [
    Record(now, SENSORS[0], {"value": 0.0}),
    Record(now, SENSORS[1], {"value": 1.0})
]
# the dictionary returned from the tdmq polystore rest api
REST_SOURCE = {
    "default_footprint": {
        "coordinates": [
            SENSORS[0].geometry.latitude,
            SENSORS[0].geometry.longitude
        ],
        "type": "Point"
    },
    "entity_type": SENSORS_TYPE[0].name,
    "entity_category": SENSORS_TYPE[0].category,
    "external_id": SENSORS[0].id_,
    "stationary": True,
    "tdmq_id": "4d9ae10d-df9b-546c-a586-925e1e9ec049"
}

logger = logging.getLogger("tdm_ingestion")


class TestRemoteClient(unittest.TestCase):

    def setUp(self):
        self.url = "http://foo.bar"

    @httpretty.activate
    def test_create_entity_types(self):
        """
        Tests correct client answer
        """
        expected_response = [s.name for s in SENSORS_TYPE]

        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.entity_types_url, body=jsons.dumps(expected_response))

        res = client.create_entity_types(SENSORS_TYPE)
        self.assertEqual(res, expected_response)

    @httpretty.activate
    def test_create_entity_types_error(self):
        """
        Tests that when an error occurs it returns None
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.entity_types_url, status=400)
        res = client.create_entity_types(SENSORS_TYPE)

        self.assertIsNone(res)

    @httpretty.activate
    def test_create_sources(self):
        """
        Tests correct client answer
        """
        expected_response = [s.id_ for s in SENSORS]

        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.sources_url, body=jsons.dumps(expected_response))

        res = client.create_sources(SENSORS)
        self.assertEqual(res, expected_response)

    @httpretty.activate
    def test_create_source_error(self):
        """
        Tests that when an error occurs it returns None
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.sources_url, status=400)

        res = client.create_sources(SENSORS)
        self.assertIsNone(res)

    @httpretty.activate
    def test_create_time_series(self):
        expected_response = jsons.dumps(TIME_SERIES)

        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.records_url, body=jsons.dumps(expected_response))

        res = client.create_time_series(TIME_SERIES)
        self.assertEqual(res, expected_response)

    @httpretty.activate
    def test_create_time_series_error(self):
        """
        Tests that when an error occurs it returns None
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.POST, client.records_url, status=400)
        res = client.create_time_series(TIME_SERIES)

        self.assertIsNone(res)

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
        httpretty.register_uri(httpretty.GET, f'{client.sources_url}/{REST_SOURCE["tdmq_id"]}',
                               body=jsons.dumps(REST_SOURCE), match_querystring=False)

        res = client.get_sources(REST_SOURCE["tdmq_id"])

        self.assertEqual(res.to_json(), expected_source.to_json())

    @httpretty.activate
    def test_get_all_sources(self):
        """
        Tests getting all sources
        """
        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, client.sources_url,
                               body=jsons.dumps([REST_SOURCE]), match_querystring=False)

        res = client.get_sources()

        expected_sources = [
            Source(
                id_=REST_SOURCE["external_id"],
                type_=EntityType(REST_SOURCE["entity_type"], REST_SOURCE["entity_category"]),
                geometry=Point(*REST_SOURCE["default_footprint"]["coordinates"][::-1]),
                controlled_properties=None,
                tdmq_id=REST_SOURCE["tdmq_id"]
            )
        ]
        self.assertEqual([s.to_json() for s in res],
                         [s.to_json() for s in expected_sources])

    @httpretty.activate
    def test_get_sources_error(self):
        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, client.sources_url, status=400, match_querystring=False)
        httpretty.register_uri(httpretty.GET, f"{client.sources_url}/{SENSORS[0].id_}", status=400, match_querystring=False)

        res = client.get_sources(SENSORS[0].id_)
        self.assertIsNone(res)

        res = client.get_sources()
        self.assertIsNone(res)

    @httpretty.activate
    def test_get_sources_count(self):

        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, client.sources_url,
                               body=jsons.dumps([REST_SOURCE]), match_querystring=False)

        res = client.sources_count()
        self.assertEqual(res, 1)

    @httpretty.activate
    def test_get_sources_count_error(self):
        client = Client(self.url)
        httpretty.register_uri(httpretty.GET, client.sources_url, status=400)

        res = client.sources_count()
        self.assertIsNone(res)


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
            self.assertEqual(jsons.loads(request.body), {"foo": "bar"})
            self.assertEqual(request.headers.get("Content-Type"), headers["Content-Type"])
            self.assertEqual(request.headers.get("Authorization"), headers["Authorization"])
            return [201, response_headers, jsons.dumps(response_body)]

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
