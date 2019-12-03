import logging
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

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
        # FIXME the url should be impossible to reach
        self.url = "http://localhost"

    def _get_dummy_http_client(self, res, method):
        assert method in ("post", "get")
        dummy_http_client = Requests()
        if res == HTTPError:
            setattr(dummy_http_client, method, Mock(side_effect=res))
        else:
            setattr(dummy_http_client, method, Mock(return_value=res))
        return dummy_http_client

    def test_create_entity_types(self):
        """
        Tests correct client answer
        """
        expected_response = [s.name for s in SENSORS_TYPE]

        dummy_http_client = self._get_dummy_http_client(expected_response, "post")

        client = Client(self.url, dummy_http_client)
        res = client.create_entity_types(SENSORS_TYPE)

        self.assertEqual(res, expected_response)

    def test_create_entity_types_error(self):
        """
        Tests that when an error occurs it returns None
        """
        dummy_http_client = self._get_dummy_http_client(HTTPError, "post")

        client = Client(self.url, dummy_http_client)
        res = client.create_entity_types(SENSORS_TYPE)

        self.assertIsNone(res)

    def test_create_sources(self):
        """
        Tests correct client answer
        """
        expected_response = [s.id_ for s in SENSORS]
        dummy_http_client = self._get_dummy_http_client(expected_response, "post")

        client = Client(self.url, dummy_http_client)
        res = client.create_sources(SENSORS)

        self.assertEqual(res, expected_response)

    def test_create_source_error(self):
        """
        Tests that when an error occurs it returns None
        """
        dummy_http_client = self._get_dummy_http_client(HTTPError, "post")

        client = Client(self.url, dummy_http_client)
        res = client.create_sources(SENSORS)

        self.assertIsNone(res)

    def test_create_time_series(self):
        expected_response = jsons.dumps(TIME_SERIES)
        dummy_http_client = self._get_dummy_http_client(expected_response, "post")

        client = Client(self.url, dummy_http_client)
        res = client.create_time_series(TIME_SERIES)

        self.assertEqual(res, expected_response)

    def test_create_time_series_error(self):
        """
        Tests that when an error occurs it returns None
        """
        dummy_http_client = self._get_dummy_http_client(HTTPError, "post")

        client = Client(self.url, dummy_http_client)
        res = client.create_time_series(TIME_SERIES)

        self.assertIsNone(res)

    def test_get_source_by_id(self):
        """
        Tests getting a source using the tdmq id
        """
        dummy_http_client = self._get_dummy_http_client(REST_SOURCE, "get")

        client = Client(self.url, dummy_http_client)
        res = client.get_sources(REST_SOURCE["tdmq_id"])

        expected_source = Source(
            id_=REST_SOURCE["external_id"],
            type_=EntityType(REST_SOURCE["entity_type"], REST_SOURCE["entity_category"]),
            geometry=Point(*REST_SOURCE["default_footprint"]["coordinates"][::-1]),  # for some strange reason the points are inverted 
            controlled_properties=None,
            tdmq_id=REST_SOURCE["tdmq_id"]
        )
        logger.debug(res.to_json())
        logger.debug(expected_source.to_json())
        self.assertEqual(res.to_json(), expected_source.to_json())

    def test_get_all_sources(self):
        """
        Tests getting a source using the tdmq id
        """
        dummy_http_client = self._get_dummy_http_client([REST_SOURCE], "get")

        client = Client(self.url, dummy_http_client)
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

    def test_get_sources_error(self):
        dummy_http_client = self._get_dummy_http_client(HTTPError, "get")

        client = Client(self.url, dummy_http_client)
        res = client.get_sources(SENSORS[0].id_)
        self.assertIsNone(res)

        res = client.get_sources()
        self.assertIsNone(res)

    def test_get_sources_count(self):
        dummy_http_client = self._get_dummy_http_client([REST_SOURCE], "get")

        client = Client(self.url, dummy_http_client)
        res = client.sources_count()
        self.assertEqual(res, 1)

    def test_get_sources_count_error(self):
        dummy_http_client = self._get_dummy_http_client(HTTPError, "get")

        client = Client(self.url, dummy_http_client)
        res = client.sources_count()
        self.assertIsNone(res)
