import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from urllib.parse import urljoin

import httpretty
import jsons
from confluent_kafka import KafkaError

from tdm_ingestion.consumers.confluent_kafka_consumer import KafkaConsumer
from tdm_ingestion.consumers.tdmq_consumer import BucketOperation, TDMQConsumer
from tdm_ingestion.tdmq.remote import Client
from tdm_ingestion.utils import TimeDelta

from .data import REST_SOURCE, REST_TIME_SERIES, SENSORS, TIME_SERIES
from .dummies import (DummyConfluentConsumerCorrectMessages,
                      DummyConfluentConsumerErrorMessages)


class TestTDMQConsumer(unittest.TestCase):
    def setUp(self):
        self.tdmq_url = "http://foo.bar"
        self.sources_url = urljoin(self.tdmq_url, 'api/v0.0/sources')
        self.client = Client(self.tdmq_url)

    @httpretty.activate
    def test_poll(self):
        """
        Tests correct message polls
        """
        httpretty.register_uri(httpretty.GET, self.sources_url,
                               body=jsons.dumps([REST_SOURCE]), match_querystring=False)

        httpretty.register_uri(httpretty.GET, f"{self.sources_url}/{SENSORS[0].tdmq_id}/timeseries",
                               body=jsons.dumps(REST_TIME_SERIES), match_querystring=False)

        consumer = TDMQConsumer(self.client)
        records = consumer.poll(SENSORS[0].type)
        self.assertEqual(len(records), 1)

    def test_error_none_operation(self):
        """
        Tests poll raises AssertionError when bucket is specified as input parameter, but operation is None
        """
        consumer = TDMQConsumer(self.client)
        self.assertRaises(AssertionError, consumer.poll, SENSORS[0].type, 60)

    @httpretty.activate
    def test_error_getting_sources(self):
        """
        Tests that, in case of error getting the sources, the consumer returns an empty list
        """
        httpretty.register_uri(httpretty.GET, self.sources_url, status=400)

        consumer = TDMQConsumer(Client(self.tdmq_url))
        records = consumer.poll(SENSORS[0].type)
        self.assertEqual(len(records), 0)

    @httpretty.activate
    def test_no_sources(self):
        """
        Tests that, in case the server returns no sources, the consumer returns an empty list
        """
        httpretty.register_uri(httpretty.GET, self.sources_url, status=200, body="[]")
        consumer = TDMQConsumer(Client(self.tdmq_url))
        records = consumer.poll(SENSORS[0].type)
        self.assertEqual(len(records), 0)

    @httpretty.activate
    def test_error_getting_time_series(self):
        """
        Tests that, in case of error getting the time series, the consumer returns an empty list
        """
        httpretty.register_uri(httpretty.GET, self.sources_url,
                               body=jsons.dumps([REST_SOURCE]), match_querystring=False)

        httpretty.register_uri(httpretty.GET, f"{self.sources_url}/{SENSORS[0].tdmq_id}/timeseries",
                               status=400)

        consumer = TDMQConsumer(self.client)
        records = consumer.poll(SENSORS[0].type)
        self.assertEqual(len(records), 0)

    @httpretty.activate
    def test_no_time_series(self):
        """
        Tests that, in case the server returns no time series, the consumer returns an empty list
        """
        httpretty.register_uri(httpretty.GET, self.sources_url,
                               body=jsons.dumps([REST_SOURCE]), match_querystring=False)

        httpretty.register_uri(httpretty.GET, f"{self.sources_url}/{SENSORS[0].tdmq_id}/timeseries",
                               body=jsons.dumps({"coords": {"time": []}}))

        consumer = TDMQConsumer(self.client)
        records = consumer.poll(SENSORS[0].type)
        print(records)
        self.assertEqual(len(records), 0)

    def test_query_params(self):
        client = Mock()
        client.get_sources = Mock(return_value=[SENSORS[0]])
        client.get_time_series = Mock(return_value=[TIME_SERIES[0]])
        consumer = TDMQConsumer(client)

        bucket = 60
        operation = BucketOperation.avg
        before = datetime.now()
        after = datetime.now() + timedelta(hours=1)
        consumer.poll(SENSORS[0].type, bucket, operation, before, after)

        expected_params = {
            "bucket": bucket,
            "op": operation,
            "before": before.isoformat(),
            "after": after.isoformat()
        }
        client.get_time_series.assert_called_with(SENSORS[0], expected_params)


class TestConfluentKafkaConsumer(unittest.TestCase):
    def test_poll(self):
        """
        Tests correct message poll
        """
        with patch('tdm_ingestion.consumers.confluent_kafka_consumer.ConfluentKafkaConsumer', DummyConfluentConsumerCorrectMessages) as ckc:
            consumer = KafkaConsumer(["foo.bar"], "foo")
            messages = consumer.poll()
            self.assertEqual(messages, [ckc().messages[0].value()])

    def test_poll_message_error(self):
        """
        Tests that, if message.error() is not None, the message is ignored
        """
        with patch('tdm_ingestion.consumers.confluent_kafka_consumer.ConfluentKafkaConsumer', DummyConfluentConsumerErrorMessages):
            consumer = KafkaConsumer(["foo.bar"], "foo")
            messages = consumer.poll()
            self.assertEqual(messages, [])

    def test_poll_exception(self):
        """
        Tests that, if an exception occurs consuming messages, empty list is returned
        """
        with patch('tdm_ingestion.consumers.confluent_kafka_consumer.ConfluentKafkaConsumer') as ckc:
            ckc().consumer = Mock(side_effetct=KafkaError)
            consumer = KafkaConsumer(["foo.bar"], "foo")
            messages = consumer.poll()
            self.assertEqual(messages, [])

        with patch('tdm_ingestion.consumers.confluent_kafka_consumer.ConfluentKafkaConsumer') as ckc:
            ckc().consumer = Mock(side_effetct=RuntimeError)
            consumer = KafkaConsumer(["foo.bar"], "foo")
            messages = consumer.poll()
            self.assertEqual(messages, [])


class TestTimeDelta(unittest.TestCase):
    def test_get_before_after_one_hour(self):
        td = TimeDelta.one_hour
        time = datetime(year=2000, day=1, month=12, hour=0, minute=0)
        before, after = td.get_before_after(time)
        self.assertEqual(before.year, 2000)
        self.assertEqual(before.day, 30)
        self.assertEqual(before.month, 11)
        self.assertEqual(before.hour, 23)
        self.assertEqual(before.minute, 59)

        self.assertEqual(after.year, 2000)
        self.assertEqual(after.day, 30)
        self.assertEqual(after.month, 11)
        self.assertEqual(after.hour, 23)
        self.assertEqual(after.minute, 0)

    def test_get_before_after_one_day(self):
        td = TimeDelta.one_day
        time = datetime(year=2000, day=1, month=12, hour=0, minute=0)
        before, after = td.get_before_after(time)
        self.assertEqual(before.year, 2000)
        self.assertEqual(before.day, 30)
        self.assertEqual(before.month, 11)
        self.assertEqual(before.hour, 23)
        self.assertEqual(before.minute, 59)

        self.assertEqual(after.year, 2000)
        self.assertEqual(after.day, 30)
        self.assertEqual(after.month, 11)
        self.assertEqual(after.hour, 0)
        self.assertEqual(after.minute, 0)
