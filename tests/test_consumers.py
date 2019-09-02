import datetime
import unittest

from tdm_ingestion.consumers.tdmq_consumer import TDMQConsumer, BucketOperation
from tdm_ingestion.models import EntityType, Source, Point, \
    Record
from tests.dummies import DummyTDMQClient

now = datetime.datetime.now(datetime.timezone.utc)
entity_types = [
    EntityType('st1', 'type1'),
    EntityType('st2', 'type2')
]
sources = [
    Source('s1', entity_types[0], Point(0, 0), ['temp']),
    Source('s2', entity_types[1], Point(1, 1), ['temp'])
]
time_series = [
    Record(now, sources[0], {'value': 0.0}),
    Record(now, sources[1], {'value': 1.0})
]


class TestTDMQConsumer(unittest.TestCase):
    def test_poll(self):
        client = DummyTDMQClient()
        client.create_time_series(time_series)
        consumer = TDMQConsumer(client, sources[0].type, 60,
                                BucketOperation.avg, before=now)
        self.assertEqual(len(consumer.poll()), 1)
