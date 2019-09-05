import datetime
import unittest

from tdm_ingestion.consumers.tdmq_consumer import TDMQConsumer, \
    BucketOperation, TimeDelta
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


class TestTimeDelta(unittest.TestCase):
    def test_get_before_after_one_hour(self):
        td = TimeDelta.one_hour
        time = datetime.datetime(year=2000, day=1, month=12, hour=0, minute=0)
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
        td = TimeDelta('1d')
        time = datetime.datetime(year=2000, day=1, month=12, hour=0, minute=0)
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
