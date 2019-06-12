import unittest

from tdm_ingestion.ingestion import Ingester
from tests.dummies import DummyConsumer, DummyStorage, DummyConverter


class TestIngester(unittest.TestCase):

    def test_process(self):
        storage = DummyStorage()
        ingester = Ingester(DummyConsumer(), storage, DummyConverter())
        ingester.process()
        self.assertEqual(len(storage.messages), 1)


class TestTimeSeries(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
