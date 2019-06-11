import unittest

from dummies import DummyConsumer, DummyStorage, DummyConverter
from ingestion import Ingester, TimeSeries


class TestIngester(unittest.TestCase):
    
    def test_process(self):
        storage = DummyStorage()
        ingester = Ingester(DummyConsumer([], []), storage, DummyConverter())
        ingester.process()
        self.assertEqual(len(storage.messages), 1)



class TestTimeSeries(unittest.TestCase):
    pass
if __name__ == '__main__':
    unittest.main()
