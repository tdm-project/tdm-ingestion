import unittest
from ingestion import Ingester
from dummies import DummyConsumer, DummyStorage, DummyConverter


class TestIngester(unittest.TestCase):
    
    def test_process(self):
        storage = DummyStorage()
        ingester = Ingester(DummyConsumer([], []), storage, DummyConverter())
        ingester.process()
        self.assertAlmostEquals(len(storage.messages), 1)

if __name__ == '__main__':
    unittest.main()
