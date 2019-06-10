import unittest
from ingester import Ingester, DummyConsumer, DummyStorage
from typing import List


class TestIngester(unittest.TestCase):
    
    def test_process(self):
        storage = DummyStorage()
        ingester = Ingester(DummyConsumer('', ''), storage)
        ingester.process()
        self.assertAlmostEquals(len(storage.messages), 1)

if __name__ == '__main__':
    unittest.main()
