import unittest
from storage import Storage
from consumer import Consumer, Message
from ingester import Ingester
from typing import List


class DummyStorage(Storage):
    def __init__(self):
        self.messages = []

    def write(self, messages: List[Message]):
        self.messages += messages

class DummyConsumer(Consumer):
    def poll(self, timeout_ms=0, max_records=0)-> List[Message]:
        return [Message('key', 'value')]


class TestIngester(unittest.TestCase):
    
    def test_process(self):
        storage = DummyStorage()
        ingester = Ingester(DummyConsumer(), storage)
        ingester.process()
        self.assertAlmostEquals(len(storage.messages), 1)

if __name__ == '__main__':
    unittest.main()
