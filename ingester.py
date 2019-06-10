from consumer import Consumer
from storage import Storage


class Ingester:
    def __init__(self, consumer: Consumer, storage: Storage):
        self.consumer = consumer
        self.storage = storage

    def process(self, timeout_ms: int=0, max_records: int=0):
        self.storage.write(self.consumer.poll())