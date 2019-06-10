import json
import time
from typing import List

from ingestion import Consumer, Storage, TimeSeries, Message, MessageConverter


class DummyStorage(Storage):
    def __init__(self):
        self.messages = []

    def write(self, messages: List[TimeSeries]):
        self.messages += messages


class DummyConsumer(Consumer):
    def poll(self, timeout_ms=0, max_records=0) -> List[Message]:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        return [Message('key', f'{{"time":"{now}", "sensorcode":1, "measure": {{"value": "test"}} }}')]


class DummyConverter(MessageConverter):
    def convert(self, messages: List[Message]) -> List[TimeSeries]:
        series = []
        for m in messages:
            m = json.loads(m.value)
            series.append(TimeSeries(m['time'], m['sensorcode'], m['measure']))

        return series
