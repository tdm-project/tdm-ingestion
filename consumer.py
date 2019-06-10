from abc import ABC, abstractmethod
from typing import List

class Message:
    def __init__(self, key: str, value: str):
        self.key = key
        self.key = value


class Consumer(ABC):
    @abstractmethod
    def poll(self, timeout_ms: int=0, max_records: int=0) -> List[Message]:
        pass
