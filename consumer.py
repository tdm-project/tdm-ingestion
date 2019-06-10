from abc import ABC, abstractmethod
from typing import List

class Message:
    def __init__(self, key: str, value: str):
        self.key = key
        self.key = value


class AbstractConsumer(ABC):
    def __init__(self, bootstrap_servers: List[str], topics: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics

    @abstractmethod
    def poll(self, timeout_ms: int=-1, max_records: int=-1) -> List[Message]:
        pass
