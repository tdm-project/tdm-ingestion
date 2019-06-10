from abc import ABC, abstractmethod
from consumer import Message
from typing import List


class Storage(ABC):
    @abstractmethod
    def write(self, messages: List[Message]):
        pass
