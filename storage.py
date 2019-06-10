from abc import ABC, abstractmethod
from consumer import Message
from typing import List


class AbstractStorage(ABC):
    @abstractmethod
    def write(self, messages: List[Message]):
        pass
