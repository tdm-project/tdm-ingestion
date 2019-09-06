from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Union

from tdm_ingestion.consumers.tdmq_consumer import BucketOperation
from tdm_ingestion.tdmq.models import EntityType, Record


class BaseKafkaConsumer(ABC):
    @abstractmethod
    def poll(self, timeout_s: int = -1, max_records: int = 1) -> List[str]:
        pass


class BaseTDMQConsumer(ABC):
    @abstractmethod
    def poll(self,
             entity_type: EntityType,
             bucket: float = None,
             operation: BucketOperation = None,
             before: Union[datetime, str] = None,
             after: Union[datetime, str] = None) -> List[Record]:
        pass
