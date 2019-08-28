from enum import Enum
from typing import List, Any

from tdm_ingestion.ingestion import Consumer
from tdm_ingestion.models import EntityType
from tdm_ingestion.tdmq import Client


class BucketOperation(Enum):
    avg = 'avg'
    sum = 'sum'
    count = 'count'


class TDMQConsumer(Consumer):

    def __init__(self,
                 client: Client,
                 entity_type: EntityType,
                 bucket: float,
                 operation: BucketOperation):
        self.client = client
        self.entity_type = entity_type
        self.bucket = bucket
        self.operation = operation

    def poll(self, timeout_s: int = -1, max_records: int = -1) -> List[Any]:
        sources = self.client.get_sources(
            query={'entity_type': self.entity_type.name})
        res = []
        for source in sources:
            res += [self.client.get_time_series(
                source, {'bucket': self.bucket,
                         'operation': self.operation
                         })
            ]
        return res
