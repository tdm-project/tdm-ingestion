from datetime import datetime
from enum import Enum
from typing import List, Union

from tdm_ingestion.tdmq import Client
from tdm_ingestion.tdmq.models import EntityType, Record


class BucketOperation(Enum):
    avg = 'avg'
    sum = 'sum'
    count = 'count'


class TDMQConsumer:

    def __init__(self, client: Client):
        self.client = client

    def poll(self,
             entity_type: EntityType,
             bucket: float = None,
             operation: BucketOperation = None,
             before: Union[datetime, str] = None,
             after: Union[datetime, str] = None) -> List[Record]:

        if bucket:
            assert operation is not None

        sources = self.client.get_sources(
            query={'entity_type': entity_type.name})

        res = []
        params = {}
        if bucket is not None:
            params['bucket'] = bucket
            params['op'] = operation

        if after is not None:
            params['after'] = after.isoformat() if \
                isinstance(after, datetime) else after

        if before is not None:
            params['before'] = before.isoformat() if \
                isinstance(before, datetime) else before

        for source in sources:
            res += self.client.get_time_series(
                source,
                params
            )
        return res
