from datetime import datetime
from enum import Enum
from typing import List, Dict, Union

from tdm_ingestion.ingestion import Consumer
from tdm_ingestion.models import EntityType, Record
from tdm_ingestion.tdmq import Client
from tdm_ingestion.utils import import_class


class BucketOperation(Enum):
    avg = 'avg'
    sum = 'sum'
    count = 'count'


class TDMQConsumer(Consumer):

    @classmethod
    def create_from_json(cls, json: Dict):
        json = json or {}
        client = json['client']
        return cls(
            import_class(client['class']).create_from_json(client['args']),
            EntityType(**json['entity_type']['args']),
            json.get('bucket'),
            json.get('operation'),
            json.get('before'),
            json.get('after')
        )

    def __init__(self,
                 client: Client,
                 entity_type: EntityType,
                 bucket: float = None,
                 operation: BucketOperation = None,
                 before: Union[datetime, str] = None,
                 after: Union[datetime, str] = None
                 ):
        self.client = client
        self.entity_type = entity_type
        self.bucket = bucket
        self.operation = operation
        self.after = after
        self.before = before
        self.after = after
        self.before = before

        if self.bucket:
            assert self.operation is not None

    def poll(self, timeout_s: int = -1, max_records: int = -1) -> List[Record]:
        sources = self.client.get_sources(
            query={'entity_type': self.entity_type.name})
        res = []
        params = {}
        if self.bucket is not None:
            params['bucket'] = self.bucket
            params['op'] = self.operation

        for p in ('after', 'before'):
            attr = getattr(self, p)
            if isinstance(attr, str):
                params[p] = attr
            elif isinstance(attr, datetime):
                params[p] = attr.isoformat()

        if self.after is not None:
            params['after'] = self.after

        if self.before is not None:
            params['before'] = self.before

        for source in sources:
            res += self.client.get_time_series(
                source,
                params
            )
        return res
