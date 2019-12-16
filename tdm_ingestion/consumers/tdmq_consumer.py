
import logging
from datetime import datetime
from enum import Enum
from typing import List, Union

from tdm_ingestion.tdmq import Client
from tdm_ingestion.tdmq.models import EntityType, Record
from tdm_ingestion.tdmq.remote import GenericHttpError

logger = logging.getLogger(__name__)

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

        if bucket is not None:
            assert operation is not None, "operation cannot be None if bucket is specified"

        logger.debug("getting sources from tdmq")
        try:
            sources = self.client.get_sources(query={'entity_type': entity_type.name})
        except GenericHttpError:
            logger.debug("error getting sources from tdmq server")
            return []

        records = []
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
            logger.debug("getting time series for source %s", source.id_)
            try:
                times_series = self.client.get_time_series(source, params)
            except GenericHttpError:
                logger.debug("error getting time series from tdmq server")
            else:
                records += times_series
        return records
