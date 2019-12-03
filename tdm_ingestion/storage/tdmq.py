import logging
from typing import List, Dict, Type
from typing import Union, Set

from tdm_ingestion.tdmq.base import Client
from tdm_ingestion.tdmq.models import EntityType, Source, Record
from tdm_ingestion.tdmq.remote import AsyncClient
from tdm_ingestion.utils import import_class


logger = logging.getLogger(__name__)
logger.debug(__name__)
class CachedStorage:
    def __init__(self, client: Client):
        self.client = client
        self._cache: Set = set()

    @classmethod
    def create_from_json(cls, json: Dict):
        client = json['client']
        return CachedStorage(
            import_class(client['class']).create_from_json(client['args']))

    def _idempotent_create_source(self, obj: Source):
        if obj._id not in self._cache:
            logger.debug('querying if source %s exists', obj._id)
            if self.client.sources_count(query={'id': obj._id}) <= 0:
                self.client.create_sources([obj])
            self._cache.add(obj._id)

    def write(self, time_series: List[Record]):
        if time_series:
            for ts in time_series:
                self._idempotent_create_source(ts.source)
            self.client.create_time_series(time_series)


class AsyncCachedStorage:
    # FIXME duplicated code
    def __init__(self, client: AsyncClient):
        self.client = client
        self._cache: Dict[Type, Set[str]] = {EntityType: set(),
                                             Source: set()}

    @classmethod
    def create_from_json(cls, json: Dict):
        client = json['client']
        return AsyncCachedStorage(
            import_class(client['class']).create_from_json(client['args']))

    async def _idempotent_create(self, obj: Union[EntityType, Source]):
        if obj._id not in self._cache[obj.__class__]:
            query = {'name': obj._id}
            if isinstance(obj, Source):
                count_method = self.client.sources_count
                create_method = self.client.create_sources
            else:
                count_method = self.client.entity_types_count
                create_method = self.client.create_entity_types

            if await count_method(query=query) <= 0:
                await create_method([obj])
            self._cache[obj.__class__].add(obj._id)

    async def write(self, time_series: List[Record]):
        if time_series:
            for ts in time_series:
                logger.debug(f"try create sensor  for ts {ts}")
                await self._idempotent_create(ts.source)

            await self.client.create_time_series(time_series)
