import logging
from typing import Dict, List, Set, Type, Union

from tdm_ingestion.tdmq.base import Client
from tdm_ingestion.tdmq.models import EntityType, Record, Source
from tdm_ingestion.tdmq.remote import AsyncClient, DuplicatedEntryError, GenericHttpError
from tdm_ingestion.utils import import_class

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
logger.debug(__name__)


def log_level():
    return logger.getEffectiveLevel()


def set_log_level(level):
    logger.setLevel(level)


class CachedStorage:
    def __init__(self, client: Client):
        self.client = client
        self._cache: Set = set()

    @classmethod
    def create_from_json(cls, json: Dict):
        client = json["client"]
        return CachedStorage(import_class(client["class"]).create_from_json(client["args"]))

    # def _idempotent_create_entity_type(self, obj: EntityType):
    #     # First try to create the entity type
    #     try:
    #         self.client.create_entity_types(obj.type_)
    #         logger.debug("trying to create entity type %s")
    #     except GenericHttpError:
    #         logger.debug("error occurred creating entity type")

    def _idempotent_create_source(self, obj: Source):
        if obj.id_ not in self._cache:
            logger.debug("creating the source with id: %s", obj.id_)
            try:
                self.client.create_sources([obj])
            except DuplicatedEntryError:
                logger.debug("source alredy present")
                self._cache.add(obj.id_)
            except GenericHttpError:
                logger.debug("error occurred creating the source")
                return False
            else:
                logger.debug("successfully created the source")
        return True

    def write(self, time_series: List[Record]):
        ts_to_write = []
        for ts in time_series:
            if self._idempotent_create_source(ts.source) is True:
                # Only if the source was created correctly or if it was already present the time series will be written
                ts_to_write.append(ts)
        if len(ts_to_write) > 0:
            self.client.create_time_series(ts_to_write)


class AsyncCachedStorage:
    # FIXME duplicated code
    def __init__(self, client: AsyncClient):
        self.client = client
        self._cache: Dict[Type, Set[str]] = {EntityType: set(),
                                             Source: set()}

    @classmethod
    def create_from_json(cls, json: Dict):
        client = json["client"]
        return AsyncCachedStorage(
            import_class(client["class"]).create_from_json(client["args"]))

    async def _idempotent_create(self, obj: Union[EntityType, Source]):
        if obj.id_ not in self._cache[obj.__class__]:
            query = {"name": obj.id_}
            if isinstance(obj, Source):
                count_method = self.client.sources_count
                create_method = self.client.create_sources
            else:
                count_method = self.client.entity_types_count
                create_method = self.client.create_entity_types

            if await count_method(query=query) <= 0:
                await create_method([obj])
            self._cache[obj.__class__].add(obj.id_)

    async def write(self, time_series: List[Record]):
        if time_series:
            for ts in time_series:
                logger.debug("try create sensor  for ts %s", ts)
                await self._idempotent_create(ts.source)

            await self.client.create_time_series(time_series)
