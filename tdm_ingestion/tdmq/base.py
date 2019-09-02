from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union

from tdm_ingestion.models import EntityType, Source, Record


class Client(ABC):
    class NotFound(Exception):
        pass

    @abstractmethod
    def create_entity_types(self,
                            sensor_types: List[EntityType]) -> List[str]:
        pass

    @abstractmethod
    def create_sources(self, sensors: List[Source]) -> List[str]:
        pass

    @abstractmethod
    def create_time_series(self, time_series: List[Record]):
        pass

    @abstractmethod
    def get_time_series(self, source: Source, query: Dict[str, Any]) -> List[
        Record]:
        pass

    @abstractmethod
    def get_entity_types(self, _id: str = None,
                         query: Dict = None) -> EntityType:
        pass

    @abstractmethod
    def get_sources(self, _id: str = None, query: Dict = None) -> Union[
        Source, List[Source]]:
        pass

    @abstractmethod
    def sources_count(self, query: Dict) -> int:
        pass

    @abstractmethod
    def entity_types_count(self, query: Dict) -> int:
        pass
