import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any

from tdm_ingestion.http.base import Http
from tdm_ingestion.ingestion import Storage
from tdm_ingestion.models import TimeSeries


class CkanClient(ABC):
    @abstractmethod
    def create_resource(self,
                        resource: str, dataset: str,
                        records: List[Dict[str, Any]],
                        upsert: bool = False
                        ) -> None:
        pass


class RemoteCkan(CkanClient):

    def __init__(self, base_url: str, client: Http, api_key: str):
        self.base_url = base_url
        self.client = client
        self.headers = {'Authorization': api_key}

    def create_resource(self, resource: str, dataset: str,
                        records: List[Dict[str, Any]],
                        upsert: bool = False) -> None:
        fields = [{"id": field} for field in records[0].keys()]
        data = dict(
            resource=dict(package_id=dataset),
            fields=fields,
            records=records
        )

        self.client.post(
            f'{self.base_url}/api/3/action/datastore_create',
            json=json.dumps(data),
            headers=self.headers)


class CkanStorage(Storage):
    @classmethod
    def create_from_json(cls, json: Dict):
        json = json or {}
        return cls(**json)

    def __init__(self, client: CkanClient, dataset: str,
                 resource: str, upsert: bool = False):
        self.client = client
        self.dataset = dataset
        self.resource = resource
        self.upsert = upsert

    def write(self, messages: List[TimeSeries]):
        self.client.create_resource(self.resource, self.dataset, [
            {
                **{'timestamp': ts.time.timestamp(),

                   },
                **ts.data
            }
            for ts in messages])
