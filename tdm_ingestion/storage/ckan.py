import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any

import jsons
from tdm_ingestion.http_client.base import Http
from tdm_ingestion.ingestion import Storage
from tdm_ingestion.models import Record
from tdm_ingestion.utils import import_class


class CkanClient(ABC):
    @abstractmethod
    def create_resource(self,
                        resource: str,
                        dataset: str,
                        records: List[Dict[str, Any]],
                        upsert: bool = False
                        ) -> None:
        pass

    def delete_resource(self, resource_id: str):
        pass

    def get_dataset_info(self, dataset: str) -> Dict:
        pass

class RemoteCkan(CkanClient):
    @classmethod
    def create_from_json(cls, json: Dict):
        client = json['client']
        return cls(json['base_url'], import_class(client['class'])(),
                   json['api_key'])

    def __init__(self, base_url: str, client: Http, api_key: str):
        self.base_url = base_url
        self.client = client
        self.headers = {'Authorization': api_key}

    def delete_resource(self, resource_id: str):
        logging.debug('deleteing resource %s', resource_id)
        self.client.post(
            f'{self.base_url}/api/3/action/resource_delete',
            headers=self.headers,
            json=jsons.dumps(dict(id=resource_id))
        )

    def get_dataset_info(self, dataset: str) -> Dict:
        return self.client.get(
            f'{self.base_url}/api/3/action/package_show',
            headers=self.headers,
            params=dict(id=dataset)
        )['result']

    def create_resource(self, resource: str, dataset: str,
                        records: List[Dict[str, Any]],
                        upsert: bool = False) -> None:
        logging.debug('create_resource %s %s, %s', resource, dataset, records)
        if records:
            if upsert:
                logging.debug('upsert is true, remove resource first')
                for r in self.get_dataset_info(dataset)['resources']:
                    if r['name'] == resource:
                        self.delete_resource(r['id'])

            fields = [{"id": field} for field in records[0].keys()]
            data = dict(
                resource=dict(package_id=dataset, name=resource),
                fields=fields,
                records=records
            )

            self.client.post(
                f'{self.base_url}/api/3/action/datastore_create',
                json=jsons.dumps(data),
                headers=self.headers)


class CkanStorage(Storage):
    @classmethod
    def create_from_json(cls, json: Dict):
        client = json['client']
        return cls(
            import_class(client['class']).create_from_json(
                client['args']),
            json['dataset'],
            json['resource'],
            json.get('upsert', False)
        )

    def __init__(self, client: CkanClient, dataset: str,
                 resource: str, upsert: bool = False):
        self.client = client
        self.dataset = dataset
        self.resource = datetime.now().strftime(resource)
        self.upsert = upsert

    def write(self, records: List[Record]):
        self.client.create_resource(self.resource, self.dataset, [
            {
                **{
                    'station': ts.source._id,
                    'type': ts.source.type.category,
                    'date': ts.time,
                    'location': f'{ts.source.geometry.latitude},{ts.source.geometry.longitude}'
                },
                **ts.data
            }
            for ts in records], upsert=self.upsert)
