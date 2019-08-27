import json
from typing import List, Dict, Any

from tdm_ingestion.ingestion import Storage
from tdm_ingestion.models import TimeSeries
from tdm_ingestion.storage.remote_client import Http


class Ckan(Storage):
    @classmethod
    def create_from_json(cls, json: Dict):
        json = json or {}
        return cls(**json)

    def __init__(self, base_url: str, client: Http, api_key: str, dataset: str,
                 resource: str, upsert: bool = False):
        self.base_url = base_url
        self.client = client
        self.headers = {'Authorization': api_key}
        self.dataset = dataset
        self.resource = resource
        self.upsert = upsert

    def _create_resource(self, records: List[Dict[str, Any]]):
        '{"resource": {"package_id": "{PACKAGE-ID}"}, "fields": [ {"id": "a"}, {"id": "b"} ], "records": [ { "a": 1, "b": "xyz"}, {"a": 2, "b": "zzz"} ]}'
        fields = [{"id": field} for field in records[0].keys()]
        data = dict(
            resource=dict(package_id=self.dataset),
            fields=fields,
            records=records
        )

        self.client.post(
            f'{self.base_url}/api/3/action/datastore_create',
            json=json.dumps(data),
            headers=self.headers)

    def write(self, messages: List[TimeSeries]):
        self._create_resource([
            {
                **{'timestamp': ts.time.timestamp(),

                   },
                **ts.data
            }
            for ts in messages])
