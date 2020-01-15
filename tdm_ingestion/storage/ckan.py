import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List
from urllib.parse import urljoin

import jsons
from requests.exceptions import HTTPError

from tdm_ingestion.http_client.base import Http
from tdm_ingestion.tdmq.models import Record
from tdm_ingestion.utils import import_class

logger = logging.getLogger(__name__)


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
        client = json["client"]
        return cls(json["base_url"], import_class(client["class"])(),
                   json["api_key"])

    def __init__(self, base_url: str, client: Http, api_key: str):
        self.base_url = base_url
        self.resource_delete_url = urljoin(self.base_url, "/api/3/action/resource_delete")
        self.dataset_info_url = urljoin(self.base_url, "/api/3/action/package_show")
        self.resource_create_url = urljoin(self.base_url, "api/3/action/datastore_create")

        self.client = client
        self.headers = {"Authorization": api_key}

    def delete_resource(self, resource_id: str):
        logger.debug("deleting resource %s", resource_id)
        self.client.post(
            self.resource_delete_url,
            headers=self.headers,
            data=jsons.dumps(dict(id=resource_id))
        )

    def get_dataset_info(self, dataset: str) -> Dict:
        return self.client.get(
            self.dataset_info_url,
            headers=self.headers,
            params=dict(id=dataset)
        )["result"]

    def create_resource(self, resource: str, dataset: str,
                        records: List[Dict[str, Any]],
                        upsert: bool = False) -> None:
        logger.debug("create_resource %s %s, %s", resource, dataset, records)
        if not records:
            return False
        
        if upsert:
            logger.debug("upsert is true, remove resource first")
            try:
                resources = self.get_dataset_info(dataset)["resources"]
            except HTTPError:
                logger.error("error querying tdmq for resources. Exiting")
                return False
            else:
                for r in resources:
                    if r["name"] == resource:
                        logger.debug("found resource to delete")
                        try:
                            self.delete_resource(r["id"])
                        except HTTPError:
                            logger.error("error occurred deleting the resource. Proceed without deleting. Exiting")
                            return False
                        else:
                            logger.debug("old resource deleted")

        fields = [{"id": field} for field in records[0].keys()]
        data = dict(
            resource=dict(package_id=dataset, name=resource),
            fields=fields,
            records=records
        )

        try:
            self.client.post(
                self.resource_create_url,
                data=jsons.dumps(data),
                headers=self.headers
            )
        except HTTPError:
            logger.error("error occurred creating new resource on ckan")
            return False
        return True


class Formatter(ABC):
    @abstractmethod
    def format(self, name):
        pass


class DateTimeFormatter(Formatter):
    def __init__(self, time_delta: timedelta = None):
        self.time_delta = time_delta or timedelta()

    def format(self, name):
        return (datetime.now() - self.time_delta).strftime(name)


class CkanStorage:
    def __init__(self, client: CkanClient):
        self.client = client

    def write(self,
              records: List[Record],
              dataset: str,
              resource: str,
              upsert: bool = False):
        return self.client.create_resource(resource, dataset, [
            {
                **{
                    "station": ts.source.id_,
                    "type": ts.source.type.category,
                    "date": ts.time,
                    "location": f"{ts.source.geometry.latitude},{ts.source.geometry.longitude}"
                },
                **ts.data
            }
            for ts in records], upsert=upsert)
