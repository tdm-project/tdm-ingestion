import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List
from urllib.parse import urljoin

import jsons
import re
from requests.exceptions import HTTPError

from tdm_ingestion.http_client.base import Http
from tdm_ingestion.tdmq.models import Record
from tdm_ingestion.utils import import_class, daterange

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


def log_level():
    return logger.getEffectiveLevel()


def set_log_level(level):
    logger.setLevel(level)


class CkanClient(ABC):
    @abstractmethod
    def create_resource(self,
                        records: List[Dict[str, Any]],
                        dataset: str,
                        resource: str,
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
        self.dataset_reorder_url = urljoin(self.base_url, "api/3/action/package_resource_reorder")
        self.client = client
        self.headers = {"Authorization": api_key}

        self.ckan_type_mapper = {
            str: "text",
            float: "float"
        }

    def _get_fields_from_records(self, records: Dict[str, List[Record]]):
        """
        It reads from the records all the fields with the correspondant datatype.
        This is necessary because data from the different sensors can have different fields.
        If Ckan finds a record with fields different from the first one, it fails the loading,
        unless the fields with the datatypes are listed in the message
        """
        fields_cache = {"station", "type", "date", "location"}
        fields_structs = [
            {"id": "station", "type": "text"},
            {"id": "type", "type": "text"},
            {"id": "date", "type": "text"},
            {"id": "location", "type": "text"}
        ]

        for _, station_records in records.items():
            record = station_records[0]  # we need only one record. Other records have the same struct
            new_fields = set(record.data.keys()) - fields_cache  # adds only fields not already present

            fields_structs += [{
                "id": field,
                "type": self.ckan_type_mapper.get(type(record.data[field]), "text")
            } for field in new_fields]

            fields_cache = fields_cache.union(new_fields)
        return fields_structs

    def _get_dict_records(self, records: Dict[str, List[Record]]):
        new_records = []
        for _, station_records in records.items():
            new_records += [{**{
                "station": record.source.id_,
                "type": record.source.type.category,
                "date": record.time,
                "location": f"{record.footprint.latitude},{record.footprint.longitude}"
            }, **record.data} for record in station_records]
        return new_records

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

    def dataset_reorder(self, dataset: str, resource_id: str):
        try:
            self.client.post(
                self.dataset_reorder_url,
                headers=self.headers,
                data=jsons.dumps(dict(id=dataset, order=[resource_id]))
            )
        except HTTPError:
            logger.error("error occurred getting resources to sort")
            return False

    def create_resource(self, records: Dict[str, List[Record]],
                        dataset: str, resource: str, description: str = "", upsert: bool = False) -> None:
        """
        Create resources in Ckan

        :param records: a dict of list of records. The Dict keys are the id of the station and the items
            are the list of records for that station
        """

        logger.debug("create resource %s, %s, %s", resource, dataset, records)
        if not records:
            return False

        if upsert:
            logger.debug("upsert is true, remove resource first")
            try:
                resources = self.get_dataset_info(dataset)["resources"]
            except HTTPError:
                logger.warning("error querying tdmq for resources. Proceeding without deleting the old resource")
            else:
                for r in resources:
                    if r["name"] == resource:
                        logger.debug("found resource to delete")
                        try:
                            self.delete_resource(r["id"])
                        except HTTPError:
                            logger.warning("error occurred deleting the resource. Proceed without deleting")
                        else:
                            logger.debug("old resource deleted")

        fields = self._get_fields_from_records(records)
        records = self._get_dict_records(records)

        data = {
            "resource": {
                "package_id": dataset,
                "name": resource,
                "description": description
            },
            "fields": fields,
            "records": records
        }

        try:
            res = self.client.post(
                self.resource_create_url,
                data=jsons.dumps(data),
                headers=self.headers
            )
        except HTTPError as e:
            logger.error("error occurred creating new resource on ckan")
            logger.error("error is %s", e.response.text)
            return False
        else:
            self.dataset_reorder(dataset, res["result"]["resource_id"])
        return True

    def prune_resources(self, dataset: str, resource_name: str,
                        after: datetime, before: datetime,
                        prune_weekly: bool) -> None:

        resources_to_delete = []

        daily_prefix = re.sub(r'-weekly.*|-monthly.*', '-daily',
                              resource_name)
        weekly_prefix = re.sub(r'-weekly.*|-monthly.*', '-weekly',
                               resource_name)
        for day in daterange(after, before):
            resources_to_delete.append(
                f"{daily_prefix}-{day.strftime('%Y-%m-%d')}")
            if prune_weekly:
                if day.weekday() == 0 and (day + timedelta(days=7)).month == before.month:
                    resources_to_delete.append(
                        f"{weekly_prefix}-{day.strftime('%Y-%m-%d')}")

        try:
            resources = self.get_dataset_info(dataset)["resources"]
        except HTTPError:
            logger.warning("error querying tdmq for resources. Proceeding without deleting the old resource")
        else:
            for r in resources:
                if r["name"] in resources_to_delete:
                    try:
                        logger.debug("found resource to prune")
                        self.delete_resource(r["id"])
                    except HTTPError:
                        logger.warning("error occurred deleting the resource. Proceed without deleting")
                    else:
                        logger.debug("old resource deleted")


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
              records: Dict[str, List[Record]],
              dataset: str,
              resource: str,
              description: str = "",
              upsert: bool = False):
        return self.client.create_resource(records, dataset, resource,
                                           description, upsert=upsert)

    def prune_resources(self,
                        dataset: str,
                        resource_name: str,
                        after: datetime,
                        before: datetime,
                        prune_weekly: bool) -> None:
        return self.client.prune_resources(dataset, resource_name, after,
                                           before, prune_weekly)
