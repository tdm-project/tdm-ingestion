import logging
from typing import Union, List, Dict

import requests
from tdm_ingestion.http_client.base import Http


class Requests(Http):
    def post(self, url: str, json: Union[List, Dict, str] = None,
             headers: Dict[str, str] = None
             ) -> Union[List, Dict]:
        json = json or {}
        logging.debug("doing POST with url %s and json %s", url, json)
        headers = headers or {}
        headers['content-type'] = 'application/json'
        r = requests.post(url, data=json, headers=headers)
        r.raise_for_status()
        return r.json()

    def get(self, url, params: Union[List, Dict] = None,
            headers: Dict[str, str] = None
            ) -> Union[List, Dict]:
        params = params or {}
        headers = headers or {}
        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()
        return r.json()
