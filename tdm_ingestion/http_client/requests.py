import logging
import json
from typing import Union, List, Dict

import requests
from tdm_ingestion.http_client.base import Http

logger = logging.getLogger(__name__)


class Requests(Http):
    def post(self, url: str, data: Union[List, Dict, str] = None,
             headers: Dict[str, str] = None
             ) -> Union[List, Dict]:
        data = data or {}
        headers = headers or {}
        headers['content-type'] = 'application/json'
        logger.debug("doing POST with url %s and json %s and headers %s", url, data, headers)
        r = requests.post(url, data=data, headers=headers)
        logger.debug("Response from server is %s", r.status_code)
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
