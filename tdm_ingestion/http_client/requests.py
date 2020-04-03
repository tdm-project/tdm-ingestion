import logging
from typing import Union, List, Dict

import requests
from tdm_ingestion.http_client.base import Http

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


def log_level():
    return logger.getEffectiveLevel()


def set_log_level(level):
    logger.setLevel(level)


class Requests(Http):
    def post(self, url: str, data: Union[List, Dict, str] = None,
             headers: Dict[str, str] = None) -> Union[List, Dict]:
        data = data or {}
        headers = headers or {}
        headers['content-type'] = 'application/json'
        logger.debug("doing POST with url %s and headers %s", url, headers)
        r = requests.post(url, data=data, headers=headers)
        logger.debug("Response status from server is %s", r.status_code)
        r.raise_for_status()
        return r.json()

    def get(self, url, params: Union[List, Dict] = None,
            headers: Dict[str, str] = None) -> Union[List, Dict]:
        params = params or {}
        headers = headers or {}
        logger.debug("doing GET with url %s and headers %s", url, headers)
        r = requests.get(url, params=params, headers=headers)
        logger.debug("Response status from server is %s", r.status_code)
        r.raise_for_status()
        return r.json()
