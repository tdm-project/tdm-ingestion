import logging
from typing import Dict, Union, List

import aiohttp
from tdm_ingestion.http.base import Http

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class AioHttp(Http):
    def __init__(self):
        self.session = aiohttp.ClientSession()

    async def post(self, url: str, json: Union[List, Dict, str] = None,
                   headers: Dict[str, str] = None
                   ) -> Union[List, Dict]:
        json = json or {}
        logger.debug("doing POST with url %s and json %s", url, json)
        r = await self.session.post(url, data=json,
                                    headers={
                                        'content-type': 'application/json'}
                                    )
        r.raise_for_status()
        return await r.json()

    async def get(self, url, params: Union[List, Dict] = None,
                  headers: Dict[str, str] = None
                  ) -> Union[List, Dict]:
        params = params or {}
        resp = await self.session.get(url, params=params)
        resp.raise_for_status()
        return await resp.json()
