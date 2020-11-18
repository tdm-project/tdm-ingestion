import logging
from typing import Dict, Union, List

import aiohttp
from tdm_ingestion.http_client.base import Http

logger = logging.getLogger(__name__)


class AioHttp(Http):
    def __init__(self, auth_token=None):
        super(AioHttp, self).__init__(auth_token)
        self.session = aiohttp.ClientSession()

    async def post(self, url: str, data: Union[List, Dict, str] = None,
                   headers: Dict[str, str] = None
                   ) -> Union[List, Dict]:
        data = data or {}
        
        headers = headers or {}
        headers.update(self.headers)
        headers.update({'content-type': 'application/json'})

        logger.debug("doing POST with url %s and json %s", url, data)
        r = await self.session.post(url, data=data,
                                    headers=headers)
        r.raise_for_status()
        return await r.json()

    async def get(self, url, params: Union[List, Dict] = None,
                  headers: Dict[str, str] = None
                  ) -> Union[List, Dict]:
        params = params or {}

        headers = headers or {}
        headers.update(self.headers)
        headers.update({'content-type': 'application/json'})

        resp = await self.session.get(url, params=params, headers=headers)
        resp.raise_for_status()
        return await resp.json()
