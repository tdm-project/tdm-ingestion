import asyncio
import functools
import logging
from typing import List

from aiokafka import AIOKafkaConsumer
from tdm_ingestion.ingestion import Consumer, Message

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class KafkaAIOConsumer(Consumer):

    def __init__(self,
                 bootstrap_servers: List[str],
                 topics: List[str],
                 group_id: str = 'tdm_ingestion',
                 loop=None,
                 **kwargs
                 ):
        loop = loop or asyncio.get_event_loop()
        self.consumer = AIOKafkaConsumer(*topics,
                                         bootstrap_servers=bootstrap_servers,
                                         loop=loop, group_id=group_id,
                                         **kwargs)
        loop.run_until_complete(asyncio.ensure_future(self.consumer.start()))

    async def poll(self, timeout_s: int = -1, max_records: int = -1
                   ) -> List[Message]:
        timeout_s = timeout_s if timeout_s > 0 else 0
        max_records = max_records if max_records > 0 else None

        logger.debug("async polling")
        data = await self.consumer.getmany(timeout_ms=timeout_s,
                                           max_records=max_records)
        logger.debug(f"{data}")
        try:
            values = list(data.values())
            if values:
                logger.debug(f"values: {values}")
                return [Message(m.key, m.value) for m in
                        functools.reduce(list.extend, list(data.values()))]
        except Exception as ex:
            logging.exception(ex)
