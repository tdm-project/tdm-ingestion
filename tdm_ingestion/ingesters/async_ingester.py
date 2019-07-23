import asyncio
import logging
from abc import ABC, abstractmethod

from tdm_ingestion.ingestion import Consumer, MessageConverter, \
    Storage, BasicIngester

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class AsyncElement(ABC):
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    @abstractmethod
    async def process(self):
        pass


class AsyncSource(AsyncElement):
    def __init__(self, queue, func, *args, **kwargs):
        super().__init__(func, *args, **kwargs)
        self.queue = queue

    async def process(self, forever: bool = False):
        async def _process():
            data = await self.func(*self.args, **self.kwargs)
            logger.debug(f"Source._process {data}")
            if data:
                await self.queue.put(data)
                logger.debug("inserted into the queue")
        logger.debug('source')
        await _process()
        while forever:
            await _process()


class AsyncSink(AsyncElement):
    def __init__(self, queue, func, *args, **kwargs):
        super().__init__(func, *args, **kwargs)
        self.queue = queue

    async def process(self, forever: bool = False):
        async def _process():
            logger.debug("Sink._process")
            messages = await self.queue.get()
            logging.debug('messages to store %s', messages)
            await self.func(messages, *self.args, **self.kwargs)

        await _process()
        while forever:
            await _process()


class AsyncStage(AsyncElement):
    def __init__(self, in_queue, out_queue, func, *args, **kwargs):
        super().__init__(func, *args, **kwargs)
        self.in_queue = in_queue
        self.out_queue = out_queue

    async def process(self, forever: bool = False):
        async def _process():
            logger.debug("Stage._process")
            messages = await self.in_queue.get()
            logging.debug('messages to convert %s', messages)
            await self.out_queue.put(
                self.func(messages, *self.args, **self.kwargs))

        await _process()
        while forever:
            await _process()


class Pipeline(ABC):
    def __init__(self):
        self.elements = []

    def connect(self, *elements: AsyncElement):
        self.elements = elements

    @abstractmethod
    def run_forever(self, *args, **kwargs):
        pass

    @abstractmethod
    def run_until_complete(self, *args, **kwargs):
        pass


class AsyncPipeline(Pipeline):
    def __init__(self, loop=None, *args, **kwargs):
        super().__init__()
        self.loop = loop or asyncio.get_event_loop()

    def _run(self, forever: bool = False) -> asyncio.Future:
        return asyncio.ensure_future(
            asyncio.gather(*[el.process(forever) for el in self.elements]))

    def run_forever(self, callback=None, *args, **kwargs):
        future = self._run(True)
        if callback:
            future.add_done_callback(callback)
        try:
            self.loop.run_forever()
        except Exception as ex:
            logging.exception(ex)
        finally:
            self.loop.close()

#    def run_forever(self, callback=None, *args, **kwargs):
#        self.loop.run_until_complete(self._run(True))

    def run_until_complete(self):
        self.loop.run_until_complete(self._run())


class AsyncIngester(BasicIngester):

    def __init__(self, consumer: Consumer, storage: Storage,
                 converter: MessageConverter, pipeline: AsyncPipeline = None):
        super().__init__(consumer, storage, converter)
        self.pipeline = pipeline or AsyncPipeline()
        messages_queue: asyncio.Queue = asyncio.Queue(loop=self.pipeline.loop)
        timeseries_queue: asyncio.Queue = asyncio.Queue(
            loop=self.pipeline.loop)
        self.elements = [
            AsyncSource(messages_queue, consumer.poll),
            AsyncStage(messages_queue, timeseries_queue,
                       converter.convert),
            AsyncSink(timeseries_queue, storage.write)
        ]
        self.pipeline.connect(*self.elements)

    def _set_first_element_args(self, args, kwargs):
        self.elements[0].args = args
        self.elements[0].kwargs = kwargs

    def process(self, *args, **kwargs):
        self._set_first_element_args(args, kwargs)
        self.pipeline.run_until_complete()

    def process_forever(self, *args, **kwargs):
        logging.info(f'process forever {args}, {kwargs}')
        self._set_first_element_args(args, kwargs)
        self.pipeline.run_forever()
