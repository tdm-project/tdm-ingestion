import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Tuple, Callable, Dict

from tdm_ingestion.ingestion import Ingester, Consumer, MessageConverter, \
    Storage


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

    async def process(self):
        logging.debug('source')
        await self.queue.put(self.func(*self.args, self.kwargs))


class AsyncSink(AsyncElement):
    def __init__(self, queue, func, *args, **kwargs):
        super().__init__(func, *args, **kwargs)
        self.queue = queue

    async def process(self):
        messages = await self.queue.get()
        logging.debug('messages to store %s', messages)
        self.func(messages, *self.args, **self.kwargs)


class AsyncStage(AsyncElement):
    def __init__(self, in_queue, out_queue, func, *args, **kwargs):
        super().__init__(func, *args, **kwargs)
        self.in_queue = in_queue
        self.out_queue = out_queue

    async def process(self):
        messages = await self.in_queue.get()
        logging.debug('messages to convert %s', messages)
        await self.out_queue.put(
            self.func(messages, *self.args, **self.kwargs))


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

    def _run(self) -> asyncio.Future:
        return asyncio.ensure_future(
            asyncio.gather(*[el.process() for el in self.elements]))

    def run_forever(self, callback=None, *args, **kwargs):
        future = self._run()
        if callback:
            future.add_done_callback(callback)
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()

    def run_until_complete(self):
        self.loop.run_until_complete(self._run())


class AsyncConsumer(AsyncSource):
    def __init__(self, queue: asyncio.Queue, consumer: Consumer,
                 timeout_s: int = -1, max_records: int = -1):
        super().__init__(queue, consumer.poll, timeout_s, max_records)


class AsyncConverter(AsyncElement):
    def __init__(self, queue: asyncio.Queue, converter: MessageConverter):
        super().__init__(queue)
        self.converter = converter

    async def process(self):
        messages = await self.queue.get()
        logging.debug('messages to convert %s', messages)
        await self.queue.put(self.converter.convert(messages))


class AsyncStorage(AsyncElement):
    def __init__(self, queue: asyncio.Queue, storage: Storage):
        super().__init__(queue)
        self.storage = storage

    async def process(self):
        messages = await self.queue.get()
        logging.debug('messages to store %s', messages)

        return self.storage.write(messages)


class AsyncIngester(Ingester):
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def process(self, *args, **kwargs):
        self.pipeline.run_until_complete(*args, **kwargs)

    def process_forever(self, *args, **kwargs):
        self.pipeline.run_forever(*args, **kwargs)


def async_ingester_factory(funcs_and_kwargs: List[Tuple[Callable, Dict]],
                           pipeline_class=AsyncPipeline,
                           pipeline_kwargs=None,
                           source_class=AsyncSource, stage_class=AsyncStage,
                           sink_class=AsyncSink):
    pipeline_kwargs = pipeline_kwargs or {}
    pipeline = pipeline_class(**pipeline_kwargs)
    elements = []
    queue = asyncio.Queue(loop=pipeline.loop)
    for idx, func_and_kwargs in enumerate(funcs_and_kwargs):
        if idx == 0:
            elements.append(
                source_class(queue, func_and_kwargs[0], **func_and_kwargs[1]))
        elif idx == len(funcs_and_kwargs) - 1:
            elements.append(
                sink_class(queue, func_and_kwargs[0], **func_and_kwargs[1]))
        else:
            input_queue = queue
            queue = asyncio.Queue(loop=pipeline.loop)
            elements.append(
                stage_class(input_queue, queue, func_and_kwargs[0],
                            **func_and_kwargs[1]))
    pipeline.connect(*elements)
    return AsyncIngester(pipeline)
