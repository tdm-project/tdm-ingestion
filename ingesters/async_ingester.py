import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Type, Dict

from tdm_ingestion.ingestion import Ingester, Consumer, Message, \
    MessageConverter, TimeSeries, Storage


class Pipeline(ABC):
    def __init__(self, *args, **kwargs):
        self.funcs = []

    def connect(self, *funcs):
        self.funcs = funcs

    @abstractmethod
    def run(self, *args, **kwargs):
        pass


class AsyncPipeline(Pipeline):

    async def _run(self, *args, **kwargs):
        logging.debug('run')
        data_out = await self.funcs[0](*args, **kwargs)
        for func in self.funcs[1:]:
            data_out = await func(data_out)
            logging.debug(f'data_out {data_out}')
        return data_out

    def run(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self._run(*args, **kwargs))
        try:
            loop.run_forever()
        finally:
            loop.close()


class AsyncElement(ABC):
    @abstractmethod
    async def process(self, *args, **kwargs):
        pass


class AsyncConsumer(AsyncElement):
    def __init__(self, consumer: Consumer):
        self.consumer = consumer

    async def process(self, timeout_s: int = -1, max_records: int = -1) -> \
            List[
                Message]:
        logging.debug('consume')
        return self.consumer.poll(timeout_s, max_records)


class AsyncConverter(AsyncElement):
    def __init__(self, converter: MessageConverter):
        self.converter = converter

    async def process(self, messages: List[Message]) -> List[TimeSeries]:
        return self.converter.convert(messages)


class AsyncStorage(AsyncElement):
    def __init__(self, storage: Storage):
        self.storage = storage

    async def process(self, messages: List[TimeSeries]):
        return self.storage.write(messages)


class AsyncIngester(Ingester):
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def process(self, timeout_s: int = -1, max_records: int = 1):
        self.pipeline.run(timeout_s, max_records)


def async_ingester_factory(pipeline_class: Type[Pipeline],
                           element_classes: List[
                               Type], pipeline_kwargs: Dict,
                           element_kwargs: List[Dict]) -> AsyncIngester:
    def get_async_element_class(el_class: Type) -> Type[AsyncElement]:
        superclasses = set(el_class.mro())
        if Storage in superclasses:
            return AsyncStorage
        elif MessageConverter in superclasses:
            return AsyncConverter
        elif Consumer in superclasses:
            return AsyncConsumer

    funcs = []
    element_kwargs = element_kwargs or [{}] * len(element_classes)

    for idx, element_class in enumerate(element_classes):
        el = element_class(**element_kwargs[idx])
        funcs.append(get_async_element_class(element_class)(el).process)

    pipeline = pipeline_class(**pipeline_kwargs)
    pipeline.connect(*funcs)
    return AsyncIngester(pipeline)


if __name__ == '__main__':
    from tests.dummies import DummyConsumer
    logging.basicConfig(level=logging.DEBUG)

    ingester = async_ingester_factory(AsyncPipeline, [DummyConsumer], {}, [])
    ingester.process()
