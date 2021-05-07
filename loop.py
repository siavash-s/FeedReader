from queue import Queue, Empty
from config import get_conf
from rss import parser, schema
from broker import task_reader, publisher
from broker.schema import Task
from logging import getLogger
from typing import Optional, List, Callable
from functools import partial
from threading import Event
from reader.data import InputData, ResultData
from pydantic import BaseModel


class MainLoop:
    def __init__(
            self, threads_num: int,
            worker_factory: Callable,
            parser_: parser.Parser,
            publisher_: publisher.Publisher,
            task_reader_: task_reader.TaskReader,
    ):
        self.threads_num = threads_num
        self.task_id_mapping = dict()
        self.parser = parser_
        self.publisher = publisher_
        self.task_reader = task_reader_
        self.task_reader_generator = task_reader_.get_task()
        self.in_q = Queue()
        self.result_q = Queue()
        self.stop_event = Event()
        self.worker_factory = partial(
            worker_factory, in_q=self.in_q, result_q=self.result_q, stop_event=self.stop_event
        )
        self.workers = []
        for _ in range(get_conf().threads_num):
            worker = self.worker_factory()
            worker.start()
            self.workers.append(worker)

    def loop(self):
        getLogger().info("Started the main loop")
        while True:
            self._worker_threads_check()
            self._iterate()

    def _iterate(self):
        if len(self.task_id_mapping) < self.threads_num:
            # block a while for a new task
            new_task: Optional[Task]
            new_task, task_id = next(self.task_reader_generator)
            # the generator may return None after timeout
            if new_task is not None:
                if new_task.link not in self.task_id_mapping:
                    self.task_id_mapping[new_task.link] = task_id
                    self.in_q.put(InputData(new_task.link))
                else:
                    self.task_reader.reject(task_id)
                    getLogger().info(f"Rejected duplicate task: {new_task.link}")

        # get all completed tasks
        while True:
            try:
                task_result: ResultData = self.result_q.get_nowait()
            except Empty:
                break
            else:
                try:
                    items = self.parser.parse(task_result.data)
                except parser.exceptions.ParseError as e:
                    error = repr(e)
                    publish_data = PublishData(link=task_result.url, items=None, error=error)
                else:
                    publish_data = PublishData(link=task_result.url, items=items, error=None)
                finally:
                    self.publisher.publish(publish_data.json())
                    self.task_reader.ack(self.task_id_mapping[task_result.url])
                    del self.task_id_mapping[task_result.url]

    def _worker_threads_check(self):
        for i, worker in enumerate(self.workers):
            if not worker.is_alive():
                getLogger().error("Found dead thread, rising another one")
                self.workers[i] = self.worker_factory()
                self.workers[i].start()

    def exit(self):
        self.stop_event.set()
        for worker in self.workers:
            worker.join(3)

        self.publisher.close()

        for task_id in self.task_id_mapping.values():
            self.task_reader.reject(task_id)
        self.task_reader.close()


class PublishData(BaseModel):
    link: str
    items: Optional[List[schema.Item]] = None
    error: Optional[str] = None
