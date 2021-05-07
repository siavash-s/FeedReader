from typing import Generator, Tuple, Optional, List
from broker import task_reader, publisher, schema
from rss import parser
from threading import Event
import loop
from queue import Queue, Empty
import pytest
from reader import data


class MockTaskReader(task_reader.TaskReader):

    def __init__(self, tasks):
        self.tasks = tasks
        self.acked = []
        self.rejected = []

    def get_task(self) -> Generator[Tuple[Optional[schema.Task], Optional[int]], None, None]:
        for i, task in enumerate(self.tasks):
            yield task, i
        i = len(self.tasks)
        while True:
            yield None, i
            i += 1

    def close(self):
        pass

    def ack(self, task_id):
        self.acked.append(task_id)

    def reject(self, task_id):
        self.rejected.append(task_id)


class MockPublisher(publisher.Publisher):
    def __init__(self):
        self.published = []

    def publish(self, msg: str):
        self.published.append(msg)

    def close(self):
        pass


class MockParser(parser.Parser):
    def __init__(self, exception=None):
        self.exception = exception

    def parse(self, data: str) -> List[parser.schema.Item]:
        if self.exception:
            raise self.exception
        else:
            return [parser.schema.Item(title="test")]


class MockThread:
    def __init__(self, in_q: Queue, result_q: Queue, stop_event: Event):
        self.alive = False

    def start(self):
        self.alive = True

    def is_alive(self):
        return self.alive


def test_main_loop_iterate_success():
    mock_parser = MockParser()
    mock_publisher = MockPublisher()
    tasks = [schema.Task(link=f"task{i}") for i in range(3)]
    mock_task_reader = MockTaskReader(tasks)
    main_loop = loop.MainLoop(
        2, MockThread, parser_=mock_parser, publisher_=mock_publisher, task_reader_=mock_task_reader
    )
    main_loop._iterate()
    main_loop._iterate()
    main_loop._iterate()
    # we should have 2 tasks in in_q as the number of threads are 2
    task1 = main_loop.in_q.get_nowait()
    task2 = main_loop.in_q.get_nowait()
    with pytest.raises(Empty):
        main_loop.in_q.get_nowait()
    # push the results back
    result1 = data.ResultData(url=task1.url, data="test", error=None)
    result2 = data.ResultData(url=task2.url, data=None, error="error")
    main_loop.result_q.put(result2)
    main_loop.result_q.put(result1)
    main_loop._iterate()
    assert len(mock_publisher.published) == 2
    assert len(mock_task_reader.acked) == 2
    assert len(mock_task_reader.rejected) == 0


def test_main_loop_iterate_reject_duplicate():
    mock_parser = MockParser()
    mock_publisher = MockPublisher()
    tasks = [schema.Task(link=f"task") for _ in range(2)]
    mock_task_reader = MockTaskReader(tasks)
    main_loop = loop.MainLoop(
        2, MockThread, parser_=mock_parser, publisher_=mock_publisher, task_reader_=mock_task_reader
    )
    main_loop._iterate()
    main_loop._iterate()
    assert len(mock_task_reader.rejected) == 1


def test_main_loop_dead_thread():
    mock_parser = MockParser()
    mock_publisher = MockPublisher()
    tasks = [schema.Task(link=f"task") for _ in range(2)]
    mock_task_reader = MockTaskReader(tasks)
    main_loop = loop.MainLoop(
        2, MockThread, parser_=mock_parser, publisher_=mock_publisher, task_reader_=mock_task_reader
    )
    for worker in main_loop.workers:
        assert worker.is_alive()
    main_loop.workers[0].alive = False
    main_loop._worker_threads_check()
    for worker in main_loop.workers:
        assert worker.is_alive()
