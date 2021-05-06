from typing import Dict, Optional
from queue import Queue
import threading
from reader import http_reader, data


def test_reader_exception():

    class MockSessionRaiseGet(http_reader.HttpSessionGettable):
        def get(self, url: str, headers: Optional[Dict] = None):
            raise Exception("test exception")

    event = threading.Event()
    in_q = Queue()
    result_q = Queue()
    r = http_reader.Reader(in_q, result_q, event, session=MockSessionRaiseGet())
    r.start()
    in_q.put(data.InputData(url="test"))
    assert result_q.get() == data.ResultData(url="test", error=repr(Exception("test exception")))
    event.set()
    r.join(3)


def test_reader_success():

    class MockSessionRaiseGet(http_reader.HttpSessionGettable):
        def get(self, url: str, headers: Optional[Dict] = None):
            return http_reader.Response(text="test", status_code=200)

    event = threading.Event()
    in_q = Queue()
    result_q = Queue()
    r = http_reader.Reader(in_q, result_q, event, session=MockSessionRaiseGet())
    r.start()
    in_q.put(data.InputData(url="test"))
    assert result_q.get(timeout=3) == data.ResultData(url="test", data="test")
    event.set()
    r.join(3)


def test_reader_status_mismatch():

    class MockSessionRaiseGet(http_reader.HttpSessionGettable):
        def get(self, url: str, headers: Optional[Dict] = None):
            return http_reader.Response(text="", status_code=500)

    event = threading.Event()
    in_q = Queue()
    result_q = Queue()
    r = http_reader.Reader(in_q, result_q, event, session=MockSessionRaiseGet())
    r.start()
    in_q.put(data.InputData(url="test"))
    result = result_q.get(timeout=3)
    assert result.error is not None and result.data is None
    event.set()
    r.join(3)
