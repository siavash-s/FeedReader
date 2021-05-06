from abc import ABC, abstractmethod
from logging import getLogger
import threading
from queue import Queue, Empty
from typing import Dict, Optional
import requests
from reader import data
from dataclasses import dataclass


@dataclass
class Response:
    text: str
    status_code: int


class HttpSessionGettable(ABC):

    @abstractmethod
    def get(self, url: str, headers: Optional[Dict] = None) -> Response:
        raise NotImplemented


class HttpSession(HttpSessionGettable):
    def __init__(self):
        self.session = requests.Session()

    def get(self, url: str, headers: Optional[Dict] = None) -> Response:
        resp = self.session.get(url, headers=headers)
        return Response(text=resp.text, status_code=resp.status_code)


class Reader(threading.Thread):

    def __init__(
            self, in_q: Queue, result_q: Queue,
            stop_event: threading.Event, *args,
            session: HttpSessionGettable = HttpSession(),
            **kwargs
    ):
        """

        :param in_q: queue to fetch the tasks from, tasks should be instances of 'InputData'.
        :param result_q: queue to push the results to, results should be instances of 'ResultData'.
        :param stop_event: event to stop the thread.
        """
        self.in_q = in_q
        self.result_q = result_q
        self.stop_event = stop_event
        self.session = session
        super().__init__(*args, **kwargs)

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                in_data: data.InputData = self.in_q.get(timeout=1)
            except Empty:
                continue

            try:
                response = self.session.get(in_data.url)
            except Exception as e:
                getLogger().error(f"Error while fetching http resource {in_data.url}, Exception: {repr(e)}")
                self.result_q.put(
                    data.ResultData(url=in_data.url, data=None, error=repr(e))
                )
                continue
            else:
                if response.status_code == 200:
                    self.result_q.put(data.ResultData(url=in_data.url, data=response.text, error=None))
                    getLogger().info(f"Fetched successful result from {in_data.url}")
                    continue
                elif response.status_code == 304:
                    continue
                else:
                    getLogger().error(
                        f"Http response status code mismatch expected 200 but received {response.status_code}"
                    )
                    self.result_q.put(data.ResultData(
                        url=in_data.url, data=None,
                        error=f"result status code mismatch, status code:{response.status_code}")
                    )
                    continue
