from broker import task_reader, publisher
from config import get_conf
from rss import parser
from queue import Queue
from threading import Event, Thread
from reader import http_reader
import loop

# Singleton task_reader
_task_reader = task_reader.RabbitmqTaskReader(
    rabbitmq_url=get_conf().rabbitmq_url,
    prefetch=get_conf().threads_num,
    queue_name=get_conf().work_queue,
    exchange_name=get_conf().task_exchange,
    binding_key=get_conf().binding_key,
    interval_timeout=get_conf().read_timeout,
    retry=get_conf().rabbitmq_connection_retry,
    retry_interval=get_conf().rabbitmq_retry_interval
)


# singleton publisher
_publisher = publisher.RabbitmqPublisher(
    rabbitmq_url=get_conf().rabbitmq_url,
    exchange_name=get_conf().result_exchange,
    routing_key=get_conf().routing_key,
    retry=get_conf().rabbitmq_connection_retry,
    retry_interval=get_conf().rabbitmq_retry_interval
)


# singleton parser
_parser = parser.BasicParser()


def reader_factory(in_q: Queue, result_q: Queue, stop_event: Event) -> Thread:
    return http_reader.Reader(in_q=in_q, result_q=result_q, stop_event=stop_event)


# main_loop singleton
_main_loop = loop.MainLoop(
    threads_num=get_conf().threads_num, worker_factory=reader_factory,
    parser_=_parser, publisher_=_publisher, task_reader_=_task_reader
)


def get_main_loop():
    global _main_loop
    return _main_loop
