from abc import ABC, abstractmethod
from typing import Generator, Optional, Tuple
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, ChannelError
from broker import schema
import pika
from json import JSONDecodeError
from logging import getLogger
from pydantic import ValidationError
from time import sleep
from broker import exceptions


class TaskReader(ABC):
    @abstractmethod
    def get_task(self) -> Generator[Tuple[Optional[schema.Task], Optional[int]], None, None]:
        """blocking task getter.

        returns None in case of read timeout.
        """
        raise NotImplemented

    @abstractmethod
    def close(self):
        """Closes all open resources."""
        raise NotImplemented

    @abstractmethod
    def ack(self, task_id):
        """Acknowledge the completion of the task_id"""
        raise NotImplementedError

    @abstractmethod
    def reject(self, task_id):
        """Reject the task_id"""
        raise NotImplemented


class RabbitmqTaskReader(TaskReader):

    def __init__(
            self, rabbitmq_url: str, prefetch: int,
            queue_name: str, exchange_name: str,
            binding_key: str, interval_timeout: int,
            retry: int, retry_interval: int
    ):
        self.connection_params = pika.connection.URLParameters(rabbitmq_url)
        self.prefetch = prefetch
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.binding_key = binding_key
        self.interval_timeout = interval_timeout
        self.retry = retry
        self.retry_interval = retry_interval
        self.last_delivery_tag = None

        self.channel = self._connect()

    def _connect(self) -> BlockingChannel:
        """returns a blocking channel

        :raises ConnectionFailed: if connection max retries fail
        """
        for _ in range(self.retry):
            getLogger().info("Trying to connect to RabbitMQ ...")
            try:
                connection = pika.BlockingConnection(self.connection_params)
                channel = connection.channel()
                channel.exchange_declare(self.exchange_name, 'direct', durable=True)
                channel.basic_qos(prefetch_count=self.prefetch)
                channel.queue_declare(
                    self.queue_name, durable=True
                )
                channel.queue_bind(self.queue_name, self.exchange_name, self.binding_key)
            except (AMQPConnectionError, ChannelError) as e:
                getLogger().error(f"Connection to RabbitMQ failed {e}")
                sleep(self.retry_interval)
            else:
                getLogger().info("Connected to RabbitMQ")
                return channel
        else:
            getLogger().error(f"Giving up connecting to RabbitMQ: {self.connection_params}")
            raise exceptions.ConnectionFailed

    def get_task(self) -> [Tuple[Optional[schema.Task], Optional[int]], None, None]:
        while True:
            try:
                for method_frame, properties, body in self.channel.consume(
                        self.queue_name, inactivity_timeout=self.interval_timeout
                ):
                    if body is None:
                        # in case of inactivity timeout
                        yield None, None
                        continue
                    try:
                        result = schema.Task.parse_raw(body)
                    except (JSONDecodeError, ValidationError, TypeError) as e:
                        getLogger().error(f"While getting tasks, received bad format data: {body}")
                        getLogger().exception(e)
                        self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                        continue
                    else:
                        yield result, method_frame.delivery_tag
            except (AMQPConnectionError, ChannelError) as e:
                getLogger().error(f"Connection to RabbitMQ failed {e}")
                self.channel = self._connect()
                continue

    def close(self):
        try:
            self.channel.cancel()
            self.channel.close()
        except Exception as e:
            getLogger().exception(e)

    def ack(self, task_id):
        try:
            self.channel.basic_ack(delivery_tag=task_id)
        except (AMQPConnectionError, ChannelError) as e:
            getLogger().error(f"Connection to RabbitMQ failed {e}")
            self.channel = self._connect()
            self.channel.basic_ack(delivery_tag=task_id)

    def reject(self, task_id):
        try:
            self.channel.basic_reject(delivery_tag=task_id)
        except (AMQPConnectionError, ChannelError) as e:
            getLogger().error(f"Connection to RabbitMQ failed {e}")
            self.channel = self._connect()
            self.channel.basic_reject(delivery_tag=task_id)
