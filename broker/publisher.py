from abc import ABC, abstractmethod
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, ChannelError
import pika
from logging import getLogger
from time import sleep
from broker import exceptions


class Publisher(ABC):
    @abstractmethod
    def publish(self, msg: str):
        """Publishes the msg"""
        raise NotImplemented

    @abstractmethod
    def close(self):
        """Closes all open resources."""
        raise NotImplemented


class RabbitmqPublisher(Publisher):
    def __init__(
            self, rabbitmq_url: str, exchange_name: str,
            routing_key: str, retry: int,
            retry_interval: int
    ):
        self.connection_params = pika.connection.URLParameters(rabbitmq_url)
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.retry = retry
        self.retry_interval = retry_interval
        self.publish_props = pika.BasicProperties(delivery_mode=2)

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
            except (AMQPConnectionError, ChannelError) as e:
                getLogger().error(f"Connection to RabbitMQ failed {e}")
                sleep(self.retry_interval)
            else:
                getLogger().info("Connected to RabbitMQ")
                return channel
        else:
            getLogger().error(f"Giving up connecting to RabbitMQ: {self.connection_params}")
            raise exceptions.ConnectionFailed

    def publish(self, msg: bytes):
        try:
            self.channel.basic_publish(
                self.exchange_name,
                self.routing_key,
                msg,
                self.publish_props
            )
        except (AMQPConnectionError, ChannelError) as e:
            getLogger().error(f"Connection to RabbitMQ failed {e}")
            self.channel = self._connect()

    def close(self):
        try:
            self.channel.close()
        except Exception as e:
            getLogger().exception(e)
