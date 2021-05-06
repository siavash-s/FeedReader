import pytest
from broker import publisher
from config import get_conf
import pika


@pytest.fixture
def basic_publisher():
    return publisher.RabbitmqPublisher(
        get_conf().rabbitmq_url, get_conf().result_exchange,
        get_conf().routing_key, 1, 0
    )


@pytest.fixture
def consumer_channel():
    connection = pika.BlockingConnection(pika.connection.URLParameters(get_conf().rabbitmq_url))
    channel = connection.channel()
    channel.exchange_declare(get_conf().result_exchange, 'direct', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(
        "test_q", durable=True
    )
    channel.queue_bind("test_q", get_conf().result_exchange, get_conf().routing_key)
    yield channel
    channel.queue_delete(queue=get_conf().work_queue)
    channel.close()


def test_publish_data(consumer_channel, basic_publisher: publisher.Publisher):
    basic_publisher.publish("test")
    msg_gen = consumer_channel.consume("test_q", inactivity_timeout=1)
    m, _, body = next(msg_gen)
    assert body == b"test"
    consumer_channel.basic_ack(delivery_tag=m.delivery_tag)
    basic_publisher.close()
