import pytest
from config import get_conf
from broker import task_reader
import pika


@pytest.fixture
def basic_task_reader():
    return task_reader.RabbitmqTaskReader(
        get_conf().rabbitmq_url,
        1,
        get_conf().work_queue,
        get_conf().task_exchange,
        get_conf().binding_key,
        1,
        1,
        0
    )


@pytest.fixture
def publisher_channel(basic_task_reader):
    connection = pika.BlockingConnection(pika.connection.URLParameters(get_conf().rabbitmq_url))
    channel = connection.channel()
    channel.exchange_declare(get_conf().task_exchange, 'direct', durable=True)
    yield channel
    channel.queue_delete(queue=get_conf().work_queue)
    channel.close()


def test_read_task(basic_task_reader: task_reader.TaskReader, publisher_channel):
    data = '{"link": "test"}'
    task_gen = basic_task_reader.get_task()
    # the first time it yields None because timeout reached and there is no task in the queue
    assert next(task_gen) is None
    # now the queue binding is in place and we can publish
    publisher_channel.basic_publish(
                get_conf().task_exchange,
                get_conf().binding_key,
                data,
                pika.BasicProperties(delivery_mode=2)
            )
    # wait for the task to reach the consumer
    task = None
    for _ in range(10):
        task = next(task_gen)
        if task:
            break
    assert task.link == "test"
    basic_task_reader.close(reject=False)
