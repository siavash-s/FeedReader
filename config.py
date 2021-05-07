from pydantic import (
    BaseSettings,
    Field,
    PositiveInt
)
from enum import Enum


class Config(BaseSettings):
    class _LogLevel(str, Enum):
        info = "INFO"
        debug = "DEBUG"
        warning = "WARNING"
    log_level: _LogLevel = Field('INFO', env='LOG_LEVEL')

    rabbitmq_url: str = Field(..., env="RABBITMQ_URL")

    work_queue: str = Field(
        "workers", env="WORK_QUEUE",
        description="RabbitMQ work queue name to be shared with other workers"
    )

    task_exchange: str = Field(
        'task_exchange', env="TASK_EXCHANGE",
        description="RabbitMQ exchange name to receive tasks from"
    )

    binding_key: str = Field("task", env="BINDING_KEY", description="RabbitMQ binding key for receiving tasks")

    threads_num: PositiveInt = Field(2, env="THREADS_NUM", description="Number of worker threads")

    read_timeout: PositiveInt = Field(2, env="READ_TIMEOUT", description="RabbitMQ read interval timeout")

    rabbitmq_retry_interval: PositiveInt = Field(
        10, env="RABBITMQ_RETRY_INTERVAL", description="RabbitMQ connection retry interval"
    )

    rabbitmq_connection_retry: PositiveInt = Field(10, env="RABBITMQ_CONNECTION_RETRY")

    routing_key: str = Field(
        "task_result", env="ROUTING_KEY", description="RabbitMQ routing key for task results to be published"
    )

    result_exchange: str = Field(
        'result_exchange', env="RESULT_EXCHANGE",
        description="RabbitMQ exchange name to send results to"
    )


settings = None


def get_conf():
    global settings
    if settings is not None:
        return settings
    else:
        settings = Config()
        return settings
