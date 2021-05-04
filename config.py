from pydantic import (
    BaseSettings,
    Field, PostgresDsn,
    PositiveInt,
    RedisDsn,
)
from enum import Enum


class Config(BaseSettings):
    class _LogLevel(str, Enum):
        info = "INFO"
        debug = "DEBUG"
        warning = "WARNING"
    log_level: _LogLevel = Field('INFO', env='LOG_LEVEL')


settings = None


def get_conf():
    global settings
    if settings is not None:
        return settings
    else:
        settings = Config()
        return settings
