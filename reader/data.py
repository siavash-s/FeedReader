from typing import Optional
from dataclasses import dataclass


@dataclass
class ResultData:
    url: str
    data: Optional[str] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.data is None and self.error is None:
            raise ValueError("'data' and 'error' attributes must be mutually exclusive")


@dataclass
class InputData:
    url: str
