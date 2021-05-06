from pydantic import BaseModel
from typing import Optional


class Item(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    link: Optional[str] = None
    author: Optional[str] = None
    guid: Optional[str] = None
