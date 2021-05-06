from pydantic import BaseModel


class Task(BaseModel):
    link: str
