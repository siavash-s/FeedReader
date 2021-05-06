from abc import ABC, abstractmethod
from rss import schema, exceptions
from typing import List
import feedparser
from pydantic import ValidationError
from logging import getLogger


class Parser(ABC):
    @abstractmethod
    def parse(self, data: str) -> List[schema.Item]:
        raise NotImplemented


class BasicParser(Parser):
    """

    :raises ParseError:
    """
    def parse(self, data: str) -> List[schema.Item]:
        results = []
        feed = feedparser.parse(data)
        if feed.bozo:
            raise exceptions.ParseError(repr(feed.bozo_exception))
        else:
            for entry in feed.entries:
                # because feedparser has its own dict implementation 'FeedParserDict'
                try:
                    item = schema.Item.parse_obj(
                        {key: entry[key] for key in schema.Item.__fields__.keys() if key in entry}
                    )
                except ValidationError as e:
                    getLogger().error(f"Item: {entry} in feed is not valid because of {e}")
                else:
                    results.append(item)

        if len(results) == 0:
            raise exceptions.ParseError("No valid Item in feed")
        return results
