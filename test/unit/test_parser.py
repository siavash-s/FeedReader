from rss import parser, exceptions, schema
import pytest


@pytest.fixture
def sample_rss():
    return """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
        <channel>
                <title>Example Feed</title>
                <description>Insert witty or insightful remark here</description>
                <link>http://example.org/</link>
                <item>
                        <title>test</title>
                        <link>http://example.org/2003/12/13/atom03</link>
                        <guid isPermaLink="false">urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a</guid>
                        <description>test</description>
                </item>
        </channel>
</rss>
    """


def test_parse_error():
    p = parser.BasicParser()
    with pytest.raises(exceptions.ParseError):
        p.parse("blah blah")


def test_parse_success(sample_rss):
    p = parser.BasicParser()
    assert p.parse(sample_rss)[0] == schema.Item(
        title="test", link="http://example.org/2003/12/13/atom03",
        guid="urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a",
        description="test"
    )
