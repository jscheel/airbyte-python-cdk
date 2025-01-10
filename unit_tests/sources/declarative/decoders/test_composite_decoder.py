#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import csv
import gzip
import json
from io import BufferedReader, BytesIO, StringIO

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import (
    CompositeRawDecoder,
    CsvParser,
    GzipParser,
    JsonLineParser,
    JsonParser,
)


def compress_with_gzip(data: str, encoding: str = "utf-8"):
    """
    Compress the data using Gzip.
    """
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(data.encode(encoding))
    return buf.getvalue()


def generate_csv(encoding: str) -> bytes:
    """
    Generate CSV data with tab-separated values (\t).
    """
    data = [
        {"id": 1, "name": "John", "age": 28},
        {"id": 2, "name": "Alice", "age": 34},
        {"id": 3, "name": "Bob", "age": 25},
    ]

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=["id", "name", "age"], delimiter="\t")
    writer.writeheader()
    for row in data:
        writer.writerow(row)

    # Ensure the pointer is at the beginning of the buffer before compressing
    output.seek(0)

    # Compress the CSV data with Gzip
    compressed_data = compress_with_gzip(output.read(), encoding=encoding)

    return compressed_data


@pytest.mark.parametrize("encoding", ["utf-8", "utf", "iso-8859-1"])
def test_composite_raw_decoder_gzip_csv_parser(requests_mock, encoding: str):
    requests_mock.register_uri(
        "GET", "https://airbyte.io/", content=generate_csv(encoding=encoding)
    )
    response = requests.get("https://airbyte.io/", stream=True)

    parser = GzipParser(inner_parser=CsvParser(encoding=encoding, delimiter="\t"))
    composite_raw_decoder = CompositeRawDecoder(parser=parser)
    counter = 0
    for _ in composite_raw_decoder.decode(response):
        counter += 1
    assert counter == 3


def generate_jsonlines():
    """
    Generator function to yield data in JSON Lines format.
    This is useful for streaming large datasets.
    """
    data = [
        {"id": 1, "message": "Hello, World!"},
        {"id": 2, "message": "Welcome to JSON Lines"},
        {"id": 3, "message": "Streaming data is fun!"},
    ]
    for item in data:
        yield json.dumps(item) + "\n"  # Serialize as JSON Lines


def generate_compressed_jsonlines(encoding: str = "utf-8") -> bytes:
    """
    Generator to compress the entire response content with Gzip and encode it.
    """
    json_lines_content = "".join(generate_jsonlines())
    compressed_data = compress_with_gzip(json_lines_content, encoding=encoding)
    return compressed_data


@pytest.mark.parametrize("encoding", ["utf-8", "utf", "iso-8859-1"])
def test_composite_raw_decoder_gzip_jsonline_parser(requests_mock, encoding: str):
    requests_mock.register_uri(
        "GET", "https://airbyte.io/", content=generate_compressed_jsonlines(encoding=encoding)
    )
    response = requests.get("https://airbyte.io/", stream=True)

    parser = GzipParser(inner_parser=JsonLineParser(encoding=encoding))
    composite_raw_decoder = CompositeRawDecoder(parser=parser)
    counter = 0
    for _ in composite_raw_decoder.decode(response):
        counter += 1
    assert counter == 3


@pytest.mark.parametrize("encoding", ["utf-8", "utf", "iso-8859-1"])
def test_composite_raw_decoder_jsonline_parser(requests_mock, encoding: str):
    response_content = "".join(generate_jsonlines())
    requests_mock.register_uri(
        "GET", "https://airbyte.io/", content=response_content.encode(encoding=encoding)
    )
    response = requests.get("https://airbyte.io/", stream=True)
    composite_raw_decoder = CompositeRawDecoder(parser=JsonLineParser(encoding=encoding))
    counter = 0
    for _ in composite_raw_decoder.decode(response):
        counter += 1
    assert counter == 3


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"data-type": "string"}, {"data-type": "string"}),
        ([{"id": 1}, {"id": 2}], [{"id": 1}, {"id": 2}]),
    ],
    ids=[
        "test_with_buffered_io_base_data_containing_string",
        "test_with_buffered_io_base_data_containing_list",
    ],
)
def test_json_parser_with_valid_data(data, expected):
    encodings = ["utf-8", "utf", "iso-8859-1"]

    for encoding in encodings:
        raw_data = json.dumps(data).encode(encoding)
        for i, actual in enumerate(
            JsonParser(encoding=encoding).parse(BufferedReader(BytesIO(raw_data)))
        ):
            if isinstance(expected, list):
                assert actual == expected[i]
            else:
                assert actual == expected
