#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest
import requests

from airbyte_cdk.sources.declarative.decoders import CsvDecoder


@pytest.mark.parametrize(
    "response_body, expected, delimiter",
    [
        ("name,age\nJohn,30", [{"name": "John", "age": "30"}], ","),
        ("name;age\nJohn;30", [{"name": "John", "age": "30"}], ";"),
        ("", [{}], ","),  # Empty response
        ("invalid,csv,data\nno,columns", [{}], ","),  # Malformed CSV
        (
            "name,age\nJohn,30\nJane,25",
            [{"name": "John", "age": "30"}, {"name": "Jane", "age": "25"}],
            ",",
        ),  # Multiple rows
    ],
)
def test_csv_decoder(requests_mock, response_body, expected, delimiter):
    requests_mock.register_uri("GET", "https://airbyte.io/", text=response_body)
    response = requests.get("https://airbyte.io/")
    decoder = CsvDecoder(parameters={"delimiter": delimiter})
    assert list(decoder.decode(response)) == expected


def test_is_stream_response():
    decoder = CsvDecoder(parameters={})
    assert decoder.is_stream_response() is True


def test_custom_encoding():
    decoder = CsvDecoder(parameters={"encoding": "latin1"})
    assert decoder.encoding == "latin1"
