#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest
from unittest.mock import MagicMock
from airbyte_cdk.sources.declarative.resolvers import (
    ComponentMappingDefinition,
    HttpComponentsResolver,
)
from airbyte_cdk.sources.embedded.catalog import (
    to_configured_catalog,
    to_configured_stream,
)
from airbyte_cdk.models import Type
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString

_MANIFEST = {
    "version": "5.0.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["Rates"]},
    "dynamic_streams": [
        {
            "type": "DynamicDeclarativeStream",
            "stream_template": {
                "type": "DeclarativeStream",
                "name": "",
                "primary_key": [],
                "schema_loader": {
                    "type": "InlineSchemaLoader",
                    "schema": {
                        "$schema": "http://json-schema.org/schema#",
                        "properties": {
                            "ABC": {"type": "number"},
                            "AED": {"type": "number"},
                        },
                        "type": "object",
                    },
                },
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "$parameters": {"item_id": ""},
                        "url_base": "https://api.test.com",
                        "path": "/items/{{parameters['item_id']}}",
                        "http_method": "GET",
                        "authenticator": {
                            "type": "ApiKeyAuthenticator",
                            "header": "apikey",
                            "api_token": "{{ config['api_key'] }}",
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "paginator": {"type": "NoPagination"},
                },
            },
            "components_resolver": {
                "type": "HttpComponentsResolver",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "type": "HttpRequester",
                        "url_base": "https://api.test.com",
                        "path": "items",
                        "http_method": "GET",
                        "authenticator": {
                            "type": "ApiKeyAuthenticator",
                            "header": "apikey",
                            "api_token": "{{ config['api_key'] }}",
                        },
                    },
                    "record_selector": {
                        "type": "RecordSelector",
                        "extractor": {"type": "DpathExtractor", "field_path": []},
                    },
                    "paginator": {"type": "NoPagination"},
                },
                "components_mapping": [
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": ["name"],
                        "value": "{{components_values['name']}}",
                    },
                    {
                        "type": "ComponentMappingDefinition",
                        "field_path": [
                            "retriever",
                            "requester",
                            "$parameters",
                            "item_id",
                        ],
                        "value": "{{components_values['id']}}",
                    },
                ],
            },
        }
    ],
}


@pytest.mark.parametrize(
    "components_mapping, retriever_data, stream_template_config, expected_result",
    [
        (
            [
                ComponentMappingDefinition(
                    field_path=[InterpolatedString.create("key1", parameters={})],
                    value="{{components_values['key1']}}",
                    value_type=str,
                    parameters={},
                )
            ],
            [{"key1": "updated_value1", "key2": "updated_value2"}],
            {"key1": None, "key2": None},
            [{"key1": "updated_value1", "key2": None}],
        )
    ],
)
def test_http_components_resolver(
    components_mapping, retriever_data, stream_template_config, expected_result
):
    mock_retriever = MagicMock()
    mock_retriever.read_records.return_value = retriever_data
    config = {}

    resolver = HttpComponentsResolver(
        retriever=mock_retriever,
        config=config,
        components_mapping=components_mapping,
        parameters={},
    )

    result = list(resolver.resolve_components(stream_template_config=stream_template_config))
    assert result == expected_result


def test_dynamic_streams_read(requests_mock):
    expected_stream_names = ["item_1", "item_2"]
    requests_mock.get(
        "https://api.test.com/items",
        json=[{"id": 1, "name": "item_1"}, {"id": 2, "name": "item_2"}],
    )
    requests_mock.get("https://api.test.com/items/1", json={"id": "1", "name": "item_1"})
    requests_mock.get("https://api.test.com/items/2", json={"id": "2", "name": "item_2"})

    source = ManifestDeclarativeSource(source_config=_MANIFEST)
    actual_catalog = source.discover(logger=source.logger, config={})

    configured_streams = [
        to_configured_stream(stream, primary_key=stream.source_defined_primary_key)
        for stream in actual_catalog.streams
    ]
    configured_catalog = to_configured_catalog(configured_streams)
    records = [
        message.record
        for message in source.read(MagicMock(), {}, configured_catalog)
        if message.type == Type.RECORD
    ]

    assert len(actual_catalog.streams) == 2
    assert [stream.name for stream in actual_catalog.streams] == expected_stream_names
    assert len(records) == 2
    assert [record.stream for record in records] == expected_stream_names
