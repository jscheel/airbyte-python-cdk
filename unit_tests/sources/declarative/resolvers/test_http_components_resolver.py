#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import json
from unittest import TestCase

import pytest
from unittest.mock import MagicMock

from airbyte_cdk.test.entrypoint_wrapper import check, discover
from airbyte_cdk.sources.declarative.concurrent_declarative_source import ConcurrentDeclarativeSource
from airbyte_cdk.sources.declarative.resolvers import (
    ComponentMappingDefinition,
    HttpComponentsResolver,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse


@pytest.mark.parametrize(
    "components_mapping, retriever_data, stream_template_config, expected_result",
    [
        (
            [
                ComponentMappingDefinition(
                    key="key1",
                    value="{{components_values['key1']}}",
                    value_type=str,
                    condition="True",
                    parameters={},
                ),
                ComponentMappingDefinition(
                    key="key2",
                    value="{{components_values['key2']}}",
                    value_type=str,
                    condition="False",
                    parameters={},
                ),
            ],
            [{"key1": "updated_value1", "key2": "updated_value2"}],
            {"key1": None, "key2": None},
            [{"key1": "updated_value1", "key2": None}],  # Only key1 is updated
        ),
        (
            [
                ComponentMappingDefinition(
                    key="key3",
                    value="{{components_values['key3']}}",
                    value_type=str,
                    condition="True",
                    parameters={},
                ),
            ],
            [{"key3": "updated_value3"}],
            {"key3": None},
            [{"key3": "updated_value3"}],  # key3 is updated
        ),
    ],
)
def test_http_components_resolver(
    components_mapping, retriever_data, stream_template_config, expected_result
):
    # Mock the retriever to simulate reading records
    mock_retriever = MagicMock()
    mock_retriever.read_records.return_value = retriever_data

    # Use a simple dictionary for the config, as Config should be a Mapping
    config = {}

    # Instantiate the resolver with mocked data
    resolver = HttpComponentsResolver(
        retriever=mock_retriever,
        config=config,
        components_mapping=components_mapping,
        parameters={},
    )

    # Run the resolve_components method and convert the result to a list
    result = list(resolver.resolve_components(stream_template_config=stream_template_config))

    # Assert the resolved components match the expected result
    assert result == expected_result


_MANIFEST = {
  "version": "6.6.7",
  "streams": [],
  "type": "DeclarativeSource",
  "check": {
    "type": "CheckStream",
    "stream_names": [
      "table1"
    ]
  },
  "definitions": {
    "base_requester": {
      "type": "HttpRequester",
      "url_base": "https://api.test.com/v0",
    },
    "streams": {
      "tables": {
        "type": "DeclarativeStream",
        "name": "tables",
        "$parameters": {
          "table_id": "table_id"
        },
        "retriever": {
          "requester": {
            "$ref": "#/definitions/base_requester",
            "path": "tables/{{parameters['table_id']}}",
            "http_method": "GET"
          },
          "record_selector": {
            "extractor": {
              "type": "DpathExtractor",
              "field_path": []
            }
          }
        },
        "schema_loader": {
          "type": "InlineSchemaLoader",
          "schema": {
            "$schema": "http://json-schema.org/schema#",
            "properties": {"col1": {"type": "string"}, "col2": {"type": "string"}},
            "type": "object",
          },
        },
      }
    }
  },
  "dynamic_streams": [
    {
      "type": "DynamicDeclarativeStream",
      "stream_template": {
        "$ref": "#/definitions/streams/tables"
      },
      "components_resolver": {
        "type": "HttpComponentsResolver",
        "retriever": {
          "type": "SimpleRetriever",
          "requester": {
            "$ref": "#/definitions/base_requester",
            "path": "tables",
            "http_method": "GET"
          },
          "record_selector": {
            "extractor": {
              "type": "DpathExtractor",
              "field_path": [
                "tables"
              ]
            }
          }
        },
        "components_mapping": [
          {
            "type": "ComponentMappingDefinition",
            "key": "name",
            "value": "{{ components_values['name'] }}",
          },
          {
            "type": "ComponentMappingDefinition",
            "key": "table_id",
            "value": "{{ components_values['id'] }}"
          }
        ]
      }
    }
  ],
  "spec": {
    "connection_specification": {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": [],
        "properties": {},
        "additionalProperties": True,
    },
    "type": "Spec",
},
}


class DynamicStreamsIntegrationTest(TestCase):

    def setUp(self) -> None:
        self._http_mocker = HttpMocker()
        self._http_mocker.__enter__()

        self._config = {
            "a_key": "config can't be empty else we don't instantiate the streams",
        }

    def tearDown(self) -> None:
        self._http_mocker.__exit__(None, None, None)

    def test_discover(self) -> None:
        self._http_mocker.get(
            HttpRequest("https://api.test.com/v0/tables"),
            HttpResponse(json.dumps({
                "tables": [
                    {"name": "table1", "id": "id_table1"},
                    {"name": "table2", "id": "id_table2"},
                ]
            })),
        )
        source = ConcurrentDeclarativeSource(source_config=_MANIFEST, config=self._config, catalog=None, state=None)

        output = discover(source, self._config)

        assert len(output.catalog.catalog.streams) == 2

    def test_check(self) -> None:
        self._http_mocker.get(
            HttpRequest("https://api.test.com/v0/tables"),
            HttpResponse(json.dumps({
                "tables": [
                    {"name": "table1", "id": "id_table1"},
                ]
            })),
        )
        self._http_mocker.get(
            HttpRequest("https://api.test.com/v0/tables/id_table1"),
            HttpResponse(json.dumps([
                {"col1": "value1", "col2": "value2"},
            ])),
        )
        source = ConcurrentDeclarativeSource(source_config=_MANIFEST, config=self._config, catalog=None, state=None)

        output = check(source, self._config)

        # TODO this test is failing right now because I'm not so sure how to map a component to $parameter.table_id in order to interpolate in the path
        assert False
