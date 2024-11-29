#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest
from unittest.mock import MagicMock
from airbyte_cdk.sources.declarative.resolvers import (
    ComponentMappingDefinition,
    HttpComponentsResolver,
)


@pytest.mark.parametrize(
    "components_mapping, retriever_data, stream_template_config, expected_result",
    [
        (
            [
                ComponentMappingDefinition(
                    field_path=["key1"],
                    value="{{components_values['key1']}}",
                    value_type=str,
                    parameters={},
                ),
            ],
            [{"key1": "updated_value1", "key2": "updated_value2"}],
            {"key1": None, "key2": None},
            [{"key1": "updated_value1", "key2": None}],  # Only key1 is updated
        )
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
