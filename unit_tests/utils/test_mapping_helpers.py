#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest

from airbyte_cdk.utils.mapping_helpers import combine_mappings


@pytest.mark.parametrize(
    "test_name, mappings, expected_result, expected_error",
    [
        ("basic_merge", [{"a": 1}, {"b": 2}, {"c": 3}, {}], {"a": 1, "b": 2, "c": 3}, None),
        ("handle_none_values", [{"a": 1}, None, {"b": 2}], {"a": 1, "b": 2}, None),
        ("empty_mappings", [], {}, None),
        ("single_mapping", [{"a": 1}], {"a": 1}, None),
        ("overlapping_keys", [{"a": 1, "b": 2}, {"b": 3}], None, "Duplicate keys"),
    ],
)
def test_basic_mapping_operations(test_name, mappings, expected_result, expected_error):
    if expected_error:
        with pytest.raises(ValueError, match=expected_error):
            combine_mappings(mappings)
    else:
        assert combine_mappings(mappings) == expected_result


@pytest.mark.parametrize(
    "test_name, mappings, expected_result, expected_error",
    [
        (
            "combine_with_string",
            [{"a": 1}, "option"],
            None,
            "Cannot combine multiple options if one is a string",
        ),
        (
            "multiple_strings",
            ["option1", "option2"],
            None,
            "Cannot combine multiple string options",
        ),
        ("string_with_empty_mapping", ["option", {}], "option", None),
    ],
)
def test_string_handling(test_name, mappings, expected_result, expected_error):
    if expected_error:
        with pytest.raises(ValueError, match=expected_error):
            combine_mappings(mappings)
    else:
        assert combine_mappings(mappings) == expected_result


@pytest.mark.parametrize(
    "test_name, mappings, expected_result, expected_error",
    [
        (
            "simple_nested_merge",
            [{"a": {"b": 1}}, {"c": {"d": 2}}],
            {"a": {"b": 1}, "c": {"d": 2}},
            None,
        ),
        (
            "deep_nested_merge",
            [{"a": {"b": {"c": 1}}}, {"d": {"e": {"f": 2}}}],
            {"a": {"b": {"c": 1}}, "d": {"e": {"f": 2}}},
            None,
        ),
        (
            "nested_merge_same_level",
            [
                {"data": {"user": {"id": 1}, "status": "active"}},
                {"data": {"user": {"name": "test"}, "type": "admin"}},
            ],
            {"data": {"user": {"id": 1, "name": "test"}, "status": "active", "type": "admin"}},
            None,
        ),
        ("nested_conflict", [{"a": {"b": 1}}, {"a": {"b": 2}}], None, "nested path conflict"),
        ("type_conflict", [{"a": 1}, {"a": {"b": 2}}], None, "nested path conflict"),
        (
            "merge_empty_nested",
            [{"a": {"b": {}}}, {"a": {"b": {"c": 1}}}],
            {"a": {"b": {"c": 1}}},
            None,
        ),
        (
            "different_value_types",
            [{"data": {"field": "string"}}, {"data": {"field": {"nested": "value"}}}],
            None,
            "nested path conflict",
        ),
    ],
)
def test_nested_structures(test_name, mappings, expected_result, expected_error):
    if expected_error:
        with pytest.raises(ValueError, match=expected_error):
            combine_mappings(mappings)
    else:
        assert combine_mappings(mappings) == expected_result
