#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest
from typing import Any, Dict, List, Optional

from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)


@pytest.mark.parametrize(
    "option_type, field_name, expected_field_name",
    [
        (RequestOptionType.request_parameter, "field", "field"),
        (RequestOptionType.header, "field", "field"),
        (RequestOptionType.body_data, "field", "field"),
        (RequestOptionType.body_json, "field", "field"),
        (
            RequestOptionType.request_parameter,
            "since_{{ parameters['cursor_field'] }}",
            "since_updated_at",
        ),
        (RequestOptionType.header, "since_{{ parameters['cursor_field'] }}", "since_updated_at"),
        (RequestOptionType.body_data, "since_{{ parameters['cursor_field'] }}", "since_updated_at"),
        (RequestOptionType.body_json, "since_{{ parameters['cursor_field'] }}", "since_updated_at"),
        (
            RequestOptionType.request_parameter,
            "since_{{ config['cursor_field'] }}",
            "since_created_at",
        ),
        (RequestOptionType.header, "since_{{ config['cursor_field'] }}", "since_created_at"),
        (RequestOptionType.body_data, "since_{{ config['cursor_field'] }}", "since_created_at"),
        (RequestOptionType.body_json, "since_{{ config['cursor_field'] }}", "since_created_at"),
    ],
    ids=[
        "test_limit_param_with_field_name",
        "test_limit_header_with_field_name",
        "test_limit_data_with_field_name",
        "test_limit_json_with_field_name",
        "test_limit_param_with_parameters_interpolation",
        "test_limit_header_with_parameters_interpolation",
        "test_limit_data_with_parameters_interpolation",
        "test_limit_json_with_parameters_interpolation",
        "test_limit_param_with_config_interpolation",
        "test_limit_header_with_config_interpolation",
        "test_limit_data_with_config_interpolation",
        "test_limit_json_with_config_interpolation",
    ],
)
def test_request_option(option_type: RequestOptionType, field_name: str, expected_field_name: str):
    request_option = RequestOption(
        inject_into=option_type, field_name=field_name, parameters={"cursor_field": "updated_at"}
    )
    assert request_option.field_name.eval({"cursor_field": "created_at"}) == expected_field_name
    assert request_option.inject_into == option_type


def test_request_option_validation():
    """Test that RequestOption properly validates its inputs"""
    # Should raise when neither field_name nor field_path is provided
    with pytest.raises(ValueError, match="RequestOption requires either a field_name or field_path"):
        RequestOption(inject_into=RequestOptionType.body_json, parameters={})

    # Should raise when both field_name and field_path are provided
    with pytest.raises(ValueError, match="Only one of field_name or field_path can be provided"):
        RequestOption(
            field_name="field",
            field_path=["data", "field"],
            inject_into=RequestOptionType.body_json,
            parameters={}
        )

    # Should raise when field_path is used with non-body-json request type
    with pytest.raises(ValueError, match="Nested field injection is only supported for body JSON injection."):
        RequestOption(
            field_path=["data", "field"],
            inject_into=RequestOptionType.header,
            parameters={}
        )


def test_inject_into_dict():
    """Test the inject_into_dict functionality"""
    config = {"base_field": "value"}
    
    # Test with field_name
    request_option = RequestOption(
        field_name="test_{{ config['base_field'] }}",
        inject_into=RequestOptionType.body_json,
        parameters={}
    )
    target: Dict[str, Any] = {}
    request_option.inject_into_dict(target, "test_value", config)
    assert target == {"test_value": "test_value"}

    # Test with field_path
    request_option = RequestOption(
        field_path=["data", "nested_{{ config['base_field'] }}", "field"],
        inject_into=RequestOptionType.body_json,
        parameters={}
    )
    target = {}
    request_option.inject_into_dict(target, "test_value", config)
    assert target == {"data": {"nested_value": {"field": "test_value"}}}

@pytest.mark.parametrize(
    "field_name, field_path, inject_into, expected_is_field_path",
    [
        ("field", None, RequestOptionType.body_json, False),
        (None, ["data", "field"], RequestOptionType.body_json, True),
    ]
)
def test_is_field_path(
    field_name: Optional[str],
    field_path: Optional[List[str]],
    inject_into: RequestOptionType,
    expected_is_field_path: bool
):
    """Test the is_field_path property"""
    request_option = RequestOption(
        field_name=field_name,
        field_path=field_path,
        inject_into=inject_into,
        parameters={}
    )
    assert request_option.is_field_path == expected_is_field_path

def test_interpolation_in_field_path():
    """Test that interpolation works in field paths"""
    config = {"nested": "user"}
    parameters = {"type": "profile"}
    
    request_option = RequestOption(
        field_path=["data", "{{ config['nested'] }}", "{{ parameters['type'] }}"],
        inject_into=RequestOptionType.body_json,
        parameters=parameters
    )
    
    target = {}
    request_option.inject_into_dict(target, "test_value", config)
    assert target == {"data": {"user": {"profile": "test_value"}}}


def test_inject_into_dict_multiple_injections():
    """Test injecting multiple values into the same target dict"""
    config = {"base": "test"}
    
    # Create target with existing data
    target = {"existing": "value"}
    
    # First injection with field_name
    option1 = RequestOption(
        field_name="field1",
        inject_into=RequestOptionType.body_json,
        parameters={}
    )
    option1.inject_into_dict(target, "value1", config)
    
    # Second injection with nested path
    option2 = RequestOption(
        field_path=["data", "nested", "field2"],
        inject_into=RequestOptionType.body_json,
        parameters={}
    )
    option2.inject_into_dict(target, "value2", config)
    
    assert target == {
        "existing": "value",
        "field1": "value1",
        "data": {
            "nested": {
                "field2": "value2"
            }
        }
    }

def test_inject_into_dict_deep_nesting():
    """Test injecting values into deeply nested structures"""
    config = {}
    target = {}
    
    request_option = RequestOption(
        field_path=["level1", "level2", "level3", "level4", "field"],
        inject_into=RequestOptionType.body_json,
        parameters={}
    )
    request_option.inject_into_dict(target, "deep_value", config)
    
    assert target == {
        "level1": {
            "level2": {
                "level3": {
                    "level4": {
                        "field": "deep_value"
                    }
                }
            }
        }
    }

def test_inject_into_dict_various_value_types():
    """Test injecting different types of values"""
    config = {}
    test_cases = [
        (42, "integer"),
        (3.14, "float"),
        (True, "boolean"),
        (["a", "b", "c"], "list"),
        ({"key": "value"}, "dict"),
        (None, "none")
    ]
    
    for value, name in test_cases:
        target = {}
        request_option = RequestOption(
            field_path=["data", name],
            inject_into=RequestOptionType.body_json,
            parameters={}
        )
        request_option.inject_into_dict(target, value, config)
        assert target["data"][name] == value

def test_inject_into_dict_with_interpolation_combinations():
    """Test various combinations of static and interpolated path segments"""
    config = {"user_type": "admin", "section": "profile"}
    parameters = {"id": "12345"}
    
    request_option = RequestOption(
        field_path=[
            "data",
            "{{ config['user_type'] }}",
            "{{ parameters['id'] }}",
            "{{ config['section'] }}",
            "details"
        ],
        inject_into=RequestOptionType.body_json,
        parameters=parameters
    )
    
    target = {}
    request_option.inject_into_dict(target, "test_value", config)
    
    assert target == {
        "data": {
            "admin": {
                "12345": {
                    "profile": {
                        "details": "test_value"
                    }
                }
            }
        }
    }

def test_inject_into_dict_error_handling():
    """Test error cases for inject_into_dict"""
    config = {}
    
    # Test with invalid target type
    with pytest.raises(TypeError):
        request_option = RequestOption(
            field_name="test",
            inject_into=RequestOptionType.body_json,
            parameters={}
        )
        request_option.inject_into_dict(None, "value", config)  # type: ignore
    
    # Test with invalid path segments
    with pytest.raises(TypeError):
        request_option = RequestOption(
            field_path={"this": "should", "be": "a list"},  # type: ignore
            inject_into=RequestOptionType.body_json,
            parameters={}
        )
        target = {}
        request_option.inject_into_dict(target, "value", config)
