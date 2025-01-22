#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from enum import Enum
from typing import Any, List, Literal, Mapping, MutableMapping, Optional, Union

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.types import Config


class RequestOptionType(Enum):
    """
    Describes where to set a value on a request
    """

    request_parameter = "request_parameter"
    header = "header"
    body_data = "body_data"
    body_json = "body_json"


@dataclass
class RequestOption:
    """
    Describes an option to set on a request

    Attributes:
        field_name (str): Describes the name of the parameter to inject. Mutually exclusive with field_path.
        field_path (list(str)): Describes the path to a nested field as a list of field names.
          Only valid for body_json injection type, and mutually exclusive with field_name.
        inject_into (RequestOptionType): Describes where in the HTTP request to inject the parameter
    """

    inject_into: RequestOptionType
    parameters: InitVar[Mapping[str, Any]]
    field_name: Optional[Union[InterpolatedString, str]] = None
    field_path: Optional[List[Union[InterpolatedString, str]]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        # Validate inputs
        if self.field_name is None and self.field_path is None:
            raise ValueError("RequestOption requires either a field_name or field_path")
        if self.field_name is not None and self.field_path is not None:
            raise ValueError("Only one of field_name or field_path can be provided")

        # Convert field_name to field_path internally for simplicity. We are keeping the field_name attribute
        # for backwards compatitibility in the Builder.
        if self.field_name is not None:
            self.field_path = [InterpolatedString.create(self.field_name, parameters=parameters)]
            self.field_name = None
        else:
            assert self.field_path is not None  # for type checker
            self.field_path = [
                InterpolatedString.create(segment, parameters=parameters)
                for segment in self.field_path
            ]

        # Validate that non-body-json requests only use single-element paths
        if len(self.field_path) > 1 and self.inject_into != RequestOptionType.body_json:
            raise ValueError(
                "Nested field injection is only supported for body JSON injection. Please use a top-level field_name for other injection types."
            )

    @property
    def _is_field_path(self) -> bool:
        """Returns whether this option is a field path (ie, a nested field)"""
        return self.field_path is not None

    def inject_into_request(
        self,
        target: MutableMapping[str, Any],
        value: Any,
        config: Config,
    ) -> None:
        """
        Inject a request option value into a target request structure.
        For non-body-json injection, only top-level field names are supported.
        For body-json injection, both field names and nested field paths are supported.

        Args:
            target: The request structure to inject the value into
            value: The value to inject
            config: The config object to use for interpolation
        """
        assert self.field_path is not None  # for type checker
        current = target

        # Convert path segments into strings, evaluating any interpolated segments
        # Example: ["data", "{{ config[user_type] }}", "id"] -> ["data", "admin", "id"]
        *path_parts, final_key = [
            str(
                segment.eval(config=config)
                if isinstance(segment, InterpolatedString)
                else segment
            )
            for segment in self.field_path
        ]

        # Build nested structure if needed (for body_json) or set directly (for other types)
        for part in path_parts:
            current = current.setdefault(part, {})
        current[final_key] = value
