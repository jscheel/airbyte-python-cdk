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
class FieldName:
    """Represents a direct field name reference"""

    value: Union[InterpolatedString, str]
    type: Literal["field_name"] = "field_name"


@dataclass
class FieldPath:
    """Represents a path to a nested field"""

    value: List[Union[InterpolatedString, str]]
    type: Literal["field_path"] = "field_path"


@dataclass
class RequestOption:
    """
    Describes an option to set on a request

    Attributes:
        field_name: Describes the name of the parameter to inject. Mutually exclusive with field_path.
        field_path: Describes the path to a nested field as a list of field names. Mutually exclusive with field_name.
        inject_into: Describes where in the HTTP request to inject the parameter
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
            raise ValueError(
                "Only one of field_name or field_path can be provided to RequestOption"
            )

        # Handle interpolation
        if self.field_name is not None:
            self.field_name = InterpolatedString.create(self.field_name, parameters=parameters)
        if self.field_path is not None:
            self.field_path = [
                InterpolatedString.create(segment, parameters=parameters)
                for segment in self.field_path
            ]

    @property
    def is_field_path(self) -> bool:
        """Returns whether this option uses a field path"""
        return self.field_path is not None

    def inject_into_dict(
        self,
        target: MutableMapping[str, Any],
        value: Any,
        config: Config,
    ) -> None:
        """
        Inject a value into a target dict using either field_name or field_path

        Args:
            target: The dict to inject the value into
            value: The value to inject
            config: The config object to use for interpolation
        """

        if self.is_field_path:
            assert self.field_path is not None
            current = target
            *path_parts, final_key = [
                segment.eval(config=config) if isinstance(segment, InterpolatedString) else segment
                for segment in self.field_path
            ]

            for part in path_parts:
                current = current.setdefault(part, {})
            current[final_key] = value
        else:
            assert self.field_name is not None
            target[
                self.field_name.eval(config=config)
                if isinstance(self.field_name, InterpolatedString)
                else self.field_name
            ] = value
