#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from copy import deepcopy
from dataclasses import InitVar, dataclass
from typing import Any, List, Mapping, MutableMapping, Optional, Union

import dpath
from deprecated.classic import deprecated

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.declarative.schema.schema_loader import SchemaLoader
from airbyte_cdk.sources.source import ExperimentalClassWarning
from airbyte_cdk.sources.types import Config

AIRBYTE_DATA_TYPES = {
    "string": {"type": "string"},
    "boolean": {"type": "boolean"},
    "date": {"type": "string", "format": "date"},
    "timestamp_without_timezone": {
        "type": "string",
        "format": "date-time",
        "airbyte_type": "timestamp_without_timezone",
    },
    "timestamp_with_timezone": {"type": "string", "format": "date-time"},
    "time_without_timezone": {
        "type": "string",
        "format": "time",
        "airbyte_type": "time_without_timezone",
    },
    "time_with_timezone": {
        "type": "string",
        "format": "time",
        "airbyte_type": "time_with_timezone",
    },
    "integer": {"type": "integer"},
    "number": {"type": "number"},
    "array": {"type": "array"},
    "object": {"type": "object"},
}


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass(frozen=True)
class TypesPair:
    """
    Represents a mapping between a current type and its corresponding target type.
    """

    target_type: Union[List[str], str]
    current_type: Union[List[str], str]


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass
class SchemaTypeIdentifier:
    """
    Identifies schema details for dynamic schema extraction and processing.
    """

    schema_pointer: List[Union[InterpolatedString, str]]
    key_pointer: List[Union[InterpolatedString, str]]
    parameters: InitVar[Mapping[str, Any]]
    type_pointer: Optional[List[Union[InterpolatedString, str]]] = None
    types_map: List[TypesPair] = None
    is_nullable: bool = True

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.schema_pointer = self._update_pointer(self.schema_pointer, parameters)
        self.key_pointer = self._update_pointer(self.key_pointer, parameters)
        self.type_pointer = (
            self._update_pointer(self.type_pointer, parameters) if self.type_pointer else None
        )

    @staticmethod
    def _update_pointer(
        pointer: Optional[List[Union[InterpolatedString, str]]], parameters: Mapping[str, Any]
    ) -> Optional[List[Union[InterpolatedString, str]]]:
        return (
            [
                InterpolatedString.create(path, parameters=parameters)
                if isinstance(path, str)
                else path
                for path in pointer
            ]
            if pointer
            else None
        )


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass
class DynamicSchemaLoader(SchemaLoader):
    """
    Dynamically loads a JSON Schema by extracting data from retrieved records.
    """

    retriever: Retriever
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    schema_type_identifier: SchemaTypeIdentifier

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Constructs a JSON Schema based on retrieved data.
        """
        properties = {}
        for retrieved_record in self.retriever.read_records({}):
            raw_schema = self._extract_data(
                retrieved_record, self.schema_type_identifier.schema_pointer
            )
            for property_definition in raw_schema:
                key = self._get_key(property_definition, self.schema_type_identifier.key_pointer)
                value = self._get_type(
                    property_definition,
                    self.schema_type_identifier.type_pointer,
                    is_nullable=self.schema_type_identifier.is_nullable,
                )
                properties[key] = value

        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": properties,
        }

    def _get_key(
        self,
        raw_schema: MutableMapping[str, Any],
        field_key_path: List[Union[InterpolatedString, str]],
    ) -> str:
        """
        Extracts the key field from the schema using the specified path.
        """
        field_key = self._extract_data(raw_schema, field_key_path)
        if not isinstance(field_key, str):
            raise ValueError(f"Expected key to be a string. Got {field_key}")
        return field_key

    def _get_type(
        self,
        raw_schema: MutableMapping[str, Any],
        field_type_path: Optional[List[Union[InterpolatedString, str]]],
        is_nullable: bool = True,
    ) -> Union[Mapping[str, Any], List[Mapping[str, Any]]]:
        """
        Determines the JSON Schema type for a field, supporting nullable and combined types.
        """
        raw_field_type = (
            self._extract_data(raw_schema, field_type_path, default="string")
            if field_type_path
            else "string"
        )
        mapped_field_type = self._replace_type_if_not_valid(raw_field_type)
        if (
            isinstance(mapped_field_type, list)
            and len(mapped_field_type) == 2
            and all(isinstance(item, str) for item in mapped_field_type)
        ):
            first_type = self._make_field_nullable(
                self._get_airbyte_type(mapped_field_type[0]), is_nullable
            )
            second_type = self._make_field_nullable(
                self._get_airbyte_type(mapped_field_type[1]), is_nullable
            )
            return {"oneOf": [first_type, second_type]}
        return self._make_field_nullable(self._get_airbyte_type(mapped_field_type), is_nullable)

    def _replace_type_if_not_valid(self, field_type: str) -> str:
        """
        Replaces a field type if it matches a type mapping in `types_map`.
        """
        if self.schema_type_identifier.types_map:
            for types_pair in self.schema_type_identifier.types_map:
                if field_type == types_pair.current_type:
                    return types_pair.target_type
        return field_type

    @staticmethod
    def _make_field_nullable(
        field_type: Mapping[str, Any], is_nullable: bool = True
    ) -> Mapping[str, Any]:
        """
        Wraps a field type to allow null values if `is_nullable` is True.
        """

        if is_nullable:
            field_type = deepcopy(field_type)
            field_type["type"] = ["null", field_type["type"]]
        return field_type

    @staticmethod
    def _get_airbyte_type(field_type: str) -> Mapping[str, Any]:
        """
        Maps a field type to its corresponding Airbyte type definition.
        """
        if field_type not in AIRBYTE_DATA_TYPES:
            raise ValueError(f"Invalid Airbyte data type: {field_type}")

        return deepcopy(AIRBYTE_DATA_TYPES[field_type])

    def _extract_data(
        self,
        body: Mapping[str, Any],
        extraction_path: List[Union[InterpolatedString, str]],
        default: Any = None,
    ) -> Any:
        """
        Extracts data from the body based on the provided extraction path.
        """

        if len(extraction_path) == 0:
            return body

        path = [path.eval(self.config) for path in extraction_path]

        if "*" in path:
            extracted = dpath.values(body, path)
        else:
            extracted = dpath.get(body, path, default=default)

        return extracted
