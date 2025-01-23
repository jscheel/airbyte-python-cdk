#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from copy import deepcopy
from dataclasses import InitVar, dataclass, field
from typing import Any, List, Mapping, MutableMapping, Optional, Tuple, Union

import dpath
from typing_extensions import deprecated

from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.declarative.schema.schema_loader import SchemaLoader
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.source import ExperimentalClassWarning
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState

AIRBYTE_DATA_TYPES: Mapping[str, Mapping[str, Any]] = {
    "string": {"type": ["null", "string"]},
    "boolean": {"type": ["null", "boolean"]},
    "date": {"type": ["null", "string"], "format": "date"},
    "timestamp_without_timezone": {
        "type": ["null", "string"],
        "format": "date-time",
        "airbyte_type": "timestamp_without_timezone",
    },
    "timestamp_with_timezone": {"type": ["null", "string"], "format": "date-time"},
    "time_without_timezone": {
        "type": ["null", "string"],
        "format": "time",
        "airbyte_type": "time_without_timezone",
    },
    "time_with_timezone": {
        "type": ["null", "string"],
        "format": "time",
        "airbyte_type": "time_with_timezone",
    },
    "integer": {"type": ["null", "integer"]},
    "number": {"type": ["null", "number"]},
    "array": {"type": ["null", "array"]},
    "object": {"type": ["null", "object"]},
}


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass(frozen=True)
class PropertyTypesMap:
    """
    Represents a mapping between a current type and its corresponding target type for property.
    """

    property_name: str
    property_type_pointer: List[Union[InterpolatedString, str]]
    type_mapping: "TypesMap"


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass(frozen=True)
class ItemsTypeMap:
    """
    Represents a mapping between a current type and its corresponding target type for item.
    """

    items_type_pointer: List[Union[InterpolatedString, str]]
    type_mapping: "TypesMap"


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass(frozen=True)
class TypesMap:
    """
    Represents a mapping between a current type and its corresponding target type.
    """

    target_type: Union[List[str], str]
    current_type: Union[List[str], str]
    condition: Optional[str]
    items_type: Optional[ItemsTypeMap] = None
    properties_types: Optional[List[PropertyTypesMap]] = None

    def __post_init__(self):
        """
        Ensures that only one of `items_type` or `properties_types` can be set.
        Additionally, enforces that `items_type` is only used when `target_type` is a list,
        and `properties_types` is only used when `target_type` is an object.
        """
        # Ensure only one of `items_type` or `properties_types` is set
        if self.items_type and self.properties_types:
            raise ValueError(
                "Cannot specify both 'items_type' and 'properties_types' at the same time."
            )

        # `items_type` is valid only for array target types
        if self.items_type and self.target_type != "array":
            raise ValueError("'items_type' can only be used when 'target_type' is an array.")

        # `properties_types` is valid only for object target types
        if self.properties_types and self.target_type != "object":
            raise ValueError("'properties_types' can only be used when 'target_type' is an object.")


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass
class SchemaTypeIdentifier:
    """
    Identifies schema details for dynamic schema extraction and processing.
    """

    key_pointer: List[Union[InterpolatedString, str]]
    parameters: InitVar[Mapping[str, Any]]
    type_pointer: Optional[List[Union[InterpolatedString, str]]] = None
    types_mapping: Optional[List[TypesMap]] = None
    schema_pointer: Optional[List[Union[InterpolatedString, str]]] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.schema_pointer = (
            self._update_pointer(self.schema_pointer, parameters) if self.schema_pointer else []
        )  # type: ignore[assignment]  # This is reqired field in model
        self.key_pointer = self._update_pointer(self.key_pointer, parameters)  # type: ignore[assignment]  # This is reqired field in model
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
    schema_transformations: List[RecordTransformation] = field(default_factory=lambda: [])

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Constructs a JSON Schema based on retrieved data.
        """
        properties = {}
        retrieved_record = next(self.retriever.read_records({}), None)  # type: ignore[call-overload] # read_records return Iterable data type

        raw_schema = (
            self._extract_data(
                retrieved_record,  # type: ignore[arg-type] # Expected that retrieved_record will be only Mapping[str, Any]
                self.schema_type_identifier.schema_pointer,
            )
            if retrieved_record
            else []
        )

        for property_definition in raw_schema:
            key = self._get_key(property_definition, self.schema_type_identifier.key_pointer)
            value = self._get_type(
                property_definition,
                self.schema_type_identifier.type_pointer,
            )
            properties[key] = value

        transformed_properties = self._transform(properties, {})

        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": transformed_properties,
        }

    def _transform(
        self,
        properties: Mapping[str, Any],
        stream_state: StreamState,
        stream_slice: Optional[StreamSlice] = None,
    ) -> Mapping[str, Any]:
        for transformation in self.schema_transformations:
            transformation.transform(
                properties,  # type: ignore  # properties has type Mapping[str, Any], but Dict[str, Any] expected
                config=self.config,
            )
        return properties

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
    ) -> Union[Mapping[str, Any], List[Mapping[str, Any]]]:
        """
        Determines the JSON Schema type for a field, supporting nullable and combined types.
        """
        raw_field_type = (
            self._extract_data(raw_schema, field_type_path, default="string")
            if field_type_path
            else "string"
        )
        mapped_field_type, mapped_additional_types = self._replace_type_if_not_valid(
            raw_field_type, raw_schema
        )

        if (
            isinstance(mapped_field_type, list)
            and len(mapped_field_type) == 2
            and all(isinstance(item, str) for item in mapped_field_type)
        ):
            first_type = self._get_airbyte_type(mapped_field_type[0])
            second_type = self._get_airbyte_type(mapped_field_type[1])
            return {"oneOf": [first_type, second_type]}
        elif isinstance(mapped_field_type, str):
            return self._get_airbyte_type(mapped_field_type, mapped_additional_types)
        else:
            raise ValueError(
                f"Invalid data type. Available string or two items list of string. Got {mapped_field_type}."
            )

    def _replace_type_if_not_valid(
        self,
        field_type: Union[List[str], str],
        raw_schema: MutableMapping[str, Any],
    ) -> Tuple[Union[List[str], str], List]:
        """
        Replaces a field type if it matches a type mapping in `types_map`.
        """
        additional_types = []
        if self.schema_type_identifier.types_mapping:
            for types_map in self.schema_type_identifier.types_mapping:
                # conditional is optional param, setting to true if not provided
                condition = InterpolatedBoolean(
                    condition=types_map.condition if types_map.condition is not None else "True",
                    parameters={},
                ).eval(config=self.config, raw_schema=raw_schema)

                if field_type == types_map.current_type and condition:
                    if types_map.items_type:
                        items_type = self._extract_data(
                            raw_schema, types_map.items_type.items_type_pointer
                        )
                        items_type_condition = InterpolatedBoolean(
                            condition=types_map.items_type.type_mapping.condition
                            if types_map.items_type.type_mapping.condition is not None
                            else "True",
                            parameters={},
                        ).eval(config=self.config, raw_schema=raw_schema)

                        if (
                            items_type == types_map.items_type.type_mapping.current_type
                            and items_type_condition
                        ):
                            additional_types = [types_map.items_type.type_mapping.target_type]

                    return types_map.target_type, additional_types
        return field_type, additional_types

    @staticmethod
    def _get_airbyte_type(
        field_type: str, additional_types: Optional[List] = None
    ) -> Mapping[str, Any]:
        """
        Maps a field type to its corresponding Airbyte type definition.
        """
        print(additional_types)
        if additional_types is None:
            additional_types = []

        if field_type not in AIRBYTE_DATA_TYPES:
            raise ValueError(f"Invalid Airbyte data type: {field_type}")

        airbyte_type = deepcopy(AIRBYTE_DATA_TYPES[field_type])

        if field_type == "array" and additional_types:
            airbyte_type["items"] = deepcopy(AIRBYTE_DATA_TYPES[additional_types[0]])

        return airbyte_type

    def _extract_data(
        self,
        body: Mapping[str, Any],
        extraction_path: Optional[List[Union[InterpolatedString, str]]] = None,
        default: Any = None,
    ) -> Any:
        """
        Extracts data from the body based on the provided extraction path.
        """

        if not extraction_path:
            return body

        path = [
            node.eval(self.config) if not isinstance(node, str) else node
            for node in extraction_path
        ]

        return dpath.get(body, path, default=default)  # type: ignore # extracted will be a MutableMapping, given input data structure
