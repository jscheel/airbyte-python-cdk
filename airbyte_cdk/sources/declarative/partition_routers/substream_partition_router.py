#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import copy
import logging
from dataclasses import InitVar, dataclass
from typing import TYPE_CHECKING, Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import dpath

from airbyte_cdk.models import AirbyteMessage
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)
from airbyte_cdk.sources.types import Config, Record, StreamSlice, StreamState
from airbyte_cdk.utils import AirbyteTracedException

if TYPE_CHECKING:
    from airbyte_cdk.sources.declarative.declarative_stream import DeclarativeStream


@dataclass
class ParentStreamConfig:
    """
    Describes how to create a stream slice from a parent stream

    stream: The stream to read records from
    parent_key: The key of the parent stream's records that will be the stream slice key
    partition_field: The partition key
    extra_fields: Additional field paths to include in the stream slice
    request_option: How to inject the slice value on an outgoing HTTP request
    incremental_dependency (bool): Indicates if the parent stream should be read incrementally.
    """

    stream: "DeclarativeStream"  # Parent streams must be DeclarativeStream because we can't know which part of the stream slice is a partition for regular Stream
    parent_key: Union[InterpolatedString, str]
    partition_field: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    extra_fields: Optional[Union[List[List[str]], List[List[InterpolatedString]]]] = (
        None  # List of field paths (arrays of strings)
    )
    request_option: Optional[RequestOption] = None
    incremental_dependency: bool = False

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.parent_key = InterpolatedString.create(self.parent_key, parameters=parameters)
        self.partition_field = InterpolatedString.create(
            self.partition_field, parameters=parameters
        )
        if self.extra_fields:
            # Create InterpolatedString for each field path in extra_keys
            self.extra_fields = [
                [InterpolatedString.create(path, parameters=parameters) for path in key_path]
                for key_path in self.extra_fields
            ]


@dataclass
class SubstreamPartitionRouter(PartitionRouter):
    """
    Partition router that iterates over the parent's stream records and emits slices
    Will populate the state with `partition_field` and `parent_slice` so they can be accessed by other components

    Attributes:
        parent_stream_configs (List[ParentStreamConfig]): parent streams to iterate over and their config
    """

    parent_stream_configs: List[ParentStreamConfig]
    config: Config
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if not self.parent_stream_configs:
            raise ValueError("SubstreamPartitionRouter needs at least 1 parent stream")
        self._parameters = parameters

    def get_request_params(
        self,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        # Pass the stream_slice from the argument, not the cursor because the cursor is updated after processing the response
        return self._get_request_option(RequestOptionType.request_parameter, stream_slice)

    def get_request_headers(
        self,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        # Pass the stream_slice from the argument, not the cursor because the cursor is updated after processing the response
        return self._get_request_option(RequestOptionType.header, stream_slice)

    def get_request_body_data(
        self,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        # Pass the stream_slice from the argument, not the cursor because the cursor is updated after processing the response
        return self._get_request_option(RequestOptionType.body_data, stream_slice)

    def get_request_body_json(
        self,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        # Pass the stream_slice from the argument, not the cursor because the cursor is updated after processing the response
        return self._get_request_option(RequestOptionType.body_json, stream_slice)

    def _get_request_option(
        self, option_type: RequestOptionType, stream_slice: Optional[StreamSlice]
    ) -> Mapping[str, Any]:
        params: MutableMapping[str, Any] = {}
        if stream_slice:
            for parent_config in self.parent_stream_configs:
                if (
                    parent_config.request_option
                    and parent_config.request_option.inject_into == option_type
                ):
                    key = parent_config.partition_field.eval(self.config)  # type: ignore # partition_field is always casted to an interpolated string
                    value = stream_slice.get(key)
                    if value:
                        parent_config.request_option.inject_into_request(params, value, self.config)
        return params

    def stream_slices(self) -> Iterable[StreamSlice]:
        """
        Iterate over each parent stream's record and create a StreamSlice for each record.

        For each stream, iterate over its stream_slices.
        For each stream slice, iterate over each record.
        yield a stream slice for each such records.

        If a parent slice contains no record, emit a slice with parent_record=None.

        The template string can interpolate the following values:
        - parent_stream_slice: mapping representing the parent's stream slice
        - parent_record: mapping representing the parent record
        - parent_stream_name: string representing the parent stream name
        """
        if not self.parent_stream_configs:
            yield from []
        else:
            for parent_stream_config in self.parent_stream_configs:
                parent_stream = parent_stream_config.stream
                parent_field = parent_stream_config.parent_key.eval(self.config)  # type: ignore # parent_key is always casted to an interpolated string
                partition_field = parent_stream_config.partition_field.eval(self.config)  # type: ignore # partition_field is always casted to an interpolated string
                extra_fields = None
                if parent_stream_config.extra_fields:
                    extra_fields = [
                        [field_path_part.eval(self.config) for field_path_part in field_path]  # type: ignore [union-attr]
                        for field_path in parent_stream_config.extra_fields
                    ]

                # read_stateless() assumes the parent is not concurrent. This is currently okay since the concurrent CDK does
                # not support either substreams or RFR, but something that needs to be considered once we do
                for parent_record in parent_stream.read_only_records():
                    parent_partition = None
                    # Skip non-records (eg AirbyteLogMessage)
                    if isinstance(parent_record, AirbyteMessage):
                        self.logger.warning(
                            f"Parent stream {parent_stream.name} returns records of type AirbyteMessage. This SubstreamPartitionRouter is not able to checkpoint incremental parent state."
                        )
                        if parent_record.type == MessageType.RECORD:
                            parent_record = parent_record.record.data  # type: ignore[union-attr, assignment]  # record is always a Record
                        else:
                            continue
                    elif isinstance(parent_record, Record):
                        parent_partition = (
                            parent_record.associated_slice.partition
                            if parent_record.associated_slice
                            else {}
                        )
                        parent_record = parent_record.data
                    elif not isinstance(parent_record, Mapping):
                        # The parent_record should only take the form of a Record, AirbyteMessage, or Mapping. Anything else is invalid
                        raise AirbyteTracedException(
                            message=f"Parent stream returned records as invalid type {type(parent_record)}"
                        )
                    try:
                        partition_value = dpath.get(
                            parent_record,  # type: ignore [arg-type]
                            parent_field,
                        )
                    except KeyError:
                        continue

                    # Add extra fields
                    extracted_extra_fields = self._extract_extra_fields(parent_record, extra_fields)

                    yield StreamSlice(
                        partition={
                            partition_field: partition_value,
                            "parent_slice": parent_partition or {},
                        },
                        cursor_slice={},
                        extra_fields=extracted_extra_fields,
                    )

    def _extract_extra_fields(
        self,
        parent_record: Mapping[str, Any] | AirbyteMessage,
        extra_fields: Optional[List[List[str]]] = None,
    ) -> Mapping[str, Any]:
        """
        Extracts additional fields specified by their paths from the parent record.

        Args:
            parent_record (Mapping[str, Any]): The record from the parent stream to extract fields from.
            extra_fields (Optional[List[List[str]]]): A list of field paths (as lists of strings) to extract from the parent record.

        Returns:
            Mapping[str, Any]: A dictionary containing the extracted fields.
                               The keys are the joined field paths, and the values are the corresponding extracted values.
        """
        extracted_extra_fields = {}
        if extra_fields:
            for extra_field_path in extra_fields:
                try:
                    extra_field_value = dpath.get(
                        parent_record,  # type: ignore [arg-type]
                        extra_field_path,
                    )
                    self.logger.debug(
                        f"Extracted extra_field_path: {extra_field_path} with value: {extra_field_value}"
                    )
                except KeyError:
                    self.logger.debug(f"Failed to extract extra_field_path: {extra_field_path}")
                    extra_field_value = None
                extracted_extra_fields[".".join(extra_field_path)] = extra_field_value
        return extracted_extra_fields

    def set_initial_state(self, stream_state: StreamState) -> None:
        """
        Set the state of the parent streams.

        If the `parent_state` key is missing from `stream_state`, migrate the child stream state to the parent stream's state format.
        This migration applies only to parent streams with incremental dependencies.

        Args:
            stream_state (StreamState): The state of the streams to be set.

        Example of state format:
        {
            "parent_state": {
                "parent_stream_name1": {
                    "last_updated": "2023-05-27T00:00:00Z"
                },
                "parent_stream_name2": {
                    "last_updated": "2023-05-27T00:00:00Z"
                }
            }
        }

        Example of migrating to parent state format:
        - Initial state:
        {
            "updated_at": "2023-05-27T00:00:00Z"
        }
        - After migration:
        {
            "updated_at": "2023-05-27T00:00:00Z",
            "parent_state": {
                "parent_stream_name": {
                    "parent_stream_cursor": "2023-05-27T00:00:00Z"
                }
            }
        }
        """
        if not stream_state:
            return

        parent_state = stream_state.get("parent_state", {})

        # If `parent_state` doesn't exist and at least one parent stream has an incremental dependency,
        # copy the child state to parent streams with incremental dependencies.
        incremental_dependency = any(
            [parent_config.incremental_dependency for parent_config in self.parent_stream_configs]
        )
        if not parent_state and not incremental_dependency:
            return

        if not parent_state and incremental_dependency:
            # Migrate child state to parent state format
            parent_state = self._migrate_child_state_to_parent_state(stream_state)

        # Set state for each parent stream with an incremental dependency
        for parent_config in self.parent_stream_configs:
            if parent_config.incremental_dependency:
                parent_config.stream.state = parent_state.get(parent_config.stream.name, {})

    def _migrate_child_state_to_parent_state(self, stream_state: StreamState) -> StreamState:
        """
        Migrate the child or global stream state into the parent stream's state format.

        This method converts the child stream state—or, if present, the global state—into a format that is
        compatible with parent streams that use incremental synchronization. The migration occurs only for
        parent streams with incremental dependencies. It filters out per-partition states and retains only the
        global state in the form {cursor_field: cursor_value}.

        The method supports multiple input formats:
          - A simple global state, e.g.:
                {"updated_at": "2023-05-27T00:00:00Z"}
          - A state object that contains a "state" key (which is assumed to hold the global state), e.g.:
                {"state": {"updated_at": "2023-05-27T00:00:00Z"}, ...}
            In this case, the migration uses the first value from the "state" dictionary.
          - Any per-partition state formats or other non-simple structures are ignored during migration.

        Args:
            stream_state (StreamState): The state to migrate. Expected formats include:
                - {"updated_at": "2023-05-27T00:00:00Z"}
                - {"state": {"updated_at": "2023-05-27T00:00:00Z"}, ...}
                  (In this format, only the first global state value is used, and per-partition states are ignored.)

        Returns:
            StreamState: A migrated state for parent streams in the format:
                {
                    "parent_stream_name": {"parent_stream_cursor": "2023-05-27T00:00:00Z"}
                }
            where each parent stream with an incremental dependency is assigned its corresponding cursor value.

        Example:
            Input: {"updated_at": "2023-05-27T00:00:00Z"}
            Output: {
                "parent_stream_name": {"parent_stream_cursor": "2023-05-27T00:00:00Z"}
            }
        """
        substream_state_values = list(stream_state.values())
        substream_state = substream_state_values[0] if substream_state_values else {}

        # Ignore per-partition states or invalid formats.
        if isinstance(substream_state, (list, dict)) or len(substream_state_values) != 1:
            # If a global state is present under the key "state", use its first value.
            if "state" in stream_state and isinstance(stream_state["state"], dict):
                substream_state = list(stream_state["state"].values())[0]
            else:
                return {}

        # Build the parent state for all parent streams with incremental dependencies.
        parent_state = {}
        if substream_state:
            for parent_config in self.parent_stream_configs:
                if parent_config.incremental_dependency:
                    parent_state[parent_config.stream.name] = {
                        parent_config.stream.cursor_field: substream_state
                    }

        return parent_state

    def get_stream_state(self) -> Optional[Mapping[str, StreamState]]:
        """
        Get the state of the parent streams.

        Returns:
            StreamState: The current state of the parent streams.

        Example of state format:
        {
            "parent_stream_name1": {
                "last_updated": "2023-05-27T00:00:00Z"
            },
            "parent_stream_name2": {
                "last_updated": "2023-05-27T00:00:00Z"
            }
        }
        """
        parent_state = {}
        for parent_config in self.parent_stream_configs:
            if parent_config.incremental_dependency:
                parent_state[parent_config.stream.name] = copy.deepcopy(parent_config.stream.state)
        return parent_state

    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger("airbyte.SubstreamPartitionRouter")
