#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, Mapping, MutableMapping, Optional, Union

from typing_extensions import deprecated

from airbyte_cdk.sources.declarative.interpolation.interpolated_nested_mapping import NestedMapping
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_nested_request_input_provider import (
    InterpolatedNestedRequestInputProvider,
)
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_input_provider import (
    InterpolatedRequestInputProvider,
)
from airbyte_cdk.sources.declarative.requesters.request_options.request_options_provider import (
    RequestOptionsProvider,
)
from airbyte_cdk.sources.source import ExperimentalClassWarning
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState

RequestInput = Union[str, Mapping[str, str]]
ValidRequestTypes = (str, list)


@dataclass
class InterpolatedRequestOptionsProvider(RequestOptionsProvider):
    """
    Defines the request options to set on an outgoing HTTP request by evaluating `InterpolatedMapping`s

    Attributes:
        config (Config): The user-provided configuration as specified by the source's spec
        request_parameters (Union[str, Mapping[str, str]]): The request parameters to set on an outgoing HTTP request
        request_headers (Union[str, Mapping[str, str]]): The request headers to set on an outgoing HTTP request
        request_body_data (Union[str, Mapping[str, str]]): The body data to set on an outgoing HTTP request
        request_body_json (Union[str, Mapping[str, str]]): The json content to set on an outgoing HTTP request
    """

    parameters: InitVar[Mapping[str, Any]]
    config: Config = field(default_factory=dict)
    request_parameters: Optional[RequestInput] = None
    request_headers: Optional[RequestInput] = None
    request_body_data: Optional[RequestInput] = None
    request_body_json: Optional[NestedMapping] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if self.request_parameters is None:
            self.request_parameters = {}
        if self.request_headers is None:
            self.request_headers = {}
        if self.request_body_data is None:
            self.request_body_data = {}
        if self.request_body_json is None:
            self.request_body_json = {}

        if self.request_body_json and self.request_body_data:
            raise ValueError(
                "RequestOptionsProvider should only contain either 'request_body_data' or 'request_body_json' not both"
            )

        self._parameter_interpolator = InterpolatedRequestInputProvider(
            config=self.config, request_inputs=self.request_parameters, parameters=parameters
        )
        self._headers_interpolator = InterpolatedRequestInputProvider(
            config=self.config, request_inputs=self.request_headers, parameters=parameters
        )
        self._body_data_interpolator = InterpolatedRequestInputProvider(
            config=self.config, request_inputs=self.request_body_data, parameters=parameters
        )
        self._body_json_interpolator = InterpolatedNestedRequestInputProvider(
            config=self.config, request_inputs=self.request_body_json, parameters=parameters
        )

    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        interpolated_value = self._parameter_interpolator.eval_request_inputs(
            stream_state,
            stream_slice,
            next_page_token,
            valid_key_types=(str,),
            valid_value_types=ValidRequestTypes,
        )
        if isinstance(interpolated_value, dict):
            return interpolated_value
        return {}

    def get_request_headers(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self._headers_interpolator.eval_request_inputs(
            stream_state, stream_slice, next_page_token
        )

    def get_request_body_data(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Union[Mapping[str, Any], str]:
        return self._body_data_interpolator.eval_request_inputs(
            stream_state,
            stream_slice,
            next_page_token,
            valid_key_types=(str,),
            valid_value_types=ValidRequestTypes,
        )

    def get_request_body_json(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self._body_json_interpolator.eval_request_inputs(
            stream_state, stream_slice, next_page_token
        )

    @deprecated(
        "This class is temporary and used to incrementally deliver low-code to concurrent",
        category=ExperimentalClassWarning,
    )
    def request_options_contain_stream_state(self) -> bool:
        """
        Temporary helper method used as we move low-code streams to the concurrent framework. This method determines if
        the InterpolatedRequestOptionsProvider has is a dependency on a non-thread safe interpolation context such as
        stream_state.
        """
        return (
            self._check_if_interpolation_uses_stream_state(self.request_parameters)
            or self._check_if_interpolation_uses_stream_state(self.request_headers)
            or self._check_if_interpolation_uses_stream_state(self.request_body_data)
            or self._check_if_interpolation_uses_stream_state(self.request_body_json)
        )

    @staticmethod
    def _check_if_interpolation_uses_stream_state(
        request_input: Optional[Union[RequestInput, NestedMapping]],
    ) -> bool:
        if not request_input:
            return False
        elif isinstance(request_input, str):
            return "stream_state" in request_input
        else:
            for key, val in request_input.items():
                # Covers the case of RequestInput in the form of a string or Mapping[str, str]. It also covers the case
                # of a NestedMapping where the value is a string.
                # Note: Doesn't account for nested mappings for request_body_json, but I don't see stream_state used in that way
                # in our code
                if "stream_state" in key or (isinstance(val, str) and "stream_state" in val):
                    return True
        return False
